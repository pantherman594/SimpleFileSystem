package fs

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/pantherman594/SimpleFileSystem/pkg/disk"
)

const BlocksPerInode = 32
const DirEntriesPerBlock = 32

const (
	NumInodeBytes = disk.BlockSize
	MaxNumInodes  = NumInodeBytes * 8
	NumDataBytes  = disk.BlockSize
	MaxNumData    = NumDataBytes * 8

	InodeBlock      = 1
	InodeBlockCount = 1
	DataBlock       = InodeBlock + InodeBlockCount
	DataBlockCount  = 1

	FirstInode = DataBlock + DataBlockCount
	NumInodes  = 64
	FirstData  = FirstInode + NumInodes
)

type Inode struct {
	Atime  time.Time
	Mtime  time.Time
	Ctime  time.Time
	Size   uint64
	Mode   os.FileMode // = uint32
	Nlink  uint32
	Nblock uint32
	Blocks [BlocksPerInode]uint64
}

// Serialize converts the inode into an array of bytes, for writing to disk.
func (i *Inode) Serialize(buf []byte) error {
	if uint32(len(buf)) < 68 + i.Nblock * 8 {
		return fmt.Errorf("Buffer too small.")
	}

	data := buf

	atime, err := i.Atime.MarshalBinary()
	if err != nil {
		return err
	}

	mtime, err := i.Mtime.MarshalBinary()
	if err != nil {
		return err
	}

	ctime, err := i.Ctime.MarshalBinary()
	if err != nil {
		return err
	}

	copy(data, atime) // bytes 0-14: atime
	copy(data[16:], mtime) // bytes 16-30: mtime
	copy(data[32:], ctime) // bytes 32-46: ctime
	binary.BigEndian.PutUint64(data[48:], i.Size) // bytes 48-55: size
	binary.BigEndian.PutUint32(data[56:], uint32(i.Mode)) // bytes 56-59: mode
	binary.BigEndian.PutUint32(data[60:], i.Nlink) // bytes 60-63: nlink
	binary.BigEndian.PutUint32(data[64:], i.Nblock) // bytes 64-67: nblock
	data = data[68:]

	// remaining bytes: block pointers
	for b := uint32(0); b < i.Nblock; b++ {
		binary.BigEndian.PutUint64(data, i.Blocks[b])
		data = data[8:]
	}

	return nil
}

// Deserialize converts an array of bytes back into an inode.
func (i *Inode) Deserialize(buf []byte) error {
	data := buf

	err := i.Atime.UnmarshalBinary(data[0:15])
	if err != nil {
		return err
	}

	err = i.Mtime.UnmarshalBinary(data[16:31])
	if err != nil {
		return err
	}

	err = i.Ctime.UnmarshalBinary(data[32:47])
	if err != nil {
		return err
	}

	i.Size = binary.BigEndian.Uint64(data[48:])
	i.Mode = os.FileMode(binary.BigEndian.Uint32(data[56:]))
	i.Nlink = binary.BigEndian.Uint32(data[60:])
	i.Nblock = binary.BigEndian.Uint32(data[64:])
	data = data[68:]

	// remaining bytes: block pointers
	for b := uint32(0); b < i.Nblock; b++ {
		i.Blocks[b] = binary.BigEndian.Uint64(data)
		data = data[8:]
	}

	return nil
}

// ToAttr converts an inode into a fuse attributes struct.
func (i *Inode) ToAttr(a *fuse.Attr) {
	a.Size = i.Size
	a.Atime = i.Atime
	a.Mtime = i.Mtime
	a.Ctime = i.Ctime
	a.Mode = i.Mode
	a.Nlink = i.Nlink
}

// GetOrAllocData allocates new data blocks as necessary, up to and including the
// requested index. It then returns the block number of that index.
func (i *Inode) GetOrAllocData(f *FS, index uint64) (uint64, error) {
	fmt.Printf("get %d\n", index)
	for uint32(index) >= i.Nblock {
		block, err := f.DataAlloc()
		if err != nil {
			return 0, err
		}

		fmt.Printf("create %d to %d\n", uint64(block), i.Nblock)

		i.Blocks[i.Nblock] = uint64(block)
		i.Nblock += 1
	}

	fmt.Printf("return %d\n", i.Blocks[index])
	return i.Blocks[index], nil
}

// Init initializes a new filesystem on the provided disk handle.
func Init(handle int, mountpoint string) error {
	fmt.Println("a")
	diskHandle := handle
	inodeBitmap := make([]byte, InodeBlockCount*disk.BlockSize)
	dataBitmap := make([]byte, DataBlockCount*disk.BlockSize)
	fmt.Println("b")

	f := &FS{
		diskHandle,
		inodeBitmap,
		dataBitmap,
	}

	fmt.Println("c")
	inodenr, err := f.InodeAlloc()
	if err != nil {
		return err
	}
	if inodenr != 0 {
		return fmt.Errorf("Allocated an unexpected inode.")
	}

	fmt.Println("d")
	inode := Inode{
		Atime:  time.Now(),
		Mtime:  time.Now(),
		Ctime:  time.Now(),
		Mode:   os.ModeDir,
		Nlink:  1,
		Blocks: [BlocksPerInode]uint64{},
	}

	fmt.Println("e")
	datanr, err := inode.GetOrAllocData(f, 0)
	if err != nil {
		return err
	}

	fmt.Println("f")
	f.writeInode(int(inodenr), &inode)

	dirents := Dirents{}
	block := [disk.BlockSize]byte{}
	dirents.Serialize(block[:])
	disk.WriteBlock(diskHandle, int(datanr), &block)

	fmt.Println("g")
	return f.fuseOpen(mountpoint)
}

// Open opens an existing filesystem on the provided disk handle.
func Open(handle int, mountpoint string) error {
	diskHandle := handle
	inodeBitmap := make([]byte, 0, InodeBlockCount*disk.BlockSize)
	dataBitmap := make([]byte, 0, DataBlockCount*disk.BlockSize)

	block := [disk.BlockSize]byte{}

	// Inode bitmap is in block 1.
	for blockNum := InodeBlock; blockNum < InodeBlock+InodeBlockCount; blockNum++ {
		_, err := disk.ReadBlock(diskHandle, blockNum, &block)
		if err != nil {
			return err
		}
		inodeBitmap = append(inodeBitmap, block[:]...)
	}

	// Data block bitmap is in block 2.
	for blockNum := DataBlock; blockNum < DataBlock+DataBlockCount; blockNum++ {
		_, err := disk.ReadBlock(diskHandle, blockNum, &block)
		if err != nil {
			return err
		}
		dataBitmap = append(dataBitmap, block[:]...)
	}

	f := &FS{
		diskHandle,
		inodeBitmap,
		dataBitmap,
	}

	return f.fuseOpen(mountpoint)
}

// fuseOpen serves the filesystem and mounts it to the provided mountpoint.
func (f *FS) fuseOpen(mountpoint string) error {
	c, err := fuse.Mount(mountpoint)
	fmt.Println("h")
	if err != nil {
		return err
	}
	defer c.Close()

	fmt.Println("i")
	err = fs.Serve(c, f)
	fmt.Println("j")
	if err != nil {
		return err
	}

	// <-c.Ready
	// err = c.MountError
	// if err != nil {
	// 	return err
	// }

	return nil
}

// writeInode writes an inode to disk.
func (f *FS) writeInode(inodeindex int, inode *Inode) error {
	block := [disk.BlockSize]byte{}

	err := inode.Serialize(block[:])
	if err != nil {
		return err
	}

	_, err = disk.WriteBlock(f.diskHandle, FirstInode+inodeindex, &block)
	if err != nil {
		return err
	}

	return nil
}

// readInode reads an inode from disk.
func (f *FS) readInode(inodeindex int, inode *Inode) error {
	block := [disk.BlockSize]byte{}

	_, err := disk.ReadBlock(f.diskHandle, FirstInode+inodeindex, &block)
	if err != nil {
		return err
	}

	err = inode.Deserialize(block[:])
	if err != nil {
		return err
	}

	return nil
}

// readBitmap returns whether the specified index in the bitmap is in use.
func readBitmap(bitmap []byte, index uint64) (bool, error) {
	byteNum := index / 8
	bitNum := index % 8

	if byteNum > uint64(len(bitmap)) {
		return false, fmt.Errorf("Bitmap index out of bounds.")
	}

	return bitmap[byteNum]&(1<<(7-bitNum)) != 0, nil
}

// writeBitmap writes the value to the bitmap at the specified index.
func writeBitmap(bitmap []byte, index uint64, value bool) error {
	byteNum := index / 8
	bitNum := index % 8

	if byteNum > uint64(len(bitmap)) {
		return fmt.Errorf("Bitmap index out of bounds.")
	}

	bitmap[byteNum] |= (1 << (7 - bitNum))
	return nil
}

// bitmapAlloc finds the first available entry in the given bitmap, marks it as
// used, and returns its index.
func bitmapAlloc(bitmap []byte, maxEntries int64) (uint64, error) {
	if maxEntries < 0 || maxEntries > int64(len(bitmap)) {
		maxEntries = int64(len(bitmap))
	}

	for i := uint64(0); i < uint64(maxEntries) / 8; i++ {
		// Check if the entire byte is filled with 1s
		if bitmap[i] == 0xff {
			continue
		}

		for j := uint64(0); j < 8; j++ {
			used, err := readBitmap(bitmap, i * 8 + j)
			if err != nil {
				return 0, fmt.Errorf("Error reading bitmap: %v", err)
			}

			if !used {
				err := writeBitmap(bitmap, i * 8 + j, true)
				if err != nil {
					return 0, fmt.Errorf("Error writing to bitmap: %v", err)
				}

				return i * 8 + j, nil
			}
		}
	}

	return 0, fmt.Errorf("All inodes are in use.")
}

// InodeAlloc allocates the first available inode, and returns the inode number.
func (f *FS) InodeAlloc() (uint64, error) {
	inodenr, err := bitmapAlloc(f.inodeBitmap, NumInodes)
	if err != nil {
		return 0, err
	}
	fmt.Printf("ALLOC INODE %d\n", inodenr)

	f.Sync()
	return inodenr, nil
}

// DataAlloc allocates the first available data block, and returns the block number
// on disk.
func (f *FS) DataAlloc() (uint64, error) {
	// TODO set a reasonable max, otherwise this could go past the size of the disk.

	datanr, err := bitmapAlloc(f.dataBitmap, -1)
	if err != nil {
		return 0, err
	}

	return datanr + FirstData, nil
}

// Sync writes the values in the bitmaps to the disk.
func (f *FS) Sync() error {
	// Inode bitmap is in block 1.
	for i := 0; i < InodeBlockCount; i++ {
		bitmapSlice := (*[disk.BlockSize]byte)(f.dataBitmap[i*disk.BlockSize : (i+1)*disk.BlockSize])
		_, err := disk.WriteBlock(f.diskHandle, InodeBlock+i, bitmapSlice)
		if err != nil {
			return err
		}
	}

	// Data block bitmap is in block 2.
	for i := 0; i < DataBlockCount; i++ {
		bitmapSlice := (*[disk.BlockSize]byte)(f.dataBitmap[i*disk.BlockSize : (i+1)*disk.BlockSize])
		_, err := disk.WriteBlock(f.diskHandle, DataBlock+i, bitmapSlice)
		if err != nil {
			return err
		}
	}

	return nil
}

type FS struct {
	diskHandle  int
	inodeBitmap []byte
	dataBitmap  []byte
}
var _ fs.FS = (*FS)(nil)

func (f *FS) Root() (fs.Node, error) {
	fmt.Println("root")
	return &Dir{f, 0}, nil
}

type Dir struct {
	fs    *FS
	inode int
}
var _ fs.Node = (*Dir)(nil)

var _ = fs.NodeRequestLookuper(&Dir{})
func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	fmt.Println("lookup")
	fmt.Println(req.Name)
	return nil, syscall.ENOENT
}

// The struct of the directory entry saved to disk.
type Dirent struct {
	inode    uint32
	nameSize uint32
	name     string
}

type Dirents struct {
	entries []Dirent
}

func (dirents *Dirents) Serialize(buf []byte) error {
	remainingSize := len(buf)
	if remainingSize < 4 {
		return fmt.Errorf("Buffer too small.")
	}

	data := buf
	remainingSize -= 4
	defer func() {
		binary.BigEndian.PutUint32(data, uint32(0xffffffff))
	}()

	for _, dirent := range dirents.entries {
		size := 8 + len(dirent.name)
		if size > remainingSize {
			return fmt.Errorf("Buffer full.")
		}

		binary.BigEndian.PutUint32(data, dirent.inode)
		binary.BigEndian.PutUint32(data[4:], uint32(len(dirent.name)))
		copy(data[8:], []byte(dirent.name))

		data = data[size:]
	}

	return nil
}

func (dirents *Dirents) Deserialize(buf []byte) error {
	var byteIndex uint32
	l := uint32(len(buf))

	for byteIndex = 0; byteIndex < l; {
		inr := binary.BigEndian.Uint32(buf[byteIndex : byteIndex+4])
		byteIndex += 4
		if inr == 0xffffffff {
			break
		}

		nameSize := binary.BigEndian.Uint32(buf[byteIndex : byteIndex+4])
		byteIndex += 4

		name := string(buf[byteIndex : byteIndex+nameSize])
		byteIndex += nameSize

		dirents.entries = append(dirents.entries, Dirent{
			inode: inr,
			nameSize: nameSize,
			name:  name,
		})
	}

	if byteIndex >= l {
		return fmt.Errorf("Directory entries corrupted: End not found.")
	}

	return nil
}

var _ = fs.HandleReadDirAller(&Dir{})
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fmt.Println("open")
	fs := d.fs
	var dirInode Inode
	err := fs.readInode(d.inode, &dirInode)
	if err != nil {
		return nil, err
	}

	dirInode.Atime = time.Now()
	fs.writeInode(d.inode, &dirInode)
	blocknr := dirInode.Blocks[0]
	block := [disk.BlockSize]byte{}

	_, err = disk.ReadBlock(fs.diskHandle, int(blocknr), &block)
	if err != nil {
		return nil, err
	}

	dirents := Dirents{}
	dirents.Deserialize(block[:])
	fmt.Println(block)
	fmt.Println(dirents.entries)

	entries := make([]fuse.Dirent, 0)

	for _, dirent := range dirents.entries {
		var inode Inode
		err = fs.readInode(int(dirent.inode), &inode)
		if err != nil {
			return nil, err
		}

		var typ fuse.DirentType
		if (inode.Mode & os.ModeSymlink) != 0 {
			typ = fuse.DT_Link
		} else if (inode.Mode & os.ModeDir) != 0 {
			typ = fuse.DT_Dir
		} else {
			typ = fuse.DT_File
		}

		entries = append(entries, fuse.Dirent{
			Inode: uint64(dirent.inode),
			Type:  typ,
			Name:  dirent.name,
		})
	}

	return entries, nil
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	var inode Inode
	err := d.fs.readInode(d.inode, &inode)
	if err != nil {
		return err
	}

	a.Inode = uint64(d.inode)
	inode.ToAttr(a)
	return nil
}

var _ = fs.NodeMkdirer(&Dir{})
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fmt.Println("mkdir")
	f := d.fs

	var dirInode Inode
	err := f.readInode(d.inode, &dirInode)
	if err != nil {
		return nil, err
	}

	block := [disk.BlockSize]byte{}
	_, err = disk.ReadBlock(f.diskHandle, int(dirInode.Blocks[0]), &block)
	if err != nil {
		return nil, err
	}

	var parentDirents Dirents
	err = parentDirents.Deserialize(block[:])
	if err != nil {
		return nil, err
	}

	inodenr, err := f.InodeAlloc()
	if err != nil {
		return nil, err
	}

	inodeSuccess := false
	// Remove the inode from the bitmap if creation fails.
	defer func() {
		if !inodeSuccess {
			fmt.Println("INODE FAIL")
			_ = writeBitmap(f.inodeBitmap, inodenr, false)
		}
	}()

	inode := Inode{
		Atime:  time.Now(),
		Mtime:  time.Now(),
		Ctime:  time.Now(),
		Mode: req.Mode | os.ModeDir,
		Nlink:  1,
		Size: 0,
	}

	blocknr, err := inode.GetOrAllocData(f, 0)
	if err != nil {
		return nil, err
	}
	dataSuccess := false
	// Remove the data block from the bitmap if creation fails.
	defer func() {
		if !dataSuccess {
			fmt.Println("DATA FAIL")
			_ = writeBitmap(f.dataBitmap, blocknr - FirstData, false)
		}
	}()

	err = f.writeInode(int(inodenr), &inode)
	if err != nil {
		return nil, err
	}

	parentDirents.entries = append(parentDirents.entries, Dirent{
		inode: uint32(inodenr), 
		name: req.Name,
	})

	err = parentDirents.Serialize(block[:])
	if err != nil {
		return nil, err
	}

	_, err = disk.WriteBlock(f.diskHandle, int(dirInode.Blocks[0]), &block)
	if err != nil {
		return nil, err
	}
	
	var dirents Dirents
	err = dirents.Serialize(block[:])
	if err != nil {
		return nil, err
	}

	_, err = disk.WriteBlock(f.diskHandle, int(blocknr), &block)
	if err != nil {
		return nil, err
	}

	dirInode.Atime = time.Now()
	dirInode.Mtime = time.Now()

	file := &File{ f, int(inodenr) }

	dataSuccess = true
	defer f.Sync()

	err = f.writeInode(d.inode, &dirInode)
	if err != nil {
		return nil, err
	}
	inodeSuccess = true

	return file, nil
}

var _ = fs.NodeCreater(&Dir{})
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	f := d.fs

	var dirInode Inode
	err := f.readInode(d.inode, &dirInode)
	if err != nil {
		return nil, nil, err
	}

	block := [disk.BlockSize]byte{}
	_, err = disk.ReadBlock(f.diskHandle, int(dirInode.Blocks[0]), &block)
	if err != nil {
		return nil, nil, err
	}

	var dirents Dirents
	err = dirents.Deserialize(block[:])
	if err != nil {
		return nil, nil, err
	}

	inodenr, err := f.InodeAlloc()
	if err != nil {
		return nil, nil, err
	}

	inodeSuccess := false
	// Remove the inode from the bitmap if creation fails.
	defer func() {
		if !inodeSuccess {
			_ = writeBitmap(f.inodeBitmap, inodenr, false)
		}
	}()

	inode := Inode{
		Atime:  time.Now(),
		Mtime:  time.Now(),
		Ctime:  time.Now(),
		Mode: req.Mode,
		Nlink:  1,
		Size: 0,
	}

	err = f.writeInode(int(inodenr), &inode)
	if err != nil {
		return nil, nil, err
	}

	dirents.entries = append(dirents.entries, Dirent{
		inode: uint32(inodenr), 
		name: req.Name,
	})

	err = dirents.Serialize(block[:])
	if err != nil {
		return nil, nil, err
	}

	_, err = disk.WriteBlock(f.diskHandle, int(dirInode.Blocks[0]), &block)
	if err != nil {
		return nil, nil, err
	}

	dirInode.Atime = time.Now()
	dirInode.Mtime = time.Now()

	file := &File{ f, int(inodenr) }
	fileHandle := &FileHandle{ file, &inode }

	inodeSuccess = true
	defer f.Sync()

	err = f.writeInode(d.inode, &dirInode)
	return file, fileHandle, err
}

type File struct {
	fs    *FS
	inode int
}
var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	var inode Inode
	err := f.fs.readInode(f.inode, &inode)
	if err != nil {
		return err
	}

	a.Inode = uint64(f.inode)
	inode.ToAttr(a)
	return nil
}

var _ = fs.NodeOpener(&File{})
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var inode Inode
	err := f.fs.readInode(f.inode, &inode)
	if err != nil {
		return nil, err
	}
	inode.Atime = time.Now()
	f.fs.writeInode(f.inode, &inode)

	return &FileHandle{f, &inode}, nil
}

type FileHandle struct {
	file  *File
	inode *Inode
}
var _ fs.Handle = (*FileHandle)(nil)

var _ = fs.HandleReader(&FileHandle{})
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	size := uint64(req.Size)
	if size+uint64(req.Offset) > fh.inode.Size {
		size = fh.inode.Size - uint64(req.Offset)
	}

	block := [disk.BlockSize]byte{}
	blocksTotal := (size + uint64(req.Offset) + disk.BlockSize - 1) / disk.BlockSize
	blocksToSkip := uint64(req.Offset) / disk.BlockSize
	blocksToRead := blocksTotal - blocksToSkip
	blockOffset := uint64(req.Offset) % disk.BlockSize

	buf := make([]byte, 0, blocksToRead*disk.BlockSize)
	resp.Data = buf[blockOffset:size]

	fh.inode.Atime = time.Now()
	fh.file.fs.writeInode(fh.file.inode, fh.inode)

	for i := uint64(0); i < blocksToRead; i++ {
		_, err := disk.ReadBlock(fh.file.fs.diskHandle, int(fh.inode.Blocks[i+blocksToSkip]), &block)
		if err != nil {
			return err
		}
		buf = append(buf, block[:]...)
	}

	return nil
}

var _ = fs.HandleWriter(&FileHandle{})
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fmt.Println("write")
	size := uint64(len(req.Data))

	block := [disk.BlockSize]byte{}
	blocksTotal := (size + uint64(req.Offset) + disk.BlockSize - 1) / disk.BlockSize
	blocksToSkip := uint64(req.Offset) / disk.BlockSize
	blocksToRead := blocksTotal - blocksToSkip
	blockOffset := uint64(req.Offset) % disk.BlockSize

	resp.Size = 0
	data := req.Data

	fh.inode.Atime = time.Now()
	fh.inode.Mtime = time.Now()
	fh.file.fs.writeInode(fh.file.inode, fh.inode)

	getOrAllocData := func(n uint64) (uint64, error) {
		return fh.inode.GetOrAllocData(fh.file.fs, n)
	}

	// Special case for writing the first block: first, read the existing block. Then, modify
	// it with the corresponding offset, before writing the entire block back.
	blocknr, err := getOrAllocData(blocksToSkip)
	if err != nil {
		return nil
	}

	defer fh.file.fs.Sync()

	_, err = disk.ReadBlock(fh.file.fs.diskHandle, int(blocknr), &block)
	if err != nil {
		return err
	}

	// Copy to the block while shifting the data slice.
	data = data[copy(block[blockOffset:], data):]

	_, err = disk.WriteBlock(fh.file.fs.diskHandle, int(blocknr), &block)
	if err != nil {
		return err
	}
	resp.Size = int(disk.BlockSize - blockOffset)

	// Write the middle blocks.
	for i := uint64(1); i < blocksToRead-1; i++ {
		blocknr, err := getOrAllocData(blocksToSkip+i)
		if err != nil {
			return nil
		}

		data = data[copy(block[:], data):]
		_, err = disk.WriteBlock(fh.file.fs.diskHandle, int(blocknr), &block)
		if err != nil {
			return err
		}
	}

	if blocksToRead > 1 {
		// Special case for writing the last block: first, read the existing block. Then, modify
		// it up to the size, before writing the entire block back.
		blocknr, err := getOrAllocData(blocksTotal)
		if err != nil {
			return nil
		}

		_, err = disk.ReadBlock(fh.file.fs.diskHandle, int(blocknr), &block)
		if err != nil {
			return err
		}

		copy(block[:], data)

		_, err = disk.WriteBlock(fh.file.fs.diskHandle, int(blocknr), &block)
		if err != nil {
			return err
		}
	}

	fh.file.fs.writeInode(fh.file.inode, fh.inode)
	resp.Size = int(size)

	return nil
}
