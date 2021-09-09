package fs

import (
	"fmt"
	"time"

	"github.com/pantherman594/SimpleFileSystem/pkg/disk"
)

type Attributes struct {
	mode  uint
	uid   uint
	gid   uint
	size  uint
	atime time.Time
	mtime time.Time
}

type NfsCookie struct{}

type FileHandle int

type Entry struct {
	FileId    uint
	FileName  string
	Cookie    NfsCookie
	NextEntry *Entry
}

type Handle struct {
	FileId   uint
	FileName string
}

const (
	NumInodeBytes = disk.BlockSize
	NumInodes     = NumInodeBytes * 8
	NumDataBytes  = disk.BlockSize
	NumData       = NumDataBytes * 8

	InodeBlock      = 1
	InodeBlockCount = 1
	DataBlock       = 2
	DataBlockCount  = 1
)

var (
	diskHandle  int
	fileHandles map[int]Handle
	inodeBitmap []byte
	dataBitmap  []byte
)

func Init(handle int) error {
	diskHandle = handle
	fileHandles = make(map[int]Handle)
	inodeBitmap = make([]byte, 0, InodeBlockCount*disk.BlockSize)
	dataBitmap = make([]byte, 0, DataBlockCount*disk.BlockSize)

	// Inode bitmap is in block 1.
	for i := 0; i < InodeBlockCount; i++ {
		bitmapSlice := (*[disk.BlockSize]byte)(dataBitmap[i*disk.BlockSize : (i+1)*disk.BlockSize])
		_, err := disk.WriteBlock(diskHandle, InodeBlock+i, bitmapSlice)
		if err != nil {
			return err
		}
	}

	// Data block bitmap is in block 2.
	for i := 0; i < DataBlockCount; i++ {
		bitmapSlice := (*[disk.BlockSize]byte)(dataBitmap[i*disk.BlockSize : (i+1)*disk.BlockSize])
		_, err := disk.WriteBlock(diskHandle, DataBlock+i, bitmapSlice)
		if err != nil {
			return err
		}
	}

	return nil
}

func Open(handle int) error {
	diskHandle = handle
	fileHandles = make(map[int]Handle)
	inodeBitmap = make([]byte, 0, InodeBlockCount*disk.BlockSize)
	dataBitmap = make([]byte, 0, DataBlockCount*disk.BlockSize)

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

	return nil
}

func readBitmap(bitmap []byte, index int) (bool, error) {
	byteNum := index / 8
	bitNum := index % 8

	if byteNum > len(bitmap) {
		return false, fmt.Errorf("Bitmap index out of bounds.")
	}

	return bitmap[byteNum]&(1<<(7-bitNum)) != 0, nil
}

func writeBitmap(bitmap []byte, index int, value bool) error {
	byteNum := index / 8
	bitNum := index % 8

	if byteNum > len(bitmap) {
		return fmt.Errorf("Bitmap index out of bounds.")
	}

	bitmap[byteNum] |= (1 << (7 - bitNum))
	return nil
}

func Sync() error {

	return nil
}

func GetAttr(f FileHandle) (Attributes, error) {
	return Attributes{}, nil
}

func SetAttr(f FileHandle, attr *Attributes) error {
	return nil
}

func Lookup() {}

func ReadLink() {}

func Read() {}

func WriteCache() {}

func Write() {}

func Create(dir FileHandle, name string, attr Attributes) (FileHandle, Attributes, error) {

	return -1, Attributes{}, nil
}

func Remove() {}

func Rename() {}

func Link() {}

func SymLink() {}

func MkDir() {}

func RmDir() {}

func ReadDir(dir FileHandle, cookie *NfsCookie, count uint) ([]Entry, bool, error) {
	return []Entry{}, false, nil
}

func StatFs() {}
