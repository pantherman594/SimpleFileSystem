package disk

import (
	"fmt"
	"os"
)

const (
	BlockSize = 4096
)

var (
	numDisks = 0
	disks    = make([]*os.File, 0)
)

func OpenDisk(filename string, nbytes int) (int, error) {
	openSuccess := false

	f, err := os.Create(filename)
	if err != nil {
		return -1, err
	}

	// Close the file if it was not successfully created and opened.
	defer func() {
		if !openSuccess {
			f.Close()
		}
	}()

	// From https://stackoverflow.com/questions/16797380/how-to-create-a-10mb-file-filled-with-000000-data-in-golang.
	err = f.Truncate(int64(nbytes))
	if err != nil {
		return -1, err
	}

	// Add the disk to the slice, and increment the handle number.
	disks = append(disks, f)
	disk := numDisks
	numDisks += 1

	openSuccess = true

	return disk, nil
}

func ReadBlock(disk int, blocknr int, block *[BlockSize]byte) (int, error) {
	if disk >= numDisks {
		return -1, fmt.Errorf("Invalid disk handle.")
	}

	diskHandle := disks[disk]

	bytesRead, err := diskHandle.ReadAt(block[:], int64(blocknr*BlockSize))
	if err != nil {
		return -1, err
	}

	return bytesRead, nil
}

func WriteBlock(disk int, blocknr int, block *[BlockSize]byte) (int, error) {
	if disk >= numDisks {
		return -1, fmt.Errorf("Invalid disk handle.")
	}

	diskHandle := disks[disk]

	bytesWritten, err := diskHandle.WriteAt(block[:], int64(blocknr*BlockSize))
	if err != nil {
		return -1, err
	}

	return bytesWritten, nil
}

func CloseDisks() {
	for _, disk := range disks {
		err := disk.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}

	numDisks = 0
	disks = make([]*os.File, 0)
}
