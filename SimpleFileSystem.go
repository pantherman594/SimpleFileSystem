package main

import (
	"fmt"
	"log"

	"github.com/pantherman594/SimpleFileSystem/pkg/disk"
	"github.com/pantherman594/SimpleFileSystem/pkg/fs"
)

func main() {
	defer disk.CloseDisks()

	diskHandle, err := disk.OpenDisk("test", 4096*64)
	if err != nil {
		log.Fatal(err)
	}

	source := [disk.BlockSize]byte{0x11, 0x22, 0x33}
	dest := [disk.BlockSize]byte{}

	bw, err := disk.WriteBlock(diskHandle, 0, &source)
	if err != nil {
		log.Fatal(err)
	} else if bw != disk.BlockSize {
		log.Fatalf("Unexpected blocks written: %d.", bw)
	}

	_, err = disk.WriteBlock(diskHandle, 5, &source)
	if err != nil {
		log.Fatal(err)
	}

	_, err = disk.ReadBlock(diskHandle, 5, &dest)
	if err != nil {
		log.Fatal(err)
	}

	if !blocksAreEqual(&source, &dest) {
		fmt.Println("1 Not equal.")
	}

	_, err = disk.ReadBlock(diskHandle, 3, &dest)
	if err != nil {
		log.Fatal(err)
	}

	if !blocksAreEqual(&source, &dest) {
		fmt.Println("2 Not equal.")
	}

	diskHandle, err = disk.OpenDisk("testfs", 4096*128)
	if err != nil {
		log.Fatal(err)
	}

	err = fs.Init(diskHandle, "m/mnt34")
	if err != nil {
		log.Fatal(err)
	}
}

func blocksAreEqual(a, b *[disk.BlockSize]byte) bool {
	for i := 0; i < disk.BlockSize; i += 1 {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
