package utils

import (
	"os"
	"syscall"
)

// TruncateFile truncate file,discard [0,startOffset), and keep [startOffset,)
func TruncateFile(file *os.File, startOffset int64) error {
	f, err := file.Stat()
	if err != nil {
		return err
	}
	if f.Size() <= startOffset {
		// don't need to truncate
		return nil
	}
	mem, err := syscall.Mmap(int(file.Fd()), 0, int(f.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	// mem[startOffset:] override mem[0:]
	copy(mem[0:], mem[startOffset:])
	err = syscall.Munmap(mem)
	if err != nil {
		return err
	}
	err = file.Truncate(f.Size() - startOffset)
	if err != nil {
		return err
	}
	_, err = file.Seek(startOffset, 0)
	return err
}
