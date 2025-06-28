//go:build linux

package util

import (
	"os"
	"syscall"
	"time"
)

// GetAtime возвращает время последнего доступа к файлу (atime) для Linux.
func GetAtime(fi os.FileInfo) time.Time {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fi.ModTime()
	}
	return time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
}
