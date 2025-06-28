//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package util

import (
	"os"
	"syscall"
	"time"
)

// GetAtime возвращает время последнего доступа к файлу (atime) для Darwin/BSD систем.
func GetAtime(fi os.FileInfo) time.Time {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fi.ModTime()
	}
	return time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec)
}
