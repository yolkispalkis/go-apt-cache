//go:build windows

package util

import (
	"os"
	"time"
)

// GetAtime для Windows возвращает время модификации, так как atime не является надежным.
func GetAtime(fi os.FileInfo) time.Time {
	return fi.ModTime()
}
