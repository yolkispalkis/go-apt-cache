package appinfo

import "fmt"

const (
	AppName    = "go-apt-proxy"
	AppVersion = "2.1.0"
)

func UserAgent() string {
	return fmt.Sprintf("%s/%s", AppName, AppVersion)
}
