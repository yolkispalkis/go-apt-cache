package appinfo

import "fmt"

const AppName = "go-apt-proxy"

var AppVersion = "dev"

func UserAgent() string {
	return fmt.Sprintf("%s/%s", AppName, AppVersion)
}
