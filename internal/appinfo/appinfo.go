package appinfo

import "fmt"

// AppName - константа с именем приложения.
const AppName = "go-apt-cache"

// AppVersion - версия приложения, устанавливается при сборке.
var AppVersion = "dev"

// UserAgent возвращает строку User-Agent для HTTP-запросов.
func UserAgent() string {
	return fmt.Sprintf("%s/%s", AppName, AppVersion)
}
