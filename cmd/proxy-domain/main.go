package main

import (
	"fmt"
	"os"

	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

func main() {
	// Check if proxy is provided as argument
	if len(os.Args) < 2 {
		// Try to get proxy from environment variables
		httpProxy := os.Getenv("HTTP_PROXY")
		httpsProxy := os.Getenv("HTTPS_PROXY")

		if httpProxy != "" {
			fmt.Printf("HTTP_PROXY domain: %s\n", utils.GetProxyDomain(httpProxy))
		}
		if httpsProxy != "" {
			fmt.Printf("HTTPS_PROXY domain: %s\n", utils.GetProxyDomain(httpsProxy))
		}

		if httpProxy == "" && httpsProxy == "" {
			fmt.Println("No proxy found in environment variables.")
			fmt.Println("Usage: proxy-domain [proxy-url]")
			fmt.Println("Example: proxy-domain http://user:pass@proxy.example.com:8080")
			os.Exit(1)
		}
		return
	}

	// Get proxy URL from command line argument
	proxyURL := os.Args[1]

	// Extract and print only the domain part
	domain := utils.GetProxyDomain(proxyURL)
	fmt.Println(domain)
}
