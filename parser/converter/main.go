package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

func main() {
	inputFile := flag.String("f", "wordpress.txt", "Input file containing domains")
	flag.Parse()

	file, err := os.Open(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	var domains []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		domain := strings.TrimPrefix(line, "https://")
		domain = strings.TrimPrefix(domain, "http://")
		domains = append(domains, domain)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)
	ipPrefixes := make(map[string]bool)
	var mu sync.Mutex

	for _, domain := range domains {
		wg.Add(1)
		sem <- struct{}{}
		go func(d string) {
			defer wg.Done()
			defer func() { <-sem }()

			ips, err := net.LookupIP(d)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not resolve: %s\n", d)
				return
			}

			for _, ip := range ips {
				if ip4 := ip.To4(); ip4 != nil {
					parts := strings.Split(ip4.String(), ".")
					if len(parts) == 4 {
						prefix := strings.Join(parts[:3], ".")
						mu.Lock()
						ipPrefixes[prefix] = true
						mu.Unlock()
					}
				}
			}
		}(domain)
	}

	wg.Wait()

	for prefix := range ipPrefixes {
		for i := 1; i <= 255; i++ {
			fmt.Printf("%s.%d\n", prefix, i)
		}
	}
}