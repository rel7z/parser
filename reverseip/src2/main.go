package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	baseURL        = "https://ip.thc.org/"
	defaultWorkers = 50
	defaultLimit   = 100 // max results per IP (API max)
	requestTimeout = 15 * time.Second
)

type result struct {
	ip      string
	domains []string
	err     error
}

func main() {
	inputFile := flag.String("f", "", "Path to .txt file with one IP per line (required)")
	outputFile := flag.String("o", "", "Output file to write results (default: stdout)")
	workers := flag.Int("w", defaultWorkers, "Number of concurrent workers")
	limit := flag.Int("l", defaultLimit, "Max domains to return per IP (1-100)")
	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintln(os.Stderr, "Usage: reverseip -f ips.txt [-o output.txt] [-w 50] [-l 100]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ips, err := readLines(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}

	var out io.Writer = os.Stdout
	if *outputFile != "" {
		f, err := os.Create(*outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		out = f
	}

	fmt.Fprintf(os.Stderr, "[*] Loaded %d IPs — starting lookup with %d workers\n", len(ips), *workers)

	jobs := make(chan string, len(ips))
	results := make(chan result, len(ips))

	client := &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        *workers,
			MaxIdleConnsPerHost: *workers,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	// Spin up workers
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ip := range jobs {
				domains, err := lookup(client, ip, *limit)
				results <- result{ip: ip, domains: domains, err: err}
			}
		}()
	}

	// Feed jobs
	for _, ip := range ips {
		jobs <- ip
	}
	close(jobs)

	// Close results once all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and write results
	var done, failed int64
	total := len(ips)
	writer := bufio.NewWriterSize(out, 1<<20) // 1 MB write buffer

	for r := range results {
		atomic.AddInt64(&done, 1)
		if r.err != nil {
			atomic.AddInt64(&failed, 1)
			fmt.Fprintf(os.Stderr, "[-] %s: %v\n", r.ip, r.err)
			continue
		}
		if len(r.domains) == 0 {
			fmt.Fprintf(writer, "%s: (no results)\n", r.ip)
		} else {
			fmt.Fprintf(writer, "%s: %s\n", r.ip, strings.Join(r.domains, ", "))
		}
		// Progress every 100 IPs
		if d := atomic.LoadInt64(&done); d%100 == 0 {
			fmt.Fprintf(os.Stderr, "[*] Progress: %d/%d\n", d, total)
		}
	}

	writer.Flush()
	fmt.Fprintf(os.Stderr, "[+] Done. %d/%d succeeded, %d failed.\n", int64(total)-failed, total, failed)
}

// lookup queries ip.thc.org for domains associated with the given IP.
func lookup(client *http.Client, ip string, limit int) ([]string, error) {
	url := fmt.Sprintf("%s%s?l=%d&nocolor=1&noheader=1", baseURL, ip, limit)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "reverseip-tool/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited (429)")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return parseResponse(resp.Body), nil
}

// parseResponse extracts domain names from the plain-text response body.
// The API (with noheader=1) returns one domain per line; lines starting
// with ";" are metadata/comments and are skipped.
func parseResponse(r io.Reader) []string {
	var domains []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}
		domains = append(domains, line)
	}
	return domains
}

// readLines reads non-empty, non-comment lines from a file.
func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	return lines, scanner.Err()
}
