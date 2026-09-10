package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type APIResponse struct {
	Status  int      `json:"status"`
	Message string   `json:"message"`
	Result  []string `json:"result"`
	Total   int      `json:"total"`
}

type Result struct {
	IP       string
	Domains  []string
	Error    error
	Duration time.Duration
}

type Client struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string
}

func NewClient(timeout time.Duration, apiKey string) *Client {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
	}

	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
		baseURL: "https://api.reverseipdomain.com/",
		apiKey:  apiKey,
	}
}

func (c *Client) Lookup(ctx context.Context, ip string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("ip", ip)
	if c.apiKey != "" {
		q.Add("api_key", c.apiKey)
	}
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp APIResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	if apiResp.Status != 200 {
		return nil, fmt.Errorf("api error: %s", apiResp.Message)
	}

	return apiResp.Result, nil
}

func worker(ctx context.Context, client *Client, jobs <-chan string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for ip := range jobs {
		start := time.Now()
		domains, err := client.Lookup(ctx, ip)
		results <- Result{
			IP:       ip,
			Domains:  domains,
			Error:    err,
			Duration: time.Since(start),
		}
	}
}

func parseIPs(input string) []string {
	var ips []string
	scanner := bufio.NewScanner(strings.NewReader(input))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if ip := net.ParseIP(line); ip != nil {
			ips = append(ips, line)
		}
	}
	return ips
}

type ProgressUI struct {
	totalIPs       int
	processedIPs   int64
	totalDomains   int64
	startTime      time.Time
	lastIP         string
	lastIPMu       sync.Mutex
	outputFile     *os.File
	outputMu       sync.Mutex
	done           chan struct{}
}

func NewProgressUI(totalIPs int, outputPath string) (*ProgressUI, error) {
	f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &ProgressUI{
		totalIPs:   totalIPs,
		startTime:  time.Now(),
		outputFile: f,
		done:       make(chan struct{}),
	}, nil
}

func (p *ProgressUI) WriteDomains(domains []string) {
	p.outputMu.Lock()
	defer p.outputMu.Unlock()
	for _, d := range domains {
		fmt.Fprintln(p.outputFile, d)
	}
}

func (p *ProgressUI) UpdateLastIP(ip string) {
	p.lastIPMu.Lock()
	p.lastIP = ip
	p.lastIPMu.Unlock()
}

func (p *ProgressUI) IncrementProcessed() {
	atomic.AddInt64(&p.processedIPs, 1)
}

func (p *ProgressUI) AddDomains(count int) {
	atomic.AddInt64(&p.totalDomains, int64(count))
}

func (p *ProgressUI) Start() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.render()
		case <-p.done:
			p.renderFinal()
			return
		}
	}
}

func (p *ProgressUI) Stop() {
	close(p.done)
	p.outputFile.Close()
}

func (p *ProgressUI) render() {
	processed := atomic.LoadInt64(&p.processedIPs)
	domains := atomic.LoadInt64(&p.totalDomains)
	elapsed := time.Since(p.startTime)

	p.lastIPMu.Lock()
	lastIP := p.lastIP
	p.lastIPMu.Unlock()

	percent := float64(processed) / float64(p.totalIPs) * 100
	rate := float64(processed) / elapsed.Seconds()
	domainRate := float64(domains) / elapsed.Seconds()
	eta := time.Duration(0)
	if rate > 0 {
		remaining := p.totalIPs - int(processed)
		eta = time.Duration(float64(remaining)/rate) * time.Second
	}

	barWidth := 40
	filled := int(float64(barWidth) * percent / 100)
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	fmt.Fprintf(os.Stderr, "\033[2J\033[H[%s] %.1f%% | %d/%d IPs | %d domains | %.1f IP/s | %.0f dom/s | ETA: %v | Last: %s",
		bar, percent, processed, p.totalIPs, domains, rate, domainRate, eta.Round(time.Second), lastIP)
}

func (p *ProgressUI) renderFinal() {
	processed := atomic.LoadInt64(&p.processedIPs)
	domains := atomic.LoadInt64(&p.totalDomains)
	elapsed := time.Since(p.startTime)

	bar := strings.Repeat("█", 40)
	fmt.Fprintf(os.Stderr, "\033[2J\033[H[%s] 100.0%% | %d/%d IPs | %d domains | %.1f IP/s | %.0f dom/s | Done in %v\n",
		bar, processed, p.totalIPs, domains, float64(processed)/elapsed.Seconds(), float64(domains)/elapsed.Seconds(), elapsed.Round(time.Second))
}

func main() {
	var (
		ipInput     = flag.String("ip", "", "Single IP or comma-separated IPs")
		fileInput   = flag.String("file", "", "File containing IPs (one per line)")
		concurrency = flag.Int("c", runtime.NumCPU()*4, "Number of concurrent workers")
		timeout     = flag.Duration("timeout", 30*time.Second, "HTTP timeout per request")
		apiKey      = flag.String("key", "", "API key (optional)")
		rateLimit   = flag.Int("rate", 0, "Rate limit (requests/sec, 0 = unlimited)")
		outputFile  = flag.String("out", "result.txt", "Output file for domains")
	)
	flag.Parse()

	var ips []string
	if *ipInput != "" {
		for _, ip := range strings.Split(*ipInput, ",") {
			ip = strings.TrimSpace(ip)
			if net.ParseIP(ip) != nil {
				ips = append(ips, ip)
			}
		}
	}
	if *fileInput != "" {
		data, err := os.ReadFile(*fileInput)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
			os.Exit(1)
		}
		ips = append(ips, parseIPs(string(data))...)
	}
	if len(ips) == 0 && flag.NArg() > 0 {
		ips = append(ips, flag.Args()...)
	}

	if len(ips) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: reverseip -ip 1.1.1.1 [-file ips.txt] [-c 100] [-out result.txt]")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(*timeout, *apiKey)

	jobs := make(chan string, len(ips))
	results := make(chan Result, len(ips))

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(ctx, client, jobs, results, &wg)
	}

	go func() {
		var limiter <-chan time.Time
		if *rateLimit > 0 {
			limiter = time.Tick(time.Second / time.Duration(*rateLimit))
		}
		for _, ip := range ips {
			if limiter != nil {
				<-limiter
			}
			select {
			case jobs <- ip:
			case <-ctx.Done():
				close(jobs)
				return
			}
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	ui, err := NewProgressUI(len(ips), *outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}

	go ui.Start()

	for r := range results {
		ui.UpdateLastIP(r.IP)
		ui.IncrementProcessed()
		if r.Error == nil && len(r.Domains) > 0 {
			ui.AddDomains(len(r.Domains))
			ui.WriteDomains(r.Domains)
		}
	}

	ui.Stop()
}