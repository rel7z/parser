package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	pingTimeout = 800 * time.Millisecond
	portTimeout = 3 * time.Second
	batchSize   = 10000
	writeBuffer = 512 * 1024
	flushEvery  = 500 * time.Millisecond
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var workers = func() int {
	cpus := runtime.NumCPU()
	// Keep goroutine count sane for blocking network I/O.
	// Each blocked DialTimeout causes Go to spawn an OS thread;
	// too many threads → "failed to create new OS thread".
	// 500 concurrent dials is already very aggressive.
	w := cpus * 500
	if w > 1000 {
		return 1000
	}
	if w < 500 {
		return 500
	}
	return w
}()

var (
	processed  uint64
	validPing  uint64
	openPort80 uint64
)

func main() {
	inputFile := flag.String("f", "ips.txt", "Input file with IPs")
	pingOut := flag.String("o", "ping.txt", "Output file for valid ping")
	portOut := flag.String("p", "60.txt", "Output file for port 80 open")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var pingMu sync.Mutex
	var portMu sync.Mutex

	f, err := os.Open(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	pingFile, err := os.Create(*pingOut)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating ping output: %v\n", err)
		os.Exit(1)
	}
	defer pingFile.Close()

	portFile, err := os.Create(*portOut)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating port output: %v\n", err)
		os.Exit(1)
	}
	defer portFile.Close()

	pingWriter := bufio.NewWriterSize(pingFile, writeBuffer)
	portWriter := bufio.NewWriterSize(portFile, writeBuffer)

	ipChan := make(chan string, workers*4)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(ipChan, pingWriter, portWriter, &wg, &pingMu, &portMu)
	}

	go statsReporter()

	// Flush output files to disk periodically so results appear in real-time.
	go func() {
		ticker := time.NewTicker(flushEvery)
		defer ticker.Stop()
		for range ticker.C {
			pingMu.Lock()
			pingWriter.Flush()
			pingMu.Unlock()
			portMu.Lock()
			portWriter.Flush()
			portMu.Unlock()
		}
	}()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	batch := make([]string, 0, batchSize)
	for scanner.Scan() {
		ip := scanner.Text()
		if ip == "" {
			continue
		}
		batch = append(batch, ip)
		if len(batch) >= batchSize {
			for _, ip := range batch {
				ipChan <- ip
			}
			batch = batch[:0]
		}
	}

	for _, ip := range batch {
		ipChan <- ip
	}
	close(ipChan)

	wg.Wait()
	pingWriter.Flush()
	portWriter.Flush()

	fmt.Fprintf(os.Stderr, "\nDone. Processed: %d, Valid ping: %d, Port 80 open: %d\n",
		atomic.LoadUint64(&processed), atomic.LoadUint64(&validPing), atomic.LoadUint64(&openPort80))
}

func worker(ipChan <-chan string, pingWriter, portWriter *bufio.Writer, wg *sync.WaitGroup, pingMu, portMu *sync.Mutex) {
	defer wg.Done()

	for ip := range ipChan {
		atomic.AddUint64(&processed, 1)

		if tcpPing(ip) {
			atomic.AddUint64(&validPing, 1)
			pingMu.Lock()
			pingWriter.WriteString(ip + "\n")
			pingMu.Unlock()

			if checkPort80(ip) {
				atomic.AddUint64(&openPort80, 1)
				portMu.Lock()
				portWriter.WriteString(ip + "\n")
				portMu.Unlock()
			}
		}
	}
}

func tcpPing(ip string) bool {
	for _, port := range []string{"80", "443", "8080", "22", "21", "25", "53", "110", "143", "993", "995", "3306", "3389", "5432", "5900", "8000", "8443", "8888"} {
		conn, err := net.DialTimeout("tcp", ip+":"+port, pingTimeout)
		if err == nil {
			conn.Close()
			return true
		}
	}
	return false
}

func checkPort80(ip string) bool {
	conn, err := net.DialTimeout("tcp", ip+":80", portTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func statsReporter() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		p := atomic.LoadUint64(&processed)
		v := atomic.LoadUint64(&validPing)
		o := atomic.LoadUint64(&openPort80)
		fmt.Fprintf(os.Stderr, "\rProcessed: %d | TCP Ping OK: %d | Port 80 open: %d", p, v, o)
	}
}