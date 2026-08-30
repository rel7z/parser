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
	workers       = 20000
	pingTimeout   = 800 * time.Millisecond
	portTimeout   = 3 * time.Second
	batchSize     = 50000
	writeBuffer   = 2 * 1024 * 1024
)

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

	runtime.GOMAXPROCS(runtime.NumCPU() * 4)

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
		go worker(ipChan, pingWriter, portWriter, &wg)
	}

	go statsReporter()

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

func worker(ipChan <-chan string, pingWriter, portWriter *bufio.Writer, wg *sync.WaitGroup) {
	defer wg.Done()

	pingMu := &sync.Mutex{}
	portMu := &sync.Mutex{}

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