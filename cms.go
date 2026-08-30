package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cmsIndicator struct {
	patterns      []string
	minMatches    int
	headerChecks  map[string]string
	metaChecks    []string
	cookieChecks  []string
}

var cmsSignatures = map[string]cmsIndicator{
	"wordpress": {
		patterns:   []string{`wp-content`, `wp-includes`, `wp-json`, `xmlrpc\.php`, `wp-login\.php`, `generator.*wordpress`, `readme\.html.*wordpress`},
		minMatches: 2,
		headerChecks: map[string]string{
			"Link": `rel="https://api.w.org/"`,
			"X-Powered-By": `wordpress`,
		},
		metaChecks:   []string{`name="generator".*wordpress`, `property="og:site_name".*wordpress`},
		cookieChecks: []string{`wordpress_`, `wp-settings-`},
	},
	"joomla": {
		patterns:   []string{`joomla`, `com_content`, `com_users`, `com_config`, `powered.*joomla`, `joomla\.css`, `joomla\.js`},
		minMatches: 2,
		headerChecks: map[string]string{
			"X-Powered-By": `joomla`,
		},
		metaChecks:   []string{`name="generator".*joomla`, `name="joomla"`},
		cookieChecks: []string{`joomla_`, `jfcookie`},
	},
	"drupal": {
		patterns:   []string{`drupal`, `sites/default`, `misc/drupal`, `core/modules`, `powered.*drupal`, `drupal\.js`, `drupal\.css`},
		minMatches: 2,
		headerChecks: map[string]string{
			"X-Powered-By": `drupal`,
			"X-Drupal-Cache": ``,
			"X-Generator": `drupal`,
		},
		metaChecks:   []string{`name="generator".*drupal`, `drupal\.settings`},
		cookieChecks: []string{`sess`, `drupal_`, `has_js`},
	},
	"shopify": {
		patterns:   []string{`checkout\.shopify`, `cdn\.shopify`, `shopify\.theme`, `shopify_checkout`, `myshopify\.com`},
		minMatches: 1,
		headerChecks: map[string]string{
			"X-Shopify-Store": ``,
			"X-Shopify-Stage": ``,
			"Server": `shopify`,
		},
		metaChecks:   []string{`shopify`, `content_for_header`},
		cookieChecks: []string{`_shopify_`, `cart_sig`, `secure_customer_sig`},
	},
	"ghost": {
		patterns:   []string{`ghost\.org`, `ghost\.min\.js`, `ghost\.min\.css`, `/ghost/`, `content/api`},
		minMatches: 1,
		headerChecks: map[string]string{
			"X-Powered-By": `ghost`,
		},
		metaChecks:   []string{`name="generator".*ghost`, `ghost-head`},
		cookieChecks: []string{`ghost`},
	},
	"magento": {
		patterns:   []string{`magento`, `mage/cookies`, `varien`, `magento\.js`, `magento\.css`, `skin/frontend`},
		minMatches: 2,
		headerChecks: map[string]string{
			"X-Magento-Tags": ``,
			"X-Magento-Cache-Control": ``,
		},
		metaChecks:   []string{`name="generator".*magento`},
		cookieChecks: []string{`magento`, `frontend`, `adminhtml`},
	},
	"wix": {
		patterns:   []string{`wix\.com`, `wixstatic`, `_wix`, `wixcode`, `wix-site`},
		minMatches: 1,
		headerChecks: map[string]string{
			"X-Wix-Request-Id": ``,
			"Server": `wix`,
		},
		metaChecks:   []string{`wix`},
		cookieChecks: []string{`wix`},
	},
	"squarespace": {
		patterns:   []string{`squarespace`, `static1\.squarespace`, `squarespace\.com`, `sqsp`},
		minMatches: 1,
		headerChecks: map[string]string{
			"X-Squarespace-Request-Id": ``,
		},
		metaChecks:   []string{`squarespace`},
		cookieChecks: []string{`squarespace`, `ss_cid`, `ss_cvisit`},
	},
}

func detectCMS(body string, headers http.Header, client *http.Client, url string) string {
	bodyLower := strings.ToLower(body)

	type cmsScore struct {
		name  string
		score int
	}

	var scores []cmsScore

	for cms, indicator := range cmsSignatures {
		score := 0

		// Check body patterns
		patternMatches := 0
		for _, pattern := range indicator.patterns {
			matched, _ := regexp.MatchString(pattern, bodyLower)
			if matched {
				patternMatches++
			}
		}

		if patternMatches < indicator.minMatches {
			continue
		}
		score += patternMatches * 10

		// Check headers
		for header, expected := range indicator.headerChecks {
			headerVal := strings.ToLower(headers.Get(header))
			if headerVal != "" {
				if expected == "" || strings.Contains(headerVal, strings.ToLower(expected)) {
					score += 20
				}
			}
		}

		// Check meta tags
		for _, metaPattern := range indicator.metaChecks {
			matched, _ := regexp.MatchString(metaPattern, bodyLower)
			if matched {
				score += 15
			}
		}

		// Check cookies
		cookies := headers.Get("Set-Cookie")
		for _, cookiePattern := range indicator.cookieChecks {
			if strings.Contains(strings.ToLower(cookies), strings.ToLower(cookiePattern)) {
				score += 10
			}
		}

		if score > 0 {
			scores = append(scores, cmsScore{name: cms, score: score})
		}
	}

	// WordPress-specific: check readme.html
	if wpScore := checkWordPressReadme(client, url); wpScore > 0 {
		scores = append(scores, cmsScore{name: "wordpress", score: wpScore})
	}

	if len(scores) == 0 {
		return "unknown"
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	if scores[0].score < 25 {
		return "unknown"
	}

	return scores[0].name
}

func checkWordPressReadme(client *http.Client, baseURL string) int {
	readmeURL := strings.TrimSuffix(baseURL, "/") + "/readme.html"
	req, err := http.NewRequest("GET", readmeURL, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0
	}

	body, _ := io.ReadAll(resp.Body)
	bodyLower := strings.ToLower(string(body))

	if strings.Contains(bodyLower, "wordpress") {
		return 50
	}
	return 0
}

func scanURL(inputURL string, files map[string]*os.File, fileMutex *sync.Mutex, wg *sync.WaitGroup, sem chan struct{}, processed *int64) {
	defer wg.Done()
	sem <- struct{}{}
	defer func() { <-sem }()

	// Auto-convert domain to URL if needed
	url := inputURL
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "https://" + url
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	resp, err := client.Do(req)

	var cmsType string

	if err != nil {
		cmsType = "unknown"
	} else {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		cmsType = detectCMS(string(body), resp.Header, client, url)
	}

	// Write to file
	fileMutex.Lock()
	file := files[cmsType]
	if file == nil {
		file = files["unknown"]
	}
	file.WriteString(url + "\n")
	fileMutex.Unlock()

	// Update counter
	atomic.AddInt64(processed, 1)
	count := atomic.LoadInt64(processed)
	if count%100 == 0 {
		fmt.Printf("\r✓ Processed: %d", count)
	}
}

func main() {
	inputFile := flag.String("f", "", "Input file with URLs (one per line)")
	outputDir := flag.String("o", "./results", "Output directory")
	concurrency := flag.Int("c", 300, "Concurrent requests")
	help := flag.Bool("h", false, "Show help")
	flag.Parse()

	if *help || *inputFile == "" {
		fmt.Println("Usage: cms-detector -f <input_file> -o <output_dir> -c <concurrency>")
		fmt.Println()
		fmt.Println("Options:")
		fmt.Println("  -f string       Input file with URLs (required)")
		fmt.Println("  -o string       Output directory (default: ./results)")
		fmt.Println("  -c int          Concurrent requests (default: 300)")
		fmt.Println("  -h              Show this help message")
		fmt.Println()
		fmt.Println("Example:")
		fmt.Println("  cms-detector -f urls.txt -o ./results -c 500")
		os.Exit(0)
	}

	// Validate input file
	if _, err := os.Stat(*inputFile); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Input file not found: %s\n", *inputFile)
		os.Exit(1)
	}

	// Create output directory
	os.MkdirAll(*outputDir, 0755)

	// Create output files
	files := make(map[string]*os.File)
	for cms := range cmsSignatures {
		fpath := filepath.Join(*outputDir, cms+".txt")
		f, err := os.Create(fpath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Cannot create file %s: %v\n", fpath, err)
			os.Exit(1)
		}
		files[cms] = f
		defer f.Close()
	}

	unknownPath := filepath.Join(*outputDir, "unknown.txt")
	unknownFile, err := os.Create(unknownPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Cannot create file %s: %v\n", unknownPath, err)
		os.Exit(1)
	}
	files["unknown"] = unknownFile
	defer unknownFile.Close()

	// Open input file
	file, err := os.Open(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Cannot open input file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Prepare scanning
	scanner := bufio.NewScanner(file)
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	var fileMutex sync.Mutex
	var processed int64

	fmt.Printf("🔍 Starting CMS detection with %d concurrent requests...\n", *concurrency)
	fmt.Printf("📁 Input: %s\n", *inputFile)
	fmt.Printf("📁 Output: %s\n", *outputDir)
	fmt.Println(strings.Repeat("=", 60))

	// Read and scan URLs (auto-converts domains to URLs)
	for scanner.Scan() {
		url := strings.TrimSpace(scanner.Text())
		if url != "" && !strings.HasPrefix(url, "#") {
			wg.Add(1)
			go scanURL(url, files, &fileMutex, &wg, sem, &processed)
		}
	}

	wg.Wait()
	close(sem)

	// Print summary
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("✅ Scan complete!\n")

	// Count results for each CMS
	for cms := range cmsSignatures {
		fpath := filepath.Join(*outputDir, cms+".txt")
		data, _ := os.ReadFile(fpath)
		trimmed := strings.TrimSpace(string(data))
		count := 0
		if trimmed != "" {
			count = len(strings.Split(trimmed, "\n"))
		}
		if count > 0 {
			fmt.Printf("📊 %s: %d sites\n", strings.ToUpper(cms), count)
		}
	}

	unknownData, _ := os.ReadFile(unknownPath)
	trimmed := strings.TrimSpace(string(unknownData))
	unknownCount := 0
	if trimmed != "" {
		unknownCount = len(strings.Split(trimmed, "\n"))
	}
	if unknownCount > 0 {
		fmt.Printf("📊 UNKNOWN: %d sites\n", unknownCount)
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("📁 Results saved to: %s\n", *outputDir)
}