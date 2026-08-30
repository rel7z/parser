import { ApifyClient } from 'apify-client';
import fs from 'fs';

const API_TOKEN = "apify_api_87e0keim4LdlPQcNn2p3c3ROlyEAJN2k2cZL"

if (!API_TOKEN || API_TOKEN === '<YOUR_API_TOKEN>') {
    console.error('Please set API_TOKEN environment variable');
    process.exit(1);
}

const args = process.argv.slice(2);

let help = false;
for (const arg of args) {
    if (arg === '-h' || arg === '--help') help = true;
}

if (help) {
    console.log('Usage: node index.js [options]');
    console.log('');
    console.log('Options:');
    console.log('  -h, --help         Show this help message');
    console.log('  -f <file>          Path to dork list .txt file');
    console.log('  -p, --pages <n>    Number of pages to scrape (0 = unlimited)');
    console.log('  -k <keyword>       Search keyword (alternative to -f)');
    process.exit(0);
}

let keywords = [];
let pages = 0;

for (let i = 0; i < args.length; i++) {
    if (args[i] === '-f' || args[i] === '--file') {
        const filePath = args[++i];
        const data = fs.readFileSync(filePath, 'utf-8');
        keywords = data.split('\n').filter(k => k.trim());
    } else if (args[i] === '-p' || args[i] === '--pages') {
        pages = parseInt(args[++i]) || 0;
    } else if (args[i] === '-k') {
        keywords = [args[++i]];
    }
}

if (keywords.length === 0) {
    console.error('Please specify keywords via -f <file> or -k <keyword>');
    process.exit(1);
}

const client = new ApifyClient({
    token: API_TOKEN,
    timeoutSecs: 300,
});

// Create write streams for real-time saving
const domainsStream = fs.createWriteStream('domains.txt', { flags: 'a' });
const urlsStream = fs.createWriteStream('urls.txt', { flags: 'a' });

// Track unique domains and URLs
const seenDomains = new Set();
const seenUrls = new Set();

let totalDomains = 0;
let totalUrls = 0;

// Function to extract hostname safely
function getHostname(link) {
    try {
        if (!link || link === 'N/A' || typeof link !== 'string') return null;
        const url = new URL(link.trim());
        return url.hostname;
    } catch (e) {
        return null;
    }
}

// Function to save domain in real-time
function saveDomain(domain) {
    if (domain && !seenDomains.has(domain)) {
        seenDomains.add(domain);
        domainsStream.write(domain + '\n');
        totalDomains++;
        process.stdout.write(`\r✓ Domains: ${totalDomains} | URLs: ${totalUrls}`);
    }
}

// Function to save URL in real-time
function saveUrl(url) {
    if (url && url !== 'N/A' && !seenUrls.has(url) && typeof url === 'string') {
        seenUrls.add(url);
        urlsStream.write(url + '\n');
        totalUrls++;
        process.stdout.write(`\r✓ Domains: ${totalDomains} | URLs: ${totalUrls}`);
    }
}

(async () => {
    try {
        // Clear files at start
        fs.writeFileSync('domains.txt', '');
        fs.writeFileSync('urls.txt', '');
        console.log('Starting real-time scraping...\n');

        for (const keyword of keywords) {
            let remainingPages = pages;
            let nextPageKeyword = keyword;

            console.log(`\n📍 Processing keyword: "${keyword}"`);
            console.log('='.repeat(60));

            do {
                try {
                    const input = {
                        keyword: nextPageKeyword,
                        include_merged: true,
                        limit: 'all',
                    };

                    const run = await client.actor('563JCPLOqM1kMmbbP').call(input);
                    const { items } = await client.dataset(run.defaultDatasetId).listItems();

                    if (items.length === 0) {
                        console.log('\n⚠ No results found for this page.');
                        break;
                    }

                    items.forEach((item) => {
                        const results = item.results || [];
                        results.forEach((result) => {
                            const link = result.url;
                            
                            // Save URL
                            saveUrl(link);
                            
                            // Extract and save domain
                            const domain = getHostname(link);
                            if (domain) {
                                saveDomain(domain);
                            }
                        });
                    });

                    if (remainingPages > 0) {
                        remainingPages--;
                        // Get next page keyword if available
                        const nextItems = await client.dataset(run.defaultDatasetId).listItems({ 
                            cursor: items[items.length - 1]?.next 
                        });
                        if (nextItems.items && nextItems.items.length > 0) {
                            nextPageKeyword = nextItems.items[0].keyword;
                        } else {
                            break;
                        }
                    } else if (pages > 0) {
                        break;
                    }
                } catch (pageError) {
                    console.error('\n❌ Page error:', pageError.message);
                    break;
                }

            } while (true);
        }

        // Close streams
        domainsStream.end();
        urlsStream.end();

        // Wait for streams to finish writing
        await new Promise((resolve) => {
            domainsStream.on('finish', () => {
                urlsStream.on('finish', resolve);
            });
        });

        console.log('\n\n' + '='.repeat(60));
        console.log('✅ Scraping complete!');
        console.log(`📊 Total domains saved: ${totalDomains}`);
        console.log(`📊 Total URLs saved: ${totalUrls}`);
        console.log('📁 Saved to: domains.txt and urls.txt');
        console.log('='.repeat(60));
        
    } catch (err) {
        console.error('\n❌ Error:', err.message);
        domainsStream.destroy();
        urlsStream.destroy();
        process.exit(1);
    }
})();