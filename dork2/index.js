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
    console.log('Usage: node google-search-scraper.js [options]');
    console.log('');
    console.log('Options:');
    console.log('  -h, --help              Show this help message');
    console.log('  -f <file>               Path to queries .txt file (one per line)');
    console.log('  -k <query>              Single search query');
    console.log('  -p, --pages <n>         Max pages per query (default: 5)');
    console.log('  -c, --country <code>    Country code: us, uk, ca, de, etc (default: us)');
    console.log('  -l, --lang <code>       Language code: en, de, fr, es, etc (default: en)');
    console.log('  -m, --mobile            Enable mobile results (default: false)');
    console.log('  -s, --site <domain>     Limit results to specific site');
    console.log('  -ai                     Enable AI overview (Gemini) (default: false)');
    process.exit(0);
}

let queries = [];
let maxPages = 5;
let countryCode = 'us';
let searchLanguage = 'en';
let languageCode = 'en';
let mobileResults = false;
let enableAiOverview = false;
let siteFilter = '';

for (let i = 0; i < args.length; i++) {
    if (args[i] === '-f' || args[i] === '--file') {
        const filePath = args[++i];
        const data = fs.readFileSync(filePath, 'utf-8');
        queries = data.split('\n').filter(q => q.trim());
    } else if (args[i] === '-k') {
        queries = [args[++i]];
    } else if (args[i] === '-p' || args[i] === '--pages') {
        maxPages = parseInt(args[++i]) || 5;
    } else if (args[i] === '-c' || args[i] === '--country') {
        countryCode = args[++i].toLowerCase();
    } else if (args[i] === '-l' || args[i] === '--lang') {
        searchLanguage = args[++i].toLowerCase();
        languageCode = args[++i].toLowerCase();
    } else if (args[i] === '-m' || args[i] === '--mobile') {
        mobileResults = true;
    } else if (args[i] === '-s' || args[i] === '--site') {
        siteFilter = args[++i];
    } else if (args[i] === '-ai') {
        enableAiOverview = true;
    }
}

if (queries.length === 0) {
    console.error('Please specify queries via -f <file> or -k <query>');
    process.exit(1);
}

const client = new ApifyClient({
    token: API_TOKEN,
    timeoutSecs: 600,
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
        if (!link || typeof link !== 'string') return null;
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
    if (url && !seenUrls.has(url) && typeof url === 'string') {
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
        console.log('🔍 Starting Google Search scraping...\n');

        for (const query of queries) {
            console.log(`\n📍 Processing query: "${query}"`);
            console.log('='.repeat(60));

            try {
                // Build the search query with site filter if provided
                let searchQuery = query;
                if (siteFilter) {
                    searchQuery = `${query} site:${siteFilter}`;
                }

                // Prepare Actor input
                const input = {
                    queries: searchQuery,
                    maxPagesPerQuery: maxPages,
                    countryCode: countryCode,
                    searchLanguage: searchLanguage,
                    languageCode: languageCode,
                    mobileResults: mobileResults,
                    focusOnPaidAds: false,
                    includeUnfilteredResults: true,
                    aiOverview: {
                        scrapeFullAiOverview: enableAiOverview
                    },
                    aiModeSearch: {
                        enableAiMode: false
                    },
                    geminiSearch: {
                        enableGemini: enableAiOverview
                    },
                    perplexitySearch: {
                        enablePerplexity: false
                    },
                    chatGptSearch: {
                        enableChatGpt: false
                    },
                    copilotSearch: {
                        enableCopilot: false
                    },
                    websiteContentScraper: {
                        enable: false
                    },
                    saveHtml: false,
                    saveHtmlToKeyValueStore: false,
                    includeIcons: false
                };

                // Run the Actor
                const run = await client.actor('nFJndFXA5zjCTuudP').call(input);
                const { items } = await client.dataset(run.defaultDatasetId).listItems();

                if (items.length === 0) {
                    console.log('\n⚠ No results found for this query.');
                    continue;
                }

                // Process results
                items.forEach((item) => {
                    // Handle regular search results
                    if (item.organicResults && Array.isArray(item.organicResults)) {
                        item.organicResults.forEach((result) => {
                            if (result.url) {
                                saveUrl(result.url);
                                const domain = getHostname(result.url);
                                if (domain) {
                                    saveDomain(domain);
                                }
                            }
                        });
                    }

                    // Handle paid ads results
                    if (item.paidResults && Array.isArray(item.paidResults)) {
                        item.paidResults.forEach((result) => {
                            if (result.url) {
                                saveUrl(result.url);
                                const domain = getHostname(result.url);
                                if (domain) {
                                    saveDomain(domain);
                                }
                            }
                        });
                    }

                    // Handle news results
                    if (item.newsResults && Array.isArray(item.newsResults)) {
                        item.newsResults.forEach((result) => {
                            if (result.url) {
                                saveUrl(result.url);
                                const domain = getHostname(result.url);
                                if (domain) {
                                    saveDomain(domain);
                                }
                            }
                        });
                    }

                    // Handle knowledge panel results
                    if (item.knowledgePanel && item.knowledgePanel.url) {
                        const url = item.knowledgePanel.url;
                        saveUrl(url);
                        const domain = getHostname(url);
                        if (domain) {
                            saveDomain(domain);
                        }
                    }

                    // Handle related searches
                    if (item.relatedSearches && Array.isArray(item.relatedSearches)) {
                        item.relatedSearches.forEach((search) => {
                            if (search.url) {
                                saveUrl(search.url);
                                const domain = getHostname(search.url);
                                if (domain) {
                                    saveDomain(domain);
                                }
                            }
                        });
                    }
                });

            } catch (queryError) {
                console.error(`\n❌ Query error for "${query}":`, queryError.message);
                continue;
            }
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
        console.error('\n❌ Fatal error:', err.message);
        domainsStream.destroy();
        urlsStream.destroy();
        process.exit(1);
    }
})();