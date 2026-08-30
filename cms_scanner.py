import asyncio
import aiohttp
import sys
import re
import os
from typing import Optional


CMS_SIGNATURES = {
    'wordpress': {
        'headers': {r'x-powered-by': r'PHP/[\d.]+', r'server': r'nginx'},
        'body': [r'wp-content', r'wp-includes', r'generator.*wordpress'],
    },
    'joomla': {
        'headers': {},
        'body': [r'joomla', r'com_content', r'powered.*joomla'],
    },
    'drupal': {
        'headers': {},
        'body': [r'drupal', r'sites/default', r'powered.*drupal'],
    },
    'shopify': {
        'headers': {r'x-shopify-store': r'.+'},
        'body': [r'checkout.shopify'],
    },
    'ghost': {
        'headers': {},
        'body': [r'ghost', r'ghost.org'],
    },
}


def detect_cms(text: str, headers: dict) -> Optional[str]:
    text_lower = text.lower()
    for cms, sig in CMS_SIGNATURES.items():
        patterns = []
        for h_pat in sig.get('headers', {}).values():
            patterns.append(('header', h_pat))
        for b_pat in sig.get('body', []):
            patterns.append(('body', b_pat))
        for ftype, pattern in patterns:
            if ftype == 'header':
                for hname, hval in headers.items():
                    if re.search(pattern, str(hval), re.I):
                        return cms
            else:
                if re.search(pattern, text_lower):
                    return cms
    return None


async def scan_url(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    cms_files: dict,
    lock: asyncio.Lock,
    unknown_file,
) -> None:
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                try:
                    text = await resp.text(errors='replace')
                except Exception:
                    text = ''
                detected = detect_cms(text, dict(resp.headers))
                fname = detected or 'unknown'
                fpath = cms_files.get(fname)
                if fpath is None:
                    fpath = unknown_file
                try:
                    fpath.write(url + '\n')
                    fpath.flush()
                except Exception:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                unknown_file.write(url + '\n')
                unknown_file.flush()
            except Exception:
                pass


async def main(input_path: str, output_dir: str, concurrency: int = 200) -> None:
    os.makedirs(output_dir, exist_ok=True)

    cms_files = {}
    for cms_name in CMS_SIGNATURES.keys():
        fpath = os.path.join(output_dir, cms_name + '.txt')
        f = open(fpath, 'w', buffering=1)
        cms_files[cms_name] = f

    unknown_path = os.path.join(output_dir, 'unknown.txt')
    unknown_f = open(unknown_path, 'w', buffering=1)

    lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(concurrency)

    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        idx = 0
        with open(input_path, 'r', errors='replace') as f:
            while True:
                url_line = f.readline()
                if not url_line:
                    break
                url = url_line.strip()
                if not url:
                    continue
                tasks.append(
                    scan_url(session, url, semaphore, cms_files, lock, unknown_f)
                )
                idx += 1
                if idx % concurrency == 0:
                    await asyncio.gather(*tasks[: idx - concurrency + 1])
                    tasks = tasks[idx - concurrency + 1 :]
            if tasks:
                await asyncio.gather(*tasks)

        for f in cms_files.values():
            f.close()
        unknown_f.close()


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python cms_scanner.py <input_urls_file> <output_dir> [concurrency]')
        sys.exit(1)
    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    conc = int(sys.argv[3]) if len(sys.argv) > 3 else 200
    asyncio.run(main(input_file, output_dir, conc))