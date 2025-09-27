import os
import re
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

import requests

PLAYLIST_URLS = [
    "http://epg.one/edem_epg_ico.m3u8",
    "http://epg.one/edem_epg_ico2.m3u8",
    "http://epg.one/edem_epg_ico3.m3u8"
]

OUTPUT_DIR = "playlists"
ICONS_SQR_DIR = Path("img")
ICONS_RECT_DIR = Path("img2")
NEW_ICONS_BASE_URL_SQR = "https://raw.githubusercontent.com/KocourKuba/epgone_mirror/master/img/"
PLAYLIST_DOWNLOAD_TIMEOUT = 60
ICONS_DOWNLOAD_TIMEOUT = 300
ICON_DOWNLOAD_WORKERS = 50


class TimeoutHandler:
    def __init__(self):
        self.is_timeout = False

    def handler(self, signum, frame):
        self.is_timeout = True
        print(f"\nTimeout signal ({signum}). Aborting...")


timeout_handler = TimeoutHandler()


@contextmanager
def timeout_context(seconds):
    old_handler = signal.signal(signal.SIGALRM, timeout_handler.handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def main():
    print("Run script...")

    repo_name = os.getenv('GITHUB_REPOSITORY')
    if not repo_name:
        raise ValueError("Error: Environment variable GITHUB_REPOSITORY is not set.")

    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(exist_ok=True)
    print(f"Output directory: '{OUTPUT_DIR}'")

    ICONS_SQR_DIR.mkdir(exist_ok=True)
    print(f"img directory: '{ICONS_SQR_DIR.name}'")

    ICONS_RECT_DIR.mkdir(exist_ok=True)
    print(f"img2 directory: '{ICONS_RECT_DIR.name}'")

    processed_files = []
    original_icons = {}
    for url in PLAYLIST_URLS:
        filename = os.path.basename(urlparse(url).path)
        print(f"\n--- Processing: {filename} ---")
        try:
            print(f"Download: {url}")
            response = requests.get(url, timeout=PLAYLIST_DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            playlist_lines = response.text.splitlines()

            replacements_count = 0
            not_replaced_count = 0
            processed_lines = []
            for line in playlist_lines:
                if 'tvg-logo="' in line:
                    match = re.search(r'tvg-logo="(https?://epg\.one/img/([^"]+))"', line)
                    if match:
                        old_icon_url = match.group(1)
                        icon_path = match.group(2)
                        original_icons[old_icon_url] = ICONS_SQR_DIR / icon_path
                        original_icons[old_icon_url.replace("/img/", "/img2/")] = ICONS_RECT_DIR / icon_path
                        new_icon_url = f"{NEW_ICONS_BASE_URL_SQR}{icon_path}"
                        line = line.replace(old_icon_url, new_icon_url)
                        replacements_count += 1
                    else:
                        print(f"External icon url: {line}")
                        not_replaced_count += 1

                processed_lines.append(line)

            if replacements_count > 0:
                print(f"Replaced urls: {replacements_count}")
            else:
                print("No replacements found.")

            print(f"External urls (not changed): {not_replaced_count}")

            playlist_content = "\n".join(processed_lines)

            file_path = output_path / filename
            file_path.write_text(playlist_content, encoding='utf-8')
            print(f"saved: {file_path}")
            processed_files.append(filename)

        except requests.RequestException as e:
            print(f"Error processing: {url}: {e}")

    if len(original_icons):
        run_download_worker(original_icons)

    print("\nDone!")


def download_icon_batch(session, batch_items):
    success = 0
    for url, save_path in batch_items:
        if timeout_handler.is_timeout:
            break
        try:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with session.get(url, stream=True, timeout=ICONS_DOWNLOAD_TIMEOUT) as r:
                r.raise_for_status()
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            success += 1
        except:
            continue
    return success

def run_download_worker(icons):
    print(f"Icons to download: {len(icons)}")
    items = list(icons.items())
    batch_size = 50
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    total_downloaded = 0
    start_time = time.time()

    with timeout_context(ICONS_DOWNLOAD_TIMEOUT):
        with requests.Session() as session:
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=ICON_DOWNLOAD_WORKERS,
                pool_maxsize=ICON_DOWNLOAD_WORKERS,
                max_retries=1
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            with ThreadPoolExecutor(max_workers=ICON_DOWNLOAD_WORKERS) as executor:
                future_to_batch = {
                    executor.submit(download_icon_batch, session, batch): i
                    for i, batch in enumerate(batches)
                }

                for future in as_completed(future_to_batch, timeout=ICONS_DOWNLOAD_TIMEOUT):
                    if timeout_handler.is_timeout:
                        break
                    try:
                        batch_idx = future_to_batch[future]
                        successful = future.result(timeout=60)
                        total_downloaded += successful

                        elapsed = time.time() - start_time
                        progress = ((batch_idx + 1) / len(batches)) * 100
                        print(f"Processed: {progress:.1f}% | Downloaded: {total_downloaded} | Time: {elapsed:.0f}с")

                    except TimeoutError:
                        print(f"Batch timeout {batch_idx + 1}")
                    except Exception as e:
                        print(f"Batch error: {e}")

    print(f"Download icons done: {total_downloaded} из {len(icons)}")

if __name__ == "__main__":
    main()
