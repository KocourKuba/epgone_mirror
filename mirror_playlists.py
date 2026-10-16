# thanks for ideas Lorax121 https://github.com/Lorax121/epg_v2

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from pathlib import Path
from urllib.parse import urlparse

import requests

# The epg.one CDN over HTTPS (https://epg.one itself redirects to plaintext http://cdn.epg.one)
PLAYLIST_URLS = [
    "https://cdn.epg.one/edem_epg_ico.m3u8",
    "https://cdn.epg.one/edem_epg_ico2.m3u8",
    "https://cdn.epg.one/edem_epg_ico3.m3u8"
]

OUTPUT_DIR = "playlists"
ICONS_SQR_DIR = Path("img")
ICONS_RECT_DIR = Path("img2")
NEW_ICONS_BASE_URL_SQR = "https://raw.githubusercontent.com/KocourKuba/epgone_mirror/master/img/"
# Secure origin used to download icons that the playlists reference via http://epg.one/...
ICONS_SOURCE_BASE = "https://cdn.epg.one/"
PLAYLIST_DOWNLOAD_TIMEOUT = 60
PLAYLIST_DOWNLOAD_RETRIES = 3
ICONS_DOWNLOAD_TIMEOUT = 300
ICON_DOWNLOAD_WORKERS = 50


def download_playlist(url):
    for attempt in range(1, PLAYLIST_DOWNLOAD_RETRIES + 1):
        try:
            response = requests.get(url, timeout=PLAYLIST_DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            if attempt == PLAYLIST_DOWNLOAD_RETRIES:
                raise
            backoff = 2 ** attempt
            print(f"Attempt {attempt} failed ({e}); retrying in {backoff}s...")
            time.sleep(backoff)


def main():
    print("Run script...")

    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(exist_ok=True)
    print(f"Output directory: '{OUTPUT_DIR}'")

    ICONS_SQR_DIR.mkdir(exist_ok=True)
    print(f"img directory: '{ICONS_SQR_DIR.name}'")

    ICONS_RECT_DIR.mkdir(exist_ok=True)
    print(f"img2 directory: '{ICONS_RECT_DIR.name}'")

    original_icons = {}
    for url in PLAYLIST_URLS:
        filename = os.path.basename(urlparse(url).path)
        print(f"\n--- Processing: {filename} ---")
        try:
            print(f"Download: {url}")
            playlist_lines = download_playlist(url).splitlines()

            replacements_count = 0
            not_replaced_count = 0
            processed_lines = []
            for line in playlist_lines:
                if 'tvg-logo="' in line:
                    # Only flat numeric file names are accepted: anything else is left untouched,
                    # so remote input can never write outside img/ or img2/.
                    match = re.search(r'tvg-logo="(https?://epg\.one/img/([0-9]+\.png))"', line)
                    if match:
                        old_icon_url = match.group(1)
                        icon_path = match.group(2)
                        download_url = re.sub(r"^https?://epg\.one/", ICONS_SOURCE_BASE, old_icon_url)
                        original_icons[download_url] = ICONS_SQR_DIR / icon_path
                        download_url_rect = download_url.replace("/img/", "/img2/", 1)
                        original_icons[download_url_rect] = ICONS_RECT_DIR / icon_path
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
            tmp_file = file_path.with_name(file_path.name + ".tmp")
            tmp_file.write_text(playlist_content, encoding='utf-8')
            os.replace(tmp_file, file_path)
            print(f"saved: {file_path}")

        except requests.RequestException as e:
            print(f"Error processing: {url}: {e}")

    if len(original_icons):
        run_download_worker(original_icons)

    cleanup_unused_icons(output_path)

    print("\nDone!")


def download_icon_batch(batch_items):
    success = 0
    with requests.Session() as session:
        adapter = requests.adapters.HTTPAdapter(max_retries=1)
        session.mount('https://', adapter)

        for url, save_path in batch_items:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = save_path.with_name(save_path.name + ".tmp")
            try:
                with session.get(url, stream=True, timeout=ICONS_DOWNLOAD_TIMEOUT) as r:
                    r.raise_for_status()
                    with open(tmp_path, 'wb') as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
                os.replace(tmp_path, save_path)
                success += 1
            except Exception as e:
                print(f"Failed {url}: {e}")
                tmp_path.unlink(missing_ok=True)
    return success


def run_download_worker(icons):
    print(f"Icons to download: {len(icons)}")
    items = list(icons.items())
    batch_size = 50
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    total_downloaded = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=ICON_DOWNLOAD_WORKERS) as executor:
        future_to_batch = {
            executor.submit(download_icon_batch, batch): i
            for i, batch in enumerate(batches)
        }

        try:
            for future in as_completed(future_to_batch, timeout=ICONS_DOWNLOAD_TIMEOUT):
                batch_idx = future_to_batch[future]
                try:
                    successful = future.result(timeout=60)
                    total_downloaded += successful

                    elapsed = time.time() - start_time
                    progress = ((batch_idx + 1) / len(batches)) * 100
                    print(f"Processed: {progress:.1f}% | Downloaded: {total_downloaded} | Time: {elapsed:.0f}с")
                except TimeoutError:
                    print(f"Batch timeout {batch_idx + 1}")
                except Exception as e:
                    print(f"Batch error: {e}")
        except TimeoutError:
            print(f"Global icon download timeout after {ICONS_DOWNLOAD_TIMEOUT}s; committing partial results")

    print(f"Download icons done: {total_downloaded} из {len(icons)}")


def cleanup_unused_icons(output_path):
    referenced_ids = set()
    for playlist_file in output_path.glob("*.m3u8"):
        content = playlist_file.read_text(encoding='utf-8')
        for match in re.finditer(re.escape(NEW_ICONS_BASE_URL_SQR) + r'([0-9]+)\.png"', content):
            referenced_ids.add(match.group(1))

    if not referenced_ids:
        print("No icon references found; skipping cleanup of unused icons")
        return

    removed_count = 0
    for icons_dir in (ICONS_SQR_DIR, ICONS_RECT_DIR):
        for icon_file in icons_dir.glob("*.png"):
            if icon_file.stem not in referenced_ids:
                icon_file.unlink()
                removed_count += 1
    print(f"Removed unused icons: {removed_count}")


if __name__ == "__main__":
    main()
