import os
import requests
import time
import json
import logging
import sys
import math
import pandas as pd
import structlog

# Get the logger for this script.
logger = structlog.get_logger()
logger.info("Structlog configured. Ready to import application modules.")

from amazon_photos import AmazonPhotos

# --- Configuration ---
AMAZON_PHOTOS_BASE_URL = "https://www.amazon.co.uk"
DOWNLOAD_DIR = "My_Amazon_Photos_Backup"

# --- MONKEY PATCHING determine_tld (CRITICAL FIX) ---
# This is not a logging hack; it's a necessary fix for a bug in the library.
if AMAZON_PHOTOS_BASE_URL == "https://www.amazon.co.uk":
    def patched_determine_tld(self, cookies: dict) -> str:
        return "co.uk"
    AmazonPhotos.determine_tld = patched_determine_tld
    logger.info("Monkey patched AmazonPhotos.determine_tld to return 'co.uk'.")
elif AMAZON_PHOTOS_BASE_URL == "https://www.amazon.com":
    def patched_determine_tld(self, cookies: dict) -> str:
        return "com"
    AmazonPhotos.determine_tld = patched_determine_tld
    logger.info("Monkey patched AmazonPhotos.determine_tld to return 'com'.")
else:
    logger.info("No specific AMAZON_PHOTOS_BASE_URL provided for monkey patching TLD.")


# Load cookies from a JSON file
cookies_data = {}
try:
    with open('cookies.json', 'r') as f:
        cookies_data = json.load(f)
    if not cookies_data:
        logger.warning("Warning: 'cookies.json' is empty. Please populate it.")
except FileNotFoundError:
    logger.error("Error: 'cookies.json' not found. Please create it.")
    sys.exit(1)
except json.JSONDecodeError:
    logger.error("Error: 'cookies.json' is not valid JSON. Please check its format.")
    sys.exit(1)

if not cookies_data.get("session-id"):
    logger.error("Error: 'session-id' not found in 'cookies.json'. This cookie is essential.")
    sys.exit(1)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- Initialize Amazon Photos API Client ---
try:
    ap = AmazonPhotos(cookies=cookies_data)
    logger.info("Attempting to connect to Amazon Photos...")
    usage = ap.usage()
    logger.info(f"Connected to Amazon Photos.", storage_gb=f"{usage['total']['value'] / (1024**3):.2f}")
except Exception as e:
    logger.error("Authentication or connection error", error_message=str(e))
    logger.error("Please ensure ALL required cookies are correct and up-to-date in 'cookies.json'.")
    sys.exit(1)

# --- Download Logic ---
logger.info("Starting download", path=os.path.abspath(DOWNLOAD_DIR))

downloaded_count = 0
error_count = 0
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 1
BATCH_SIZE = 20
all_media_list = []
offset = 0
total_fetched = 0

logger.info("Fetching list of all photos and videos from your Amazon Photos library...")

while True:
    batch_fetched = False
    retries = 0
    current_batch = pd.DataFrame()
    while retries < MAX_RETRIES:
        try:
            logger.info("Fetching batch...", offset=offset, limit=BATCH_SIZE, attempt=f"{retries + 1}/{MAX_RETRIES}")
            current_batch = ap.query(filters='type:(PHOTOS OR VIDEOS)', offset=offset, limit=BATCH_SIZE)

            if not current_batch.empty:
                batch_size_actual = len(current_batch)
                all_media_list.append(current_batch)
                total_fetched += batch_size_actual
                offset += batch_size_actual
                logger.info("Fetched batch.", count=batch_size_actual, total_fetched=total_fetched)
                batch_fetched = True
                time.sleep(0.5)
                break
            else:
                logger.info("No more items found in this batch. Ending fetch.")
                batch_fetched = True
                break

        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in [500, 502, 503, 504]:
                logger.warning("Received HTTP error for batch. Retrying...", status_code=e.response.status_code)
            elif isinstance(e, requests.exceptions.ConnectionError):
                logger.warning("Connection error for batch. Retrying...", error=str(e))
            else:
                logger.error("Unhandled network error fetching batch", error=str(e))
                error_count += 1
                batch_fetched = True
                break

            retries += 1
            if retries < MAX_RETRIES:
                delay = INITIAL_RETRY_DELAY * (2 ** (retries - 1))
                logger.info(f"Waiting {delay:.2f} seconds before next retry...")
                time.sleep(delay)
            else:
                error_count += 1
                logger.error("Failed to fetch batch after max retries", retries=MAX_RETRIES)
                batch_fetched = True
                break
        except Exception as e:
            error_count += 1
            logger.error("An unexpected error occurred while fetching batch", offset=offset, error=str(e))
            batch_fetched = True
            break

    if not batch_fetched or current_batch.empty:
        break

if all_media_list:
    all_media = pd.concat(all_media_list, ignore_index=True)
else:
    all_media = pd.DataFrame()

logger.info("Finished fetching list.", total_items_to_process=len(all_media))

for index, media_item in all_media.iterrows():
    media_id = media_item['id']
    media_name = media_item.get('name', f"untitled_{media_id}")

    safe_media_name = "".join([c for c in media_name if c.isalnum() or c in ('.', '_', '-')]).strip()
    if not safe_media_name:
        safe_media_name = f"untitled_{media_id}"

    base_name, file_extension = os.path.splitext(safe_media_name)
    if not file_extension:
        content_type = media_item.get('contentProperties.contentType', '')
        if 'image/jpeg' in content_type: file_extension = '.jpg'
        elif 'image/png' in content_type: file_extension = '.png'
        elif 'video/mp4' in content_type: file_extension = '.mp4'
        else: file_extension = '.jpg'

    final_filename = f"{base_name}{file_extension}"
    local_filepath = os.path.join(DOWNLOAD_DIR, final_filename)
    counter = 1
    while os.path.exists(local_filepath):
        final_filename = f"{base_name}_{counter}{file_extension}"
        local_filepath = os.path.join(DOWNLOAD_DIR, final_filename)
        counter += 1

    retries = 0
    while retries < MAX_RETRIES:
        try:
            logger.info("Downloading...", source_name=media_name, destination_name=final_filename)
            file_content = ap.download(media_id)

            with open(local_filepath, 'wb') as f:
                f.write(file_content)
            downloaded_count += 1
            logger.info("Successfully downloaded.", filename=final_filename)
            break

        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in [500, 502, 503, 504]:
                logger.warning("Received HTTP error for download. Retrying...", filename=media_name, status_code=e.response.status_code)
            elif isinstance(e, requests.exceptions.ConnectionError):
                logger.warning("Connection error for download. Retrying...", filename=media_name, error=str(e))
            else:
                logger.error("Unhandled network error for download.", filename=media_name, error=str(e))
                error_count += 1
                break

            retries += 1
            if retries < MAX_RETRIES:
                delay = INITIAL_RETRY_DELAY * (2 ** (retries - 1))
                logger.info(f"Waiting {delay:.2f} seconds before next retry...")
                time.sleep(delay)
            else:
                error_count += 1
                logger.error("Failed to download after max retries.", filename=media_name, retries=MAX_RETRIES)
                break
        except Exception as e:
            error_count += 1
            logger.error("An unexpected error occurred during download.", filename=media_name, error=str(e))
            break

logger.info("\n--- Download Summary ---")
logger.info("Totals", items_found=len(all_media), downloaded=downloaded_count, errors=error_count)
logger.info(f"Download process completed. Check '{os.path.abspath(DOWNLOAD_DIR)}' for your photos.")
