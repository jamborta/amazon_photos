"""
Amazon Photos Sync Script with Caching

Usage:
  python photos_sync.py                # Use cached media list if available
  python photos_sync.py --refresh-cache   # Force refresh media list from API
  python photos_sync.py --force          # Same as --refresh-cache

Cache behavior:
- Media list and folder structure are cached for 24 hours
- Warns if cache is older than 24 hours
- Downloads preserve folder structure and original filenames
- Skips files that already exist with correct size
- Supports Live Photos (downloads both .heic and .qt components)

Cache files:
- amazon_photos_media_cache.parquet   # Media list cache
- amazon_photos_folders_cache.json    # Folder structure cache
"""

import os
import time
import json
import logging
import sys
import pandas as pd
from collections import defaultdict

folder_report = defaultdict(lambda: {
    "photos": {"server": set(), "local": set()},
    "videos": {"server": set(), "local": set()}
})

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from amazon_photos import AmazonPhotos

# --- Configuration ---
AMAZON_PHOTOS_BASE_URL = "https://www.amazon.co.uk"
DOWNLOAD_PHOTOS_DIR = "/Volumes/My Passport/amazon_photos_backup/"
DOWNLOAD_VIDEOS_DIR = "/Volumes/My Passport/amazon_videos_backup/"

# Check for command line arguments
FORCE_REFRESH_CACHE = "--refresh-cache" in sys.argv or "--force" in sys.argv

if FORCE_REFRESH_CACHE:
    logger.info("🔄 Force refresh mode enabled - will fetch fresh data from API")

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

os.makedirs(DOWNLOAD_PHOTOS_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_VIDEOS_DIR, exist_ok=True)

# --- Initialize Amazon Photos API Client ---
try:
    ap = AmazonPhotos(cookies=cookies_data)
    logger.info("Attempting to connect to Amazon Photos...")
    usage = ap.usage()
    logger.info(f"Connected to Amazon Photos. number_of_photos={int(usage[usage.type == 'photo']['total_count'].values[0])}, number_of_videos={int(usage[usage.type == 'video']['total_count'].values[0])}")
except Exception as e:
    logger.error("Authentication or connection error: %s", str(e))
    logger.error("Please ensure ALL required cookies are correct and up-to-date in 'cookies.json'.")
    sys.exit(1)

# --- Download Logic ---
logger.info("Starting download to: %s", os.path.abspath(DOWNLOAD_PHOTOS_DIR))

logger.info("Fetching list of all photos and videos from your Amazon Photos library...")

# Cache files
media_cache_file = "amazon_photos_media_cache.parquet"
folders_cache_file = "amazon_photos_folders_cache.json"
children_cache_file = "amazon_photos_children_cache.json"
children_cache = {}

# Load children cache if it exists
if os.path.exists(children_cache_file):
    try:
        with open(children_cache_file, 'r') as f:
            children_cache = json.load(f)
        logger.info(f"Loaded children cache: {children_cache_file} ({len(children_cache)} entries)")
    except Exception as e:
        logger.warning(f"Failed to load children cache: {e}")
        children_cache = {}
else:
    children_cache = {}

try:
    # Try to load from cache first (unless force refresh is requested)
    if os.path.exists(media_cache_file) and not FORCE_REFRESH_CACHE:
        logger.info("Loading media list from cache: %s", media_cache_file)
        all_media = pd.read_parquet(media_cache_file)
        logger.info("Loaded from cache. total_items_to_process=%s", len(all_media))
        
        # Check if cache is older than 24 hours
        cache_age = time.time() - os.path.getmtime(media_cache_file)
        if cache_age > 24 * 60 * 60:  # 24 hours in seconds
            logger.warning("Cache is %.1f hours old - consider refreshing", cache_age / 3600)
            logger.info("To refresh cache, run with: python photos_sync.py --refresh-cache")
    else:
        if FORCE_REFRESH_CACHE:
            logger.info("Force refresh requested, fetching fresh data from Amazon Photos API...")
        else:
            logger.info("No cache found, fetching from Amazon Photos API...")
            
        all_media = ap.query('type:(PHOTOS OR VIDEOS)')  # Get both photos and videos
        logger.info("Finished fetching list. total_items_to_process=%s", len(all_media))
        
        # Save to cache
        logger.info("Saving media list to cache: %s", media_cache_file)
        all_media.to_parquet(media_cache_file)
        logger.info("Cache saved successfully")
        
except Exception as e:
    logger.error("Failed to fetch photos and videos: %s", str(e))
    sys.exit(1)

# Just use the fucking working API method and fix the naming!
logger.info("Using the working API download method...")

# Let's monkey patch the download method to handle folder structure
from pathlib import Path

# Use the patched method
result = ap.download_with_folders(
    media_df=all_media,
    folder_report=folder_report,
    folders_cache_file=folders_cache_file,
    children_cache=children_cache,
    FORCE_REFRESH_CACHE=FORCE_REFRESH_CACHE,
    DOWNLOAD_PHOTOS_DIR=DOWNLOAD_PHOTOS_DIR,
    DOWNLOAD_VIDEOS_DIR=DOWNLOAD_VIDEOS_DIR,
    out=DOWNLOAD_PHOTOS_DIR,
)

# After all downloads, save the children cache
try:
    with open(children_cache_file, 'w') as f:
        json.dump(children_cache, f, indent=2)
    logger.info(f"Saved children cache: {children_cache_file} ({len(children_cache)} entries)")
except Exception as e:
    logger.warning(f"Failed to save children cache: {e}")

# --- Folder Sync Report ---
print("\n--- Folder Sync Report: Extra local files not on server ---")
any_issues = False
from pathlib import Path
import os

# 1. Get all server files and folders
server_files_by_folder = defaultdict(set)
for folder, file_types in folder_report.items():
    server_files_by_folder[folder].update(file_types["photos"]["server"])
    server_files_by_folder[folder].update(file_types["videos"]["server"])
server_folders = set(server_files_by_folder.keys())

# 2. Get all local files, keeping track of their source (photo/video backup)
local_files = defaultdict(lambda: {'photos': set(), 'videos': set()})
photos_root = Path(DOWNLOAD_PHOTOS_DIR)
videos_root = Path(DOWNLOAD_VIDEOS_DIR)

def scan_local_folder(root_path, files_map, source_key):
    if not root_path.exists():
        return
    for file_path in root_path.rglob("*"):
        if file_path.is_file() and file_path.name != '.DS_Store':
            rel_parts = file_path.relative_to(root_path).parts
            folder_path = "/".join(rel_parts[:-1])
            filename = rel_parts[-1]
            files_map[folder_path][source_key].add(filename)

scan_local_folder(photos_root, local_files, 'photos')
scan_local_folder(videos_root, local_files, 'videos')
local_folders = set(local_files.keys())

# --- Part 1: Report extra files in folders that exist on the server ---
matching_folders = local_folders & server_folders
if matching_folders:
    print("\n--- Checking for extra files in folders that exist on server ---")
    has_extras_in_matching = False
    for folder in sorted(list(matching_folders)):
        server_files = server_files_by_folder.get(folder, set())
        
        extra_in_photos_dir = local_files[folder]['photos'] - server_files
        extra_in_videos_dir = local_files[folder]['videos'] - server_files
        
        if extra_in_photos_dir or extra_in_videos_dir:
            has_extras_in_matching = True
            any_issues = True

        for fname in sorted(list(extra_in_photos_dir)):
            print(f"Extra file: {os.path.join(DOWNLOAD_PHOTOS_DIR, folder, fname).replace(' ', '\\ ')}")
        for fname in sorted(list(extra_in_videos_dir)):
            print(f"Extra file: {os.path.join(DOWNLOAD_VIDEOS_DIR, folder, fname).replace(' ', '\\ ')}")
            
    if not has_extras_in_matching:
        print("No extra files found in matching folders.")

# --- Part 2: Report all files in folders that ONLY exist locally ---
extra_folders = local_folders - server_folders
if extra_folders:
    any_issues = True
    print("\n--- Found local folders that do not exist on the server ---")
    for folder in sorted(list(extra_folders)):
        photos = local_files[folder]['photos']
        videos = local_files[folder]['videos']
        
        for fname in sorted(list(photos)):
            print(f"Extra file: {os.path.join(DOWNLOAD_PHOTOS_DIR, folder, fname).replace(' ', '\\ ')}")
        for fname in sorted(list(videos)):
            print(f"Extra file: {os.path.join(DOWNLOAD_VIDEOS_DIR, folder, fname).replace(' ', '\\ ')}")

if not any_issues:
    print("\nAll local files are present on the server (no extras found).")

logger.info("\n--- Download Summary ---")
logger.info("Total files processed: %s", result['total'])
logger.info("Actually downloaded: %s", result['downloaded'])
logger.info("Skipped (already exists): %s", result['skipped'])
logger.info("Failed downloads: %s", result['failed'])
if result['incomplete'] > 0:
    logger.warning("Incomplete downloads: %s", result['incomplete'])
logger.info("Files saved to: %s", os.path.abspath(DOWNLOAD_PHOTOS_DIR))
logger.info("Media list cached to: %s", os.path.abspath(media_cache_file))
logger.info("Folder structure cached to: %s", os.path.abspath(folders_cache_file))
logger.info("Download process completed!")
