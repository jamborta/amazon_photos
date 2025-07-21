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
import numpy as np
import requests
import time
import json
import logging
import sys
import math
import pandas as pd
from collections import defaultdict

folder_report = defaultdict(lambda: {"server": set(), "local": set()})

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
import types
from pathlib import Path
from functools import partial
import asyncio

def download_with_folders(self, media_df, out: str = 'media', chunk_size: int = None, **kwargs):
    """Modified download that preserves folder structure and splits photos/videos"""
    
    # Build folder mapping with caching
    logger.info("Building folder structure mapping...")
    
    # Try to load folders from cache
    if os.path.exists(folders_cache_file) and not FORCE_REFRESH_CACHE:
        logger.info("Loading folder structure from cache: %s", folders_cache_file)
        with open(folders_cache_file, 'r') as f:
            cached_data = json.load(f)
            self.folders = cached_data['folders']
            self.tree = cached_data['tree']
        logger.info("Loaded %s folders from cache", len(self.folders))
        
        # Check if folder cache is older than 24 hours
        folder_cache_age = time.time() - os.path.getmtime(folders_cache_file)
        if folder_cache_age > 24 * 60 * 60:  # 24 hours in seconds
            logger.warning("Folder cache is %.1f hours old - consider refreshing", folder_cache_age / 3600)
    else:
        if FORCE_REFRESH_CACHE:
            logger.info("Force refresh: fetching folder structure from API...")
        else:
            logger.info("No folder cache found, fetching from Amazon Photos API...")
        
        self._ensure_folders()
        
        # Save folders to cache
        logger.info("Saving folder structure to cache: %s", folders_cache_file)
        cache_data = {
            'folders': self.folders,
            'tree': self.tree,
            'timestamp': time.time()
        }
        with open(folders_cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2, default=str)
        logger.info("Folder cache saved successfully")
    
    folder_nodes = {item['id']: item for item in self.folders}
    folder_nodes[self.root['id']] = {'name': '', 'parents': [None]}
    
    def get_folder_path(folder_id):
        if not folder_id or folder_id == self.root['id']:
            return ""
        path_parts = []
        current_id = folder_id
        while current_id and current_id != self.root['id']:
            folder = folder_nodes.get(current_id)
            if not folder:
                break
            if folder['name']:
                path_parts.append(folder['name'])
            parents = folder.get('parents', [])
            current_id = parents[0] if parents and parents[0] else None
        path_parts.reverse()
        return '/'.join(path_parts)
    
    def get_download_dir(row, is_live_photo_child=False):
        # Live Photo child always goes to photos dir
        if is_live_photo_child:
            return Path(DOWNLOAD_PHOTOS_DIR)
        ext = str(row.get('extension', '')).lower()
        # Photo extensions always go to photos dir
        if ext in ("jpg", "jpeg", "png", "heic", "heif", "gif", "bmp", "tiff", "raw"):
            return Path(DOWNLOAD_PHOTOS_DIR)
        # Only true standalone videos go to videos dir
        content_type = row.get('contentType', '').lower()
        if ext in ("mp4", "mov", "avi", "qt") or content_type.startswith("video"):
            return Path(DOWNLOAD_VIDEOS_DIR)
        return Path(DOWNLOAD_PHOTOS_DIR)

    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    params = {
        'querySuffix': '?download=true',
        'ownerId': self.root['ownerId'],
    }

    async def download_single_file(client, sem, node_id, expected_size, local_filepath, relative_path, parent_name):

        """Helper function to download a single file"""
        # Check if file already exists with correct size
        if local_filepath.exists():
            existing_size = local_filepath.stat().st_size
            if expected_size > 0 and existing_size == expected_size:
                logger.debug(f"SKIPPED (already exists): {relative_path} ({existing_size:,} bytes)")
                return "skipped"
            elif expected_size > 0:
                logger.info(f"RE-DOWNLOADING (size mismatch): {relative_path} - Expected: {expected_size:,}, Found: {existing_size:,}")
            else:
                logger.info(f"RE-DOWNLOADING (no size info): {relative_path}")
        
        logger.debug(f'Downloading {node_id} to {local_filepath}')
        try:
            async with sem:
                url = f'{self.drive_url}/nodes/{node_id}/contentRedirection'
                async with client.stream('GET', url, params=params) as r:
                    r.raise_for_status()
                    import aiofiles
                    async with aiofiles.open(local_filepath, 'wb') as fp:
                        async for chunk in r.aiter_bytes(chunk_size or 8192):
                            await fp.write(chunk)
                
                # Verify file size after download
                actual_size = local_filepath.stat().st_size
                
                if expected_size > 0 and actual_size != expected_size:
                    logger.error(f"SIZE MISMATCH: {relative_path} - Expected: {expected_size:,} bytes, Got: {actual_size:,} bytes")
                    return False
                else:
                    logger.info(f"Downloaded: {relative_path} ({actual_size:,} bytes)")
                    return True
                    
        except Exception as e:
            logger.error(f'Download FAILED for {node_id}: {e}')
            return False

    async def get(client, sem, row):
        node_id = row['id']
        original_name = row.get('name', f"{node_id}.jpg")
        expected_size = row.get('size', 0)
        
        # Get folder path
        folder_path = ""
        if 'parents' in row and len(row['parents']) > 0:
            parent_id = row['parents'][0] if (isinstance(row['parents'], list) or isinstance(row['parents'], np.ndarray)) else row['parents']
            folder_path = get_folder_path(parent_id)
        
        # Track server file for this folder
        folder_report[folder_path]["server"].add(original_name)
        # Determine main file download dir
        main_dir = get_download_dir(row)
        # Create local directory
        if folder_path:
            local_dir = main_dir / folder_path
            local_dir.mkdir(parents=True, exist_ok=True)
        else:
            local_dir = main_dir
        
        # Download main file
        local_filepath = local_dir / original_name
        relative_path = f"{folder_path}/{original_name}" if folder_path else original_name
        
        main_result = await download_single_file(client, sem, node_id, expected_size, local_filepath, relative_path, row["name"])
        
        # Track local file for this folder (if it exists after download)
        if local_filepath.exists():
            folder_report[folder_path]["local"].add(original_name)
        # Check for Live Photo video component
        is_live_photo = False
        child_asset_info = row.get('childAssetTypeInfo', [])
        for asset_info in child_asset_info:
            if isinstance(asset_info, dict) and asset_info.get('assetType') == 'LIVE_VIDEO':
                is_live_photo = True
                break
        
        if is_live_photo:
            # Check if we have child data in the cache
            children_data = children_cache.get(node_id)
            if not children_data:
                try:
                    async with sem:
                        children_url = f'{self.drive_url}/nodes/{node_id}/children'
                        children_params = self.base_params
                        r = await client.get(children_url, params=children_params)
                        r.raise_for_status()
                        children_data = r.json().get('data', [])
                        # Save to cache
                        children_cache[node_id] = children_data
                        logger.info(f"Cached children for node {node_id} ({len(children_data)} children)")
                except Exception as e:
                    logger.error(f'Failed to get Live Photo video component for {node_id}: {e}')
                    children_data = []
            for child in children_data:
                if (child.get('kind') == 'ASSET' and 
                    child.get('assetProperties', {}).get('assetType') == 'LIVE_VIDEO'):
                    child_id = child['id']
                    child_size = child.get('contentProperties', {}).get('size', 0)
                    child_extension = child.get('contentProperties', {}).get('extension', 'qt')
                    if original_name.lower().endswith('.heic'):
                        video_name = f"{original_name}.{child_extension}"
                    else:
                        video_name = f"{original_name}.{child_extension}"
                    # Live Photo child always goes to photos dir
                    video_dir = get_download_dir(child, is_live_photo_child=True)
                    if folder_path:
                        video_local_dir = video_dir / folder_path
                        video_local_dir.mkdir(parents=True, exist_ok=True)
                    else:
                        video_local_dir = video_dir
                    video_filepath = video_local_dir / video_name
                    video_relative_path = f"{folder_path}/{video_name}" if folder_path else video_name
                    video_result = await download_single_file(client, sem, child_id, child_size, video_filepath, video_relative_path, row["name"])
                    # Track server and local for Live Photo child
                    folder_report[folder_path]["server"].add(video_name)
                    if video_filepath.exists():
                        folder_report[folder_path]["local"].add(video_name)
                    if main_result == True and video_result == False:
                        logger.warning(f"Live Photo partially downloaded: {relative_path} (missing video component)")
        
        return main_result

    # Convert DataFrame to list of dicts for easier processing
    media_list = [row for _, row in media_df.iterrows()]
    fns = [partial(get, row=row) for row in media_list]
    results = asyncio.run(self.process(fns, desc='Downloading media with folders', **kwargs))
    
    # Count successful downloads, skipped, and failures  
    downloaded = sum(1 for r in results if r is True)
    skipped = sum(1 for r in results if r == "skipped")
    failed = sum(1 for r in results if r is False)
    none_results = sum(1 for r in results if r is None)  # Handle any None returns
    
    logger.info(f"Download Results: {downloaded} downloaded, {skipped} skipped, {failed} failed, {none_results} incomplete")
    
    return {
        'timestamp': time.time_ns(), 
        'nodes': [row['id'] for row in media_list],
        'downloaded': downloaded,
        'skipped': skipped,
        'failed': failed,
        'incomplete': none_results,
        'total': len(media_list)
    }

# Patch the method
ap.download_with_folders = types.MethodType(download_with_folders, ap)

# Use the patched method
result = ap.download_with_folders(all_media, out=DOWNLOAD_PHOTOS_DIR)

# After all downloads, save the children cache
try:
    with open(children_cache_file, 'w') as f:
        json.dump(children_cache, f, indent=2)
    logger.info(f"Saved children cache: {children_cache_file} ({len(children_cache)} entries)")
except Exception as e:
    logger.warning(f"Failed to save children cache: {e}")

# --- Folder Sync Report ---
print("\n--- Folder Sync Report: Local files not on server ---")
any_missing = False
for folder, files in folder_report.items():
    extra_local = files["local"] - files["server"]
    if extra_local:
        any_missing = True
        print(f"\nFolder: {folder or '.'}")
        for fname in sorted(extra_local):
            print(f"  Local only: {fname}")
if not any_missing:
    print("All local files are present on the server (no extras found).")

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
