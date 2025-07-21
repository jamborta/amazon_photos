import os
import requests
import time
import json
import logging
import sys
import math
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from amazon_photos import AmazonPhotos

# --- Configuration ---
AMAZON_PHOTOS_BASE_URL = "https://www.amazon.co.uk"
DOWNLOAD_DIR = "/Volumes/My Passport/amazon_photos_backup/Amazon Photos Downloads"

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
    logger.info(f"Connected to Amazon Photos. number_of_photos={int(usage[usage.type == 'photo']['total_count'].values[0])}, number_of_videos={int(usage[usage.type == 'video']['total_count'].values[0])}")
except Exception as e:
    logger.error("Authentication or connection error: %s", str(e))
    logger.error("Please ensure ALL required cookies are correct and up-to-date in 'cookies.json'.")
    sys.exit(1)

# --- Download Logic ---
logger.info("Starting download to: %s", os.path.abspath(DOWNLOAD_DIR))

logger.info("Fetching list of all photos and videos from your Amazon Photos library...")

try:
    all_media = ap.photos()
    logger.info("Finished fetching list. total_items_to_process=%s", len(all_media))
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
    """Modified download that preserves folder structure"""
    
    # Build folder mapping
    self._ensure_folders()
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
    
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    params = {
        'querySuffix': '?download=true',
        'ownerId': self.root['ownerId'],
    }

    async def get(client, sem, row):
        node_id = row['id']
        original_name = row.get('name', f"{node_id}.jpg")
        expected_size = row.get('size', 0)  # Get expected file size
        
        # Get folder path
        folder_path = ""
        if 'parents' in row and row['parents'] and len(row['parents']) > 0:
            parent_id = row['parents'][0] if isinstance(row['parents'], list) else row['parents']
            folder_path = get_folder_path(parent_id)
        
        # Create local path
        if folder_path:
            local_dir = out / folder_path
            local_dir.mkdir(parents=True, exist_ok=True)
            local_filepath = local_dir / original_name
        else:
            local_filepath = out / original_name
        
        relative_path = f"{folder_path}/{original_name}" if folder_path else original_name
        
        # Check if file already exists with correct size
        if local_filepath.exists():
            existing_size = local_filepath.stat().st_size
            if expected_size > 0 and existing_size == expected_size:
                logger.debug(f"SKIPPED (already exists): {relative_path} ({existing_size:,} bytes)")
                return "skipped"  # Return different value for skipped files
            elif expected_size > 0:
                logger.info(f"RE-DOWNLOADING (size mismatch): {relative_path} - Expected: {expected_size:,}, Found: {existing_size:,}")
            else:
                logger.info(f"RE-DOWNLOADING (no size info): {relative_path}")
        
        logger.debug(f'Downloading {node_id} to {local_filepath}')
        try:
            async with sem:
                url = f'{self.drive_url}/nodes/{node_id}/contentRedirection'
                # Note: 302 redirects are normal - contentRedirection redirects to actual download URL
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
result = ap.download_with_folders(all_media, out=DOWNLOAD_DIR)

logger.info("\n--- Download Summary ---")
logger.info("Total files processed: %s", result['total'])
logger.info("Actually downloaded: %s", result['downloaded'])
logger.info("Skipped (already exists): %s", result['skipped'])
logger.info("Failed downloads: %s", result['failed'])
if result['incomplete'] > 0:
    logger.warning("Incomplete downloads: %s", result['incomplete'])
logger.info("Files saved to: %s", os.path.abspath(DOWNLOAD_DIR))
logger.info("Download process completed!")
