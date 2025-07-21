import asyncio
import json
import logging.config
import math
import os
import platform
import random
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from functools import partial
from hashlib import md5
from logging import getLogger, Logger
from pathlib import Path
from typing import Generator

import aiofiles
import numpy as np
import orjson
import pandas as pd
import psutil
from httpx import AsyncClient, Client, Response, Limits
from tqdm.asyncio import tqdm, tqdm_asyncio

from ._constants import *
from ._helpers import format_nodes, folder_relmap

try:
    get_ipython()
    import nest_asyncio

    nest_asyncio.apply()
except:
    ...

if platform.system() != "Windows":
    try:
        import uvloop

        uvloop.install()
    except:
        ...

logging.config.dictConfig(LOG_CONFIG)
logger = getLogger(list(Logger.manager.loggerDict)[-1])


class AmazonPhotos:
    def __init__(
        self, cookies: dict, db_path: str | Path = "ap.parquet", tmp: str = "", **kwargs
    ):
        self.n_threads = psutil.cpu_count(logical=True)
        self.tld = self.determine_tld(cookies)
        self.drive_url = f"https://www.amazon.{self.tld}/drive/v1"
        self.cdproxy_url = self.determine_cdproxy(kwargs.pop("cdproxy_override", None))
        self.thumb_url = f"https://thumbnails-photos.amazon.{self.tld}/v1/thumbnail"  # /{node_id}?ownerId={ap.root["ownerId"]}&viewBox={width}'
        self.base_params = {
            "asset": "ALL",
            "tempLink": "false",
            "resourceVersion": "V2",
            "ContentType": "JSON",
        }
        self.limits = kwargs.pop(
            "limits",
            Limits(
                max_connections=10,  # Much more conservative to avoid rate limiting
                max_keepalive_connections=5,
                keepalive_expiry=5.0,
            ),
        )
        self.client = Client(
            http2=False,  # todo: "Max outbound streams is 128, 128 open" errors with http2?
            follow_redirects=True,
            timeout=60,
            headers={
                "user-agent": random.choice(USER_AGENTS),
                "x-amzn-sessionid": cookies["session-id"],
            },
            cookies=cookies,
        )
        self.tmp = Path(tmp)
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(db_path).expanduser()
        self.root = self.get_root()
        # Only initialize these if needed for uploads/folder operations
        self.folders = None
        self.db = None
        self.tree = None

    def determine_tld(self, cookies: dict) -> str:
        """
        Determine top-level domain based on cookies

        @param cookies: cookies dict
        @return: top-level domain
        """
        for k, v in cookies.items():
            if k.endswith("_main"):
                return "com"
            if k.startswith(x := "at-acb"):
                return k.split(x)[-1]

    def determine_cdproxy(self, override: str = None):
        """
        Determine cdproxy url based on tld

        @param override: override cdproxy url
        @return: cdproxy url
        """
        # NA variant? https://content-na.drive.amazonaws.com/cdproxy/v1/nodes
        # EU variant? https://content-eu.drive.amazonaws.com/v2/upload
        if override:
            return override
        if self.tld in NORTH_AMERICA_TLD_MAP:
            return "https://content-na.drive.amazonaws.com/cdproxy/nodes"
        elif self.tld in EUROPEAN_TLD_MAP:
            return f"https://content-eu.drive.amazonaws.com/cdproxy/nodes"

    async def process(self, fns: Generator, max_connections: int = None, **kwargs):
        """
        Efficiently process a generator of async partials

        @param fns: generator of async partials
        @param max_connections: max number of connections to use (controlled by semaphore)
        @param kwargs: optional kwargs to pass to AsyncClient and tqdm
        @return: list of results
        """
        desc = kwargs.pop("desc", None)  # tqdm
        defaults = {
            "http2": kwargs.pop("http2", True),
            "follow_redirects": kwargs.pop("follow_redirects", True),
            "timeout": kwargs.pop("timeout", 30.0),
            "verify": kwargs.pop("verify", False),
        }
        headers, cookies = (
            kwargs.pop("headers", {}) | dict(self.client.headers),
            kwargs.pop("cookies", {}) | dict(self.client.cookies),
        )
        sem = asyncio.Semaphore(max_connections or min(self.limits.max_connections, 5))
        async with AsyncClient(
            limits=self.limits, headers=headers, cookies=cookies, **defaults, **kwargs
        ) as client:
            tasks = (fn(client=client, sem=sem) for fn in fns)
            if desc:
                return await tqdm_asyncio.gather(*tasks, desc=desc)
            return await asyncio.gather(*tasks)

    async def async_backoff(
        self,
        fn,
        sem: asyncio.Semaphore,
        *args,
        m: int = 20,
        b: int = 2,
        max_retries: int = 12,
        **kwargs,
    ) -> any:
        """Async truncated exponential backoff"""
        for i in range(max_retries + 1):
            try:
                async with sem:

                    r = await fn(*args, **kwargs)

                    if r.status_code == 409:  # conflict
                        logger.debug(f"{r.status_code} {r.text}")
                        return r

                    if r.status_code == 401:  # BadAuthenticationData
                        logger.error(f"{r.status_code} {r.text}")
                        logger.error(
                            f"Cookies expired. Log in to Amazon Photos and copy fresh cookies."
                        )
                        # sys.exit(1)

                    r.raise_for_status()

                    if self.tmp.name:
                        async with aiofiles.open(
                            f"{self.tmp}/{time.time_ns()}", "wb"
                        ) as fp:
                            await fp.write(r.content)
                    return r
            except Exception as e:
                if i == max_retries:
                    logger.debug(f"Max retries exceeded\n{e}")
                    return
                t = min(random.random() * (b**i), m)
                logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                await asyncio.sleep(t)

    def backoff(
        self, fn, *args, m: int = 20, b: int = 2, max_retries: int = 12, **kwargs
    ) -> any:
        """Exponential truncated exponential backoff"""
        for i in range(max_retries + 1):
            try:
                r = fn(*args, **kwargs)

                if r.status_code == 409:  # conflict
                    logger.debug(f"{r.status_code} {r.text}")
                    return r

                if r.status_code == 400:  # malformed query
                    if r.json().get("message").startswith("Invalid filter:"):
                        logger.error(
                            f"{r.status_code} {r.text}\t\tSee readme for query language syntax."
                        )
                        return
                    else:
                        logger.error(f"{r.status_code} {r.text}")
                        # sys.exit(1)

                if r.status_code == 401:  # "BadAuthenticationData"
                    logger.error(f"{r.status_code} {r.text}")
                    logger.error(
                        f"Cookies expired. Log in to Amazon Photos and copy fresh cookies."
                    )
                    # sys.exit(1) ## testing

                r.raise_for_status()

                if self.tmp.name:
                    with open(f"{self.tmp}/{time.time_ns()}", "wb") as fp:
                        fp.write(r.content)
                return r
            except Exception as e:
                if i == max_retries:
                    logger.debug(f"Max retries exceeded\n{e}")
                    return
                t = min(random.random() * (b**i), m)
                logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                time.sleep(t)

    def usage(self) -> dict | pd.DataFrame:
        """
        Get Amazon Photos current memory usage stats

        @param as_df: return as pandas DataFrame
        @return: dict or pd.DataFrame
        """
        r = self.backoff(
            self.client.get,
            f"{self.drive_url}/account/usage",
            params=self.base_params,
        )
        data = r.json()
        data.pop("lastCalculated")
        return pd.DataFrame(
            {
                "type": k,
                "billable_bytes": int(v["billable"]["bytes"]),
                "billable_count": int(v["billable"]["count"]),
                "total_bytes": int(v["total"]["bytes"]),
                "total_count": int(v["total"]["count"]),
            }
            for k, v in data.items()
        )

    async def q(
        self,
        client: AsyncClient,
        sem: asyncio.Semaphore,
        filters: str,
        offset: int,
        limit: int = MAX_LIMIT,
    ) -> dict:
        """
        Lower level access to Amazon Photos search.
        The `query` method using this to search all media in Amazon Photos.

        @param client: an async client instance
        @param filters: filters to apply to query
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @return: media data as a dict
        """
        r = await self.async_backoff(
            client.get,
            sem,
            f"{self.drive_url}/search",
            params=self.base_params
            | {
                "limit": limit,
                "offset": offset,
                "filters": filters,
                "lowResThumbnail": "true",
                "searchContext": "customer",
                "sort": "['createdDate DESC']",
            },
        )
        return r.json()

    def query(
        self,
        filters: str = "type:(PHOTOS OR VIDEOS)",
        offset: int = 0,
        limit: int = math.inf,
        **kwargs,
    ) -> list[dict] | pd.DataFrame:
        """
        Search all media in Amazon Photos

        @param filters: query Amazon Photos database. See query language syntax in readme.
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @return: media data as a dict or DataFrame
        """
        initial = self.backoff(
            self.client.get,
            f"{self.drive_url}/search",
            params=self.base_params
            | {
                "limit": MAX_LIMIT,
                "offset": offset,
                "filters": filters,
                "lowResThumbnail": "true",
                "searchContext": "customer",
                "sort": "['createdDate DESC']",
            },
        ).json()
        res = [initial]
        # small number of results, no need to paginate
        if initial["count"] <= MAX_LIMIT:
            return format_nodes(pd.json_normalize(initial.get("data", [])))

        offsets = range(offset, min(initial["count"], limit), MAX_LIMIT)
        fns = (
            partial(self.q, offset=o, filters=filters, limit=MAX_LIMIT) for o in offsets
        )
        res.extend(asyncio.run(self.process(fns, desc="Search nodes", **kwargs)))
        return format_nodes(
            pd.json_normalize(y for x in res for y in x.get("data", []))
        )

    def photos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all photos"""
        return self.query("type:(PHOTOS)", **kwargs)

    def videos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all videos"""
        return self.query("type:(VIDEOS)", **kwargs)

    @staticmethod
    def _md5(p):
        return p, md5(p.read_bytes()).hexdigest()

    def dedup_files(
        self,
        path: str | Path,
        md5s: set[str],
        max_workers=psutil.cpu_count(logical=False) // 2,
    ) -> list[Path]:
        """
        Deduplicate all files in folder by comparing md5 against database md5

        @param path: path to folder to dedup
        @param md5s: set of file hashes to deduplicate against
        @param max_workers: max number of workers to use
        @return: list of unique file paths
        """
        dups = []
        unique = []
        files = [x for x in Path(path).rglob("*") if x.is_file()]

        with ProcessPoolExecutor(max_workers=max_workers) as e:
            fut = {e.submit(self._md5, file): file for file in files}
            with tqdm(total=len(files), desc="Checking for duplicate files") as pbar:
                for future in as_completed(fut):
                    file, file_md5 = future.result()
                    if file_md5 not in md5s:
                        unique.append(file)
                    else:
                        dups.append(file)
                    pbar.update()
        logger.info(f"{len(dups)} Duplicate files found")
        logger.info(f"{len(unique)} Unique files found")
        return unique

    def upload(
        self,
        path: str | Path,
        md5s: set[str] = None,
        refresh: bool = True,
        chunk_size=64 * 1024,
        **kwargs,
    ) -> list[dict]:
        """
        Upload files to Amazon Photos

        @param path: path to folder to upload
        @param md5s: set of file hashes to deduplicate against
        @param chunk_size: optional chunk size
        @param refresh: refresh database after upload
        @param kwargs: optional kwargs to pass to AsyncClient
        @return: upload response
        """
        max_workers = kwargs.pop("max_workers", self.n_threads // 2)
        skip_dedup = kwargs.pop("skip_dedup", False)

        async def stream_bytes(file: Path) -> bytes:
            async with aiofiles.open(file, "rb") as f:
                while chunk := await f.read(chunk_size):
                    yield chunk

        async def post(
            client: AsyncClient,
            sem: asyncio.Semaphore,
            pid: str,
            file: Path,
            max_retries: int = 12,
            m: int = 20,
            b: int = 2,
        ):
            for i in range(max_retries + 1):
                try:
                    async with sem:
                        r = await client.post(
                            self.cdproxy_url,
                            # f'https://content-na.drive.amazonaws.com/cdproxy/nodes',
                            data=stream_bytes(file),
                            params={
                                "name": file.name,
                                "kind": "FILE",
                                # 'parents': [pid], # careful, official docs are wrong again
                                "parentNodeId": pid,
                            },
                        )

                        if r.status_code == 409:  # conflict
                            logger.debug(f"{r.status_code} {r.text}")
                            return r

                        if r.status_code == 400:
                            if r.json().get("message").startswith("Invalid filter:"):
                                logger.error(
                                    f"{r.status_code} {r.text}\t\tSee readme for query language syntax."
                                )
                                return
                            else:
                                logger.error(f"{r.status_code} {r.text}")
                                # sys.exit(1)

                        if r.status_code == 401:  # BadAuthenticationData
                            logger.error(f"{r.status_code} {r.text}")
                            logger.error(
                                f"Cookies expired. Log in to Amazon Photos and copy fresh cookies."
                            )
                            # sys.exit(1)
                        r.raise_for_status()
                        return r
                except Exception as e:
                    if i == max_retries:
                        logger.debug(f"Max retries exceeded\n{e}")
                        return
                    t = min(random.random() * (b**i), m)
                    logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                    await asyncio.sleep(t)

        # t0 = time.time()
        path = Path(path)
        folder_map, folders = self.create_folders(path)

        if skip_dedup:
            files = (x for x in path.rglob("*") if x.is_file())
        elif md5s:
            files = self.dedup_files(path, md5s, max_workers)
        else:
            self._ensure_db()
            if "md5" in self.db.columns:
                files = self.dedup_files(path, set(self.db.md5), max_workers)
            else:
                logger.warning(
                    "`md5` column missing from database, skipping deduplication checks."
                )
                files = (x for x in path.rglob("*") if x.is_file())

        relmap = folder_relmap(path.name, files, folder_map)
        fns = (partial(post, pid=pid, file=file) for pid, file in relmap)

        res = asyncio.run(self.process(fns, desc="Uploading files", **kwargs))

        if refresh:
            self.refresh_db()
            ## todo: make sure tz correct, need to add multiple filters + pagination if upload is > MAX_NODES
            # add some padding in case amazon disagrees with the timestamps
            # ub_offset = 60 * 5
            # lb_offset = 0
            # self.refresh_db(filters=[f'createdDate:[{convert_ts(t0 - lb_offset)} TO {convert_ts(time.time() + ub_offset)}]'])
        return res

    def download(
        self,
        node_ids: list[str] | pd.Series,
        out: str = "media",
        chunk_size: int = None,
        **kwargs,
    ) -> dict:
        """
        Download files from Amazon Photos

        This method has faster downloads and returns the filename in the response headers which makes things easier.
        Alternatively self.cdproxy_url can be used, but additional requests are required to get the filename.

        @param node_ids: list of media node ids to download
        @param out: path to save files
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        params = {
            "querySuffix": "?download=true",
            "ownerId": self.root["ownerId"],
        }

        async def get(client: AsyncClient, sem: asyncio.Semaphore, node: str) -> None:
            logger.debug(f"Downloading {node}")
            try:
                async with sem:
                    url = f"{self.drive_url}/nodes/{node}/contentRedirection"
                    async with client.stream("GET", url, params=params) as r:
                        content_disposition = dict(
                            [
                                y
                                for x in r.headers["content-disposition"].split("; ")
                                if len((y := x.split("="))) > 1
                            ]
                        )
                        fname = content_disposition["filename"].strip('"')
                        async with aiofiles.open(f"{out}/{node}_{fname}", "wb") as fp:
                            async for chunk in r.aiter_bytes(chunk_size):
                                await fp.write(chunk)
            except Exception as e:
                logger.debug(f"Download FAILED for {node}\t{e}")

        fns = (partial(get, node=node) for node in node_ids)
        asyncio.run(self.process(fns, desc="Downloading media", **kwargs))
        return {"timestamp": time.time_ns(), "nodes": node_ids}

    def trashed(
        self,
        filters: str = "",
        offset: int = 0,
        limit: int = MAX_LIMIT,
        sort: str = "['modifiedDate DESC']",
        **kwargs,
    ) -> list[dict]:
        """
        Get trashed nodes. Essentially a view your trash bin in Amazon Photos.

        **Note**: Amazon restricts API access to first 9999 nodes

        @param filters: filters to apply to query
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @param sort: sort order
        @return: trashed nodes as a dict or DataFrame
        """
        initial = self.backoff(
            self.client.get,
            f"{self.drive_url}/trash",
            params=self.base_params
            | {
                "sort": sort,
                "limit": MAX_LIMIT,
                "offset": offset,
                "filters": filters or "kind:(FILE* OR FOLDER*) AND status:(TRASH*)",
            },
        ).json()
        res = [initial]
        # small number of results, no need to paginate
        if initial["count"] <= MAX_LIMIT:
            return format_nodes(pd.json_normalize(initial.get("data", [])))

        # see AWS error: E.g. "Offset + limit cannot be greater than 9999"
        # offset must be 9799 + limit of 200
        if initial["count"] > MAX_NODES:
            offsets = MAX_NODE_OFFSETS
        else:
            offsets = range(offset, min(initial["count"], limit), MAX_LIMIT)
        fns = (
            partial(
                self._get_nodes, offset=o, filters=filters, limit=MAX_LIMIT, sort=sort
            )
            for o in offsets
        )
        res.extend(
            asyncio.run(self.process(fns, desc="Getting trashed nodes", **kwargs))
        )
        return format_nodes(
            pd.json_normalize(y for x in res for y in x.get("data", []))
        )

    def trash(
        self, node_ids: list[str] | pd.Series, filters: str = "", **kwargs
    ) -> list[dict]:
        """
        Move nodes or entire folders to trash bin

        @param node_ids: list of node ids to trash
        @return: the trash response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(
            client: AsyncClient, sem: asyncio.Semaphore, ids: list[str]
        ) -> Response:
            async with sem:
                return await client.patch(
                    f"{self.drive_url}/trash",
                    json={
                        "recurse": "true",
                        "op": "add",
                        "filters": filters,
                        "conflictResolution": "RENAME",
                        "value": ids,
                        "resourceVersion": "V2",
                        "ContentType": "JSON",
                    },
                )

        id_batches = [
            node_ids[i : i + MAX_TRASH_BATCH]
            for i in range(0, len(node_ids), MAX_TRASH_BATCH)
        ]
        fns = (partial(patch, ids=ids) for ids in id_batches)
        return asyncio.run(self.process(fns, desc="Trashing files", **kwargs))

    def restore(self, node_ids: list[str] | pd.Series) -> Response:
        """
        Restore nodes from trash bin

        @param node_ids: list of node ids to restore
        @return: the restore response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.patch(
            f"{self.drive_url}/trash",
            json={
                "recurse": "true",
                "op": "remove",
                "conflictResolution": "RENAME",
                "value": node_ids,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        )

    def delete(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Permanently delete nodes from Amazon Photos

        @param node_ids: list of node ids to delete
        @return: the delete response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def post(
            client: AsyncClient, sem: asyncio.Semaphore, ids: list[str]
        ) -> Response:
            async with sem:
                return await client.post(
                    f"{self.drive_url}/bulk/nodes/purge",
                    json={
                        "recurse": "false",
                        "nodeIds": ids,
                        "resourceVersion": "V2",
                        "ContentType": "JSON",
                    },
                )

        id_batches = [
            node_ids[i : i + MAX_PURGE_BATCH]
            for i in range(0, len(node_ids), MAX_PURGE_BATCH)
        ]
        fns = (partial(post, ids=ids) for ids in id_batches)
        return asyncio.run(
            self.process(fns, desc="Permanently deleting files", **kwargs)
        )

    def aggregations(self, category: str, out: str = "aggregations") -> dict:
        """
        Get Amazon Photos aggregations. E.g. see all identified people, locations, things, etc.

        @param category: category to get aggregations for. See readme for list of categories.
        @param out: path to save results
        @return: aggregations as a dict
        """
        if category == "all":
            r = self.backoff(
                self.client.get,
                f"{self.drive_url}/search",
                params=self.base_params
                | {
                    "limit": 1,  # don't care about node info, just want aggregations
                    "lowResThumbnail": "true",
                    "searchContext": "all",
                    "groupByForTime": "year",
                },
            )
            data = r.json()["aggregations"]
            if out:
                _out = Path(out)
                _out.mkdir(parents=True, exist_ok=True)
                [
                    (_out / f"{k}.json").write_bytes(
                        orjson.dumps(
                            data[k], option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
                        )
                    )
                    for k in data
                ]
            return data

        categories = {
            "allPeople",
            "clusterId",
            "familyMembers",
            "favorite",
            "location",
            "people",
            "things",
            "time",
            "type",
        }
        if category not in categories:
            raise ValueError(f"category must be one of {categories}")

        r = self.backoff(
            self.client.get,
            f"{self.drive_url}/search/aggregation",
            params={
                "aggregationContext": "all",
                "category": category,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        )
        data = r.json()["aggregations"]
        path = f"{category}.json" if category else out
        if out:
            # save to disk
            _out = Path(path)
            _out.mkdir(parents=True, exist_ok=True)
            _out.write_bytes(orjson.dumps(data))
        return data

    def favorite(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Add media to favorites

        @param node_ids: media node ids to add to favorites
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(
            client: AsyncClient, sem: asyncio.Semaphore, node_id: str
        ) -> dict:
            async with sem:
                r = await client.patch(
                    f"{self.drive_url}/nodes/{node_id}",
                    json={
                        "settings": {
                            "favorite": True,
                        },
                        "resourceVersion": "V2",
                        "ContentType": "JSON",
                    },
                )
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(
            self.process(fns, desc="adding media to favorites", **kwargs)
        )

    def unfavorite(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Remove media from favorites

        @param node_ids: media node ids to remove from favorites
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(
            client: AsyncClient, sem: asyncio.Semaphore, node_id: str
        ) -> dict:
            async with sem:
                r = await client.patch(
                    f"{self.drive_url}/nodes/{node_id}",
                    json={
                        "settings": {
                            "favorite": False,
                        },
                        "resourceVersion": "V2",
                        "ContentType": "JSON",
                    },
                )
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(
            self.process(fns, desc="removing media from favorites", **kwargs)
        )

    def create_album(self, album_name: str, node_ids: list[str] | pd.Series):
        """
        Create album

        @param album_name: name of album to create
        @param node_ids: media node ids to add to album
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        r = self.client.post(
            f"{self.drive_url}/nodes",
            json={
                "kind": "VISUAL_COLLECTION",
                "name": album_name,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        )
        created_album = r.json()
        album_id = created_album["id"]
        self.client.patch(
            f"{self.drive_url}/nodes/{album_id}/children",
            json={
                "op": "add",
                "value": node_ids,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        )
        return created_album

    def rename_album(self, album_id: str, name: str) -> dict:
        """
        Rename album

        @param album_id: album node id
        @param name: new name
        @return: operation response
        """
        return self.client.patch(
            f"{self.drive_url}/nodes/{album_id}",
            json={
                "name": name,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def add_to_album(self, album_id: str, node_ids: list[str] | pd.Series) -> dict:
        """
        Add media to album

        @param album_id: album node id
        @param node_ids: media node ids to add to album
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.patch(
            f"{self.drive_url}/nodes/{album_id}/children",
            json={
                "op": "add",
                "value": node_ids,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def remove_from_album(self, album_id: str, node_ids: list[str] | pd.Series):
        """
        Remove media from album

        @param album_id: album node id
        @param node_ids: media node ids to remove from album
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.patch(
            f"{self.drive_url}/nodes/{album_id}/children",
            json={
                "op": "remove",
                "value": node_ids,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def hide(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Hide media

        @param node_ids: node ids to hide
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(
            client: AsyncClient, sem: asyncio.Semaphore, node_id: str
        ) -> dict:
            async with sem:
                r = await client.patch(
                    f"{self.drive_url}/nodes/{node_id}",
                    json={
                        "settings": {
                            "hidden": True,
                        },
                        "resourceVersion": "V2",
                        "ContentType": "JSON",
                    },
                )
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(self.process(fns, desc="hiding media", **kwargs))

    def unhide(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Unhide media

        @param node_ids: node ids to unhide
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(
            client: AsyncClient, sem: asyncio.Semaphore, node_id: str
        ) -> dict:
            async with sem:
                r = await client.patch(
                    f"{self.drive_url}/nodes/{node_id}",
                    json={
                        "settings": {
                            "hidden": False,
                        },
                        "resourceVersion": "V2",
                        "ContentType": "JSON",
                    },
                )
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(self.process(fns, desc="unhiding media", **kwargs))

    def rename_cluster(self, cluster_id: str, name: str) -> dict:
        """
        Rename a cluster

        E.g. rename a person cluster from a default hash to a readable name

        @param cluster_id: target cluster id
        @param name: new name
        @return: operation response
        """
        return self.client.put(
            f"{self.drive_url}/cluster/name",
            json={
                "sourceCluster": cluster_id,
                "newName": name,
                "context": "customer",
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def combine_clusters(self, cluster_ids: list[str] | pd.Series, name: str) -> dict:
        """
        Combine clusters

        E.g. combine multiple person clusters into a single cluster

        @param cluster_ids: target cluster ids
        @param name: name of new combined cluster
        @return: operation response
        """

        if isinstance(cluster_ids, pd.Series):
            cluster_ids = cluster_ids.tolist()

        return self.client.post(
            f"{self.drive_url}/cluster",
            json={
                "clusterIds": cluster_ids,
                "newName": name,
                "context": "customer",
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def update_cluster_thumbnail(self, cluster_id: str, node_id: str) -> dict:
        """
        Update cluster thumbnail

        @param cluster_id: target cluster id
        @param node_id: node id to set as thumbnail
        @return: operation response
        """
        return self.client.put(
            f"{self.drive_url}/cluster/hero/{cluster_id}",
            json={
                "clusterId": cluster_id,
                "nodeId": node_id,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def add_to_family_vault(
        self, family_id: str, node_ids: list[str] | pd.Series
    ) -> dict:
        """
        Add media to family vault

        @param family_id: family vault id
        @param node_ids: node ids to add to family vault
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.post(
            f"{self.drive_url}/familyArchive/",
            json={
                "nodesToAdd": node_ids,
                "nodesToRemove": [],
                "familyId": family_id,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def remove_from_family_vault(
        self, family_id: str, node_ids: list[str] | pd.Series
    ) -> dict:
        """
        Remove media from family vault

        @param family_id: family vault id
        @param node_ids: node ids to remove from family vault
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.post(
            f"{self.drive_url}/familyArchive/",
            json={
                "nodesToAdd": [],
                "nodesToRemove": node_ids,
                "familyId": family_id,
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()

    def get_root(self) -> dict:
        """
        isRoot:true filter is sketchy, can add extra data and request is still valid.
        seems like only check is something like: `if(data.contains("true"))`
        """
        r = self.backoff(
            self.client.get,
            f"{self.drive_url}/nodes",
            params={"filters": "isRoot:true"} | self.base_params,
        )
        root = r.json()["data"][0]
        logger.debug(f"Got root node: {root}")
        return root

    def get_folders(self) -> list[dict]:
        """
        Get all folders in Amazon Photos

        @return: list of folders
        """
        logger.info("Fetching folders with conservative rate limiting...")

        async def helper(aclient, sem, node):
            url = f'{self.drive_url}/nodes/{node["id"]}/children'
            params = {"filters": "kind:FOLDER"} | self.base_params
            # Add a small delay to be gentler on the API
            await asyncio.sleep(0.2)  # Increased delay

            # Try multiple times with longer delays for 503 errors
            for attempt in range(5):  # More aggressive retries
                r = await self.async_backoff(
                    aclient.get, sem, url, params=params, max_retries=15, m=30, b=3
                )
                if r and r.status_code == 200:
                    return r.json()["data"]
                elif r and r.status_code == 503:
                    # 503 Service Unavailable - wait longer and retry
                    wait_time = min(
                        5 * (2**attempt), 60
                    )  # Exponential backoff up to 60s
                    logger.warning(
                        f"503 error for node {node.get('id')}, waiting {wait_time}s before retry {attempt+1}/5"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    break

            logger.error(
                f"Failed to get folders for node {node.get('id')} after all retries: {r.status_code if r else 'No response'}"
            )
            return None  # Return None to indicate failure, not empty list

        async def process_node(aclient, sem, queue):
            result = []
            processed_nodes = set()
            failed_nodes = []

            while True:
                try:
                    node = await asyncio.wait_for(
                        queue.get(), timeout=5.0
                    )  # Increased timeout
                    node_id = node.get("id")
                    if node_id and node_id not in processed_nodes:
                        processed_nodes.add(node_id)
                        subfolders = await helper(aclient, sem, node)
                        if subfolders is None:
                            # Failed to fetch, add to failed list for potential retry
                            failed_nodes.append(node)
                            logger.warning(
                                f"Failed to fetch subfolders for node {node_id}, will retry later"
                            )
                        else:
                            for folder in subfolders:
                                if folder.get("id") not in processed_nodes:
                                    await queue.put(folder)
                            result.extend(subfolders)
                    queue.task_done()
                except asyncio.TimeoutError:
                    # No more items in queue, we're done
                    break
                except Exception as e:
                    logger.debug(f"Error processing node: {e}")
                    queue.task_done()
                    break

            logger.info(
                f"Processed {len(processed_nodes)} nodes, {len(failed_nodes)} failed"
            )

            # Retry failed nodes multiple times with exponential backoff
            retry_count = 0
            max_retries = 3
            while failed_nodes and retry_count < max_retries:
                retry_count += 1
                logger.info(
                    f"Retry {retry_count}/{max_retries}: Retrying {len(failed_nodes)} failed nodes..."
                )
                still_failed = []
                for node in failed_nodes:
                    if node.get("id") not in processed_nodes:
                        await asyncio.sleep(2.0 * retry_count)  # Exponential delay
                        subfolders = await helper(aclient, sem, node)
                        if subfolders is not None:
                            result.extend(subfolders)
                            logger.info(f"Successfully retried node {node.get('id')}")
                        else:
                            still_failed.append(node)
                            logger.error(
                                f"Still failed for node {node.get('id')} after retry {retry_count}"
                            )
                failed_nodes = still_failed

            if failed_nodes:
                logger.error(
                    f"Final failure for {len(failed_nodes)} nodes after all retries"
                )

            return result

        async def main(
            data, batch_size=min(self.n_threads, 3)
        ):  # Limit to max 3 workers
            sem = asyncio.Semaphore(3)  # Even more conservative semaphore
            async with AsyncClient(
                http2=False,
                limits=Limits(
                    max_connections=3, max_keepalive_connections=2
                ),  # Very conservative
                headers=self.client.headers,
                cookies=self.client.cookies,
                verify=False,
                timeout=60.0,  # Increased timeout
            ) as aclient:
                Q = asyncio.Queue()
                for node in data:
                    await Q.put(node)
                tasks = [
                    asyncio.create_task(process_node(aclient, sem, Q))
                    for _ in range(batch_size)
                ]
                try:
                    await asyncio.wait_for(Q.join(), timeout=600.0)  # 10 minute timeout
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                except asyncio.TimeoutError:
                    logger.warning("Folder fetching timed out after 10 minutes")
                    for task in tasks:
                        task.cancel()
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                all_folders = [y for x in results if isinstance(x, list) for y in x]
                logger.info(f"Successfully fetched {len(all_folders)} folders")
                return all_folders

        folders = asyncio.run(main([{"id": self.root["id"]}]))
        logger.info(f"Total folders fetched: {len(folders)}")
        return folders

    def find_path(self, target: str, root: dict = None):
        """
        Find path to node in tree

        @param target: path to node
        @param root: optional root node to search from
        @return: node
        """
        self._ensure_folders()
        if target == "":
            return self.tree
        root = root or self.tree
        if root["name"] == "":
            current_path = root["name"]
        else:
            current_path = "/".join(filter(len, root["path"].keys()))
        if current_path == target:
            return root
        for child in root["children"]:
            result = self.find_path(target, child)
            if result is not None:
                return result
        return None

    def build_tree(self, folders: list[dict] = None) -> dict:
        """
        Build tree from folders

        @param folders: list of folders
        @return: tree
        """
        folders = folders or self.folders
        folders.extend([{"id": self.root["id"], "name": "", "parents": [None]}])
        nodes = {
            item["id"]: {
                "name": item["name"],
                "id": item["id"],
                "children": [],
                "path": {},
            }
            for item in folders
        }
        root = None
        for item in folders:
            node = nodes[item["id"]]
            if parent_id := item.get("parents")[0]:
                parent = nodes[parent_id]
                parent["children"].append(node)
                node["path"] = (
                    parent["path"]
                    | {parent["name"]: parent_id}
                    | {node["name"]: node["id"]}
                )
            else:
                root = node
                root["path"] = {root["name"]: root["id"]}
        return root

    def print_tree(
        self,
        node: dict = None,
        show_id: bool = True,
        color: bool = True,
        indent: int = 4,
        prefix="",
        last=True,
    ):
        """
        Visualize your Amazon Photos folder structure

        @param node: optional root node to start from (default is root)
        @param show_id: optionally show node id in output
        @param color: optionally colorize output
        @param indent: optional indentation level
        @param prefix: optional prefix to add to output
        """
        if node is None:
            self._ensure_folders()
        node = node or self.tree
        conn = ("└── " if last else "├── ") if node["name"] else ""
        name = node["name"] or "~"
        print(
            f"{prefix}{conn}{Red if color else ''}{name}{Reset if color else ''} {node['id'] if show_id else ''}"
        )
        if node["name"]:
            prefix += (indent * " ") if last else "│" + (" " * (indent - 1))
        else:
            prefix = ""
        for i, child in enumerate(node["children"]):
            last_child = i == len(node["children"]) - 1
            self.print_tree(child, show_id, color, indent, prefix, last_child)

    def create_folders(self, path: str | Path) -> tuple[dict, list[dict]]:
        """
        Recursively create folders in Amazon Photos

        @param path: path to root folder to create
        @return: a {folder: parent ID} map, and list of created folders
        """
        folder_map = {}
        aclient = AsyncClient(
            http2=False,
            limits=self.limits,
            headers=self.client.headers,
            cookies=self.client.cookies,
            verify=False,
        )

        def get_id(data: dict) -> str:
            return data.get("id") or data.get("info", {}).get("nodeId")

        async def mkdir(sem: asyncio.Semaphore, parent_id: str, path: Path):
            while True:
                r = await self.async_backoff(
                    aclient.post,
                    sem,
                    f"{self.drive_url}/nodes",
                    json=self.base_params
                    | {
                        "kind": "FOLDER",
                        "name": path.name,
                        "parents": [parent_id],
                    },
                )
                if r.status_code < 300:
                    logger.debug(
                        f"Folder created: {path.name}\t{r.status_code} {r.text}"
                    )
                else:
                    logger.debug(
                        f"Folder creation failed: {path.name}\t{r.status_code} {r.text}"
                    )
                folder_data = r.json()
                if get_id(folder_data):
                    return folder_data

        async def process_folder(
            sem: asyncio.Semaphore, Q: asyncio.Queue, parent_id: str, path_: Path
        ) -> dict:
            folder = await mkdir(sem, parent_id, path_)
            fid = get_id(folder)
            idx = path_.parts.index(path.name)
            rel = Path(*path_.parts[idx:])
            folder_map[str(rel)] = fid  # track parent folder id relative to root
            for p in path_.iterdir():
                if p.is_dir():
                    await Q.put([fid, p])  # map to newly created folder(s) ID
            return folder

        async def folder_worker(sem: asyncio.Semaphore, Q: asyncio.Queue) -> list[dict]:
            res = []
            while True:
                parent_id, path = await Q.get()
                folder = await process_folder(sem, Q, parent_id, path)
                res.append(folder)
                Q.task_done()
                if Q.empty():
                    return res

        async def main(
            root,
            n_workers=self.n_threads,
            max_connections: int = self.limits.max_connections,
        ):
            sem = asyncio.Semaphore(max_connections)
            # check if local folder name exists in Amazon Photos root
            root_folder = self.find_path(root.name)
            if not root_folder:
                logger.debug(
                    f"Root folder not found, creating root folder: {root.name}"
                )
                root_folder = await mkdir(sem, self.root["id"], root)
                logger.debug(f"Created root folder: {root_folder = }")

            parent_id = get_id(root_folder)
            folder_map[root_folder["name"]] = parent_id
            # init queue with root folder + sub-folders
            Q = asyncio.Queue()
            for p in root.iterdir():
                if p.is_dir():
                    await Q.put([parent_id, p])  # Add folder and parent ID to queue
            workers = [
                asyncio.create_task(folder_worker(sem, Q)) for _ in range(n_workers)
            ]
            await Q.join()
            [w.cancel() for w in workers]
            res = await asyncio.gather(*workers, return_exceptions=True)
            return [y for x in filter(lambda x: isinstance(x, list), res) for y in x]

        res = asyncio.run(main(Path(path)))
        return folder_map, res

    def refresh_db_aggressive(self, **kwargs) -> pd.DataFrame:
        self._ensure_db()
        now = datetime.now()
        ap_yesterday = self.query(
            f"type:(PHOTOS OR VIDEOS) AND timeYear:({now.year}) AND timeMonth:({now.month}) AND timeDay:({now.day - 1})"
        )
        ap_today = self.query(
            f"type:(PHOTOS OR VIDEOS) AND timeYear:({now.year}) AND timeMonth:({now.month}) AND timeDay:({now.day})"
        )
        ap_tomorrow = self.query(
            f"type:(PHOTOS OR VIDEOS) AND timeYear:({now.year}) AND timeMonth:({now.month}) AND timeDay:({now.day + 1})"
        )
        cols = (
            set(ap_yesterday.columns)
            | set(ap_today.columns)
            | set(ap_tomorrow.columns)
            | set(self.db.columns)
        )

        # disgusting
        obj_dtype = np.dtypes.ObjectDType()
        df = (
            pd.concat(
                [
                    ap_today.reindex(columns=cols).astype(obj_dtype),
                    ap_tomorrow.reindex(columns=cols).astype(obj_dtype),
                    self.db.reindex(columns=cols).astype(obj_dtype),
                ]
            )
            .drop_duplicates("id")
            .reset_index(drop=True)
        )

        df = format_nodes(df)

        df.to_parquet(self.db_path)
        self.db = df
        return df

    def refresh_db(self, filters: list[str] = None, **kwargs) -> pd.DataFrame:
        """
        Refresh database
        """
        logger.info(f"Refreshing db `{self.db_path}`")
        if filters is None:
            db = self.refresh_db_aggressive(**kwargs)
        else:
            db = self.nodes(filters=filters, **kwargs)

        self._ensure_db()
        cols = set(db.columns) | set(self.db.columns)

        # disgusting
        obj_dtype = np.dtypes.ObjectDType()
        df = (
            pd.concat(
                [
                    db.reindex(columns=cols).astype(obj_dtype),
                    self.db.reindex(columns=cols).astype(obj_dtype),
                ]
            )
            .drop_duplicates("id")
            .reset_index(drop=True)
        )

        df = format_nodes(df)

        df.to_parquet(self.db_path)
        self.db = df
        return df

    def load_db(self, **kwargs):
        """
        Load database

        @param kwargs: optional kwargs to pass to pd.read_parquet
        @return: DataFrame
        """
        df = None
        if self.db_path.name and self.db_path.exists():
            try:
                df = format_nodes(pd.read_parquet(self.db_path, **kwargs))
            except Exception as e:
                logger.warning(f"Failed to load db `{self.db_path}`\t{e}")
        else:
            logger.warning(
                f"Database `{self.db_path}` not found, initializing new database"
            )
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            df = format_nodes(self.query())
            df.to_parquet(self.db_path)
        return df

    def nodes(
        self,
        filters: list[str] = None,
        sort: list[str] = None,
        limit: int = MAX_LIMIT,
        offset: int = 0,
        **kwargs,
    ) -> pd.DataFrame | None:
        """
        Get first 9999 Amazon Drive nodes

        **Note**: Amazon restricts API access to first 9999 nodes. error: "Offset + limit cannot be greater than 9999"
        startToken/endToken are no longer returned, so we can't use them to paginate.
        """

        filters = " AND ".join([f"({x})" for x in filters]) if filters else ""
        sort = str(sort) if sort else ""
        r = self.backoff(
            self.client.get,
            f"{self.drive_url}/nodes",
            params={
                "sort": sort,
                "limit": limit,
                "offset": offset,
                "filters": filters,
            },
        )
        if r is None:
            return r
        initial = r.json()
        # small number of results, no need to paginate
        if initial["count"] <= MAX_LIMIT:
            if not (df := pd.json_normalize(initial["data"])).empty:
                return format_nodes(df)
            logger.info(f"No results found for {filters = }")
            return

        res = [initial]
        # see AWS error: E.g. "Offset + limit cannot be greater than 9999"
        # offset must be 9799 + limit of 200
        if initial["count"] > MAX_NODES:
            offsets = MAX_NODE_OFFSETS
        else:
            offsets = range(offset, min(initial["count"], limit), MAX_LIMIT)
        fns = (
            partial(self._get_nodes, offset=o, filters=filters, sort=sort, limit=limit)
            for o in offsets
        )
        res.extend(asyncio.run(self.process(fns, desc="Node Query", **kwargs)))
        return format_nodes(
            pd.json_normalize(y for x in res for y in x.get("data", []))
            .drop_duplicates("id")
            .reset_index(drop=True)
        )

    def nodes_by_name(
        self, names: list[str] | pd.Series, **kwargs
    ) -> pd.DataFrame | None:
        if isinstance(names, pd.Series):
            names = names.tolist()
        fns = (
            partial(self._get_nodes, offset=0, filters=f"name:{name}", sort="", limit=1)
            for name in names
        )
        res = asyncio.run(self.process(fns, desc="Node Query", **kwargs))
        return format_nodes(
            pd.json_normalize(y for x in res for y in x.get("data", []))
            .drop_duplicates("id")
            .reset_index(drop=True)
        )

    async def _get_nodes(
        self,
        client: AsyncClient,
        sem: asyncio.Semaphore,
        filters: list[str],
        sort: list[str],
        limit: int,
        offset: int,
    ) -> dict:
        """
        Get Amazon Photos nodes

        @param client: an async client instance
        @param filters: filters to apply to query
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @return: nodes as a dict
        """

        r = await self.async_backoff(
            client.get,
            sem,
            f"{self.drive_url}/nodes",
            params=self.base_params
            | {
                "sort": sort,
                "limit": limit,
                "offset": offset,
                "filters": filters,
            },
        )
        return r.json()

    def __at(self):
        r = self.client.get(
            "https://www.amazon.ca/photos/auth/token",
            params={"_": int(time.time_ns() // 1e6)},
        )
        at = r.json()["access_token"]
        # todo: does not work, investigate later
        # self.client.cookies.update({'at-acb': at})
        return at

    def _ensure_folders(self):
        if self.folders is None:
            self.folders = self.get_folders()
        if self.tree is None:
            self.tree = self.build_tree()

    def _ensure_db(self):
        if self.db is None:
            self.db = self.load_db()

    def download_with_folders(
        self,
        media_df,
        folders_cache_file: str,
        folder_report: dict,
        children_cache: dict,
        DOWNLOAD_PHOTOS_DIR: str,
        DOWNLOAD_VIDEOS_DIR: str,
        FORCE_REFRESH_CACHE: bool,
        out: str = "media",
        chunk_size: int = None,
        **kwargs,
    ):
        """Modified download that preserves folder structure and splits photos/videos"""

        # Build folder mapping with caching
        logger.info("Building folder structure mapping...")

        # Try to load folders from cache
        if os.path.exists(folders_cache_file) and not FORCE_REFRESH_CACHE:
            logger.info("Loading folder structure from cache: %s", folders_cache_file)
            with open(folders_cache_file, "r") as f:
                cached_data = json.load(f)
                self.folders = cached_data["folders"]
                self.tree = cached_data["tree"]
            logger.info("Loaded %s folders from cache", len(self.folders))

            # Check if folder cache is older than 24 hours
            folder_cache_age = time.time() - os.path.getmtime(folders_cache_file)
            if folder_cache_age > 24 * 60 * 60:  # 24 hours in seconds
                logger.warning(
                    "Folder cache is %.1f hours old - consider refreshing",
                    folder_cache_age / 3600,
                )
        else:
            if FORCE_REFRESH_CACHE:
                logger.info("Force refresh: fetching folder structure from API...")
            else:
                logger.info("No folder cache found, fetching from Amazon Photos API...")

            self._ensure_folders()

            # Save folders to cache
            logger.info("Saving folder structure to cache: %s", folders_cache_file)
            cache_data = {
                "folders": self.folders,
                "tree": self.tree,
                "timestamp": time.time(),
            }
            with open(folders_cache_file, "w") as f:
                json.dump(cache_data, f, indent=2, default=str)
            logger.info("Folder cache saved successfully")

        folder_nodes = {item["id"]: item for item in self.folders}
        folder_nodes[self.root["id"]] = {"name": "", "parents": [None]}

        def get_folder_path(folder_id):
            if not folder_id or folder_id == self.root["id"]:
                return ""
            path_parts = []
            current_id = folder_id
            while current_id and current_id != self.root["id"]:
                folder = folder_nodes.get(current_id)
                if not folder:
                    break
                if folder["name"]:
                    path_parts.append(folder["name"])
                parents = folder.get("parents", [])
                current_id = parents[0] if parents and parents[0] else None
            path_parts.reverse()
            return "/".join(path_parts)

        def get_file_type(row, is_live_photo_child=False):
            """Determine if a file is a photo or video"""
            if is_live_photo_child:
                return "photos"  # Live Photo children always go to photos dir
            ext = str(row.get("extension", "")).lower()
            # Photo extensions
            if ext in (
                "jpg",
                "jpeg",
                "png",
                "heic",
                "heif",
                "gif",
                "bmp",
                "tiff",
                "raw",
            ):
                return "photos"
            # Video extensions
            content_type = row.get("contentType", "").lower()
            if ext in ("mp4", "mov", "avi", "qt") or content_type.startswith("video"):
                return "videos"
            return "photos"  # Default to photos for unknown types

        def get_download_dir(row, is_live_photo_child=False):
            # Live Photo child always goes to photos dir
            if is_live_photo_child:
                return Path(DOWNLOAD_PHOTOS_DIR)
            ext = str(row.get("extension", "")).lower()
            # Photo extensions always go to photos dir
            if ext in (
                "jpg",
                "jpeg",
                "png",
                "heic",
                "heif",
                "gif",
                "bmp",
                "tiff",
                "raw",
            ):
                return Path(DOWNLOAD_PHOTOS_DIR)
            # Only true standalone videos go to videos dir
            content_type = row.get("contentType", "").lower()
            if ext in ("mp4", "mov", "avi", "qt") or content_type.startswith("video"):
                return Path(DOWNLOAD_VIDEOS_DIR)
            return Path(DOWNLOAD_PHOTOS_DIR)

        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        params = {
            "querySuffix": "?download=true",
            "ownerId": self.root["ownerId"],
        }

        async def download_single_file(
            client, sem, node_id, expected_size, local_filepath, relative_path
        ):
            """Helper function to download a single file"""
            # Check if file already exists with correct size
            if local_filepath.exists():
                existing_size = local_filepath.stat().st_size
                if expected_size > 0 and existing_size == expected_size:
                    logger.debug(
                        f"SKIPPED (already exists): {relative_path} ({existing_size:,} bytes)"
                    )
                    return "skipped"
                elif expected_size > 0:
                    logger.info(
                        f"RE-DOWNLOADING (size mismatch): {relative_path} - Expected: {expected_size:,}, Found: {existing_size:,}"
                    )
                else:
                    logger.info(f"RE-DOWNLOADING (no size info): {relative_path}")

            logger.debug(f"Downloading {node_id} to {local_filepath}")
            try:
                async with sem:
                    url = f"{self.drive_url}/nodes/{node_id}/contentRedirection"
                    async with client.stream("GET", url, params=params) as r:
                        r.raise_for_status()
                        import aiofiles

                        async with aiofiles.open(local_filepath, "wb") as fp:
                            async for chunk in r.aiter_bytes(chunk_size or 8192):
                                await fp.write(chunk)

                    # Verify file size after download
                    actual_size = local_filepath.stat().st_size

                    if expected_size > 0 and actual_size != expected_size:
                        logger.error(
                            f"SIZE MISMATCH: {relative_path} - Expected: {expected_size:,} bytes, Got: {actual_size:,} bytes"
                        )
                        return False
                    else:
                        logger.info(
                            f"Downloaded: {relative_path} ({actual_size:,} bytes)"
                        )
                        return True

            except Exception as e:
                logger.error(f"Download FAILED for {node_id}: {e}")
                return False

        async def get(client, sem, row):
            node_id = row["id"]
            original_name = row.get("name", f"{node_id}.jpg")
            expected_size = row.get("size", 0)

            # Get folder path
            folder_path = ""
            if "parents" in row and len(row["parents"]) > 0:
                parent_id = (
                    row["parents"][0]
                    if (
                        isinstance(row["parents"], list)
                        or isinstance(row["parents"], np.ndarray)
                    )
                    else row["parents"]
                )
                folder_path = get_folder_path(parent_id)

            # Determine file type and track server file for this folder
            file_type = get_file_type(row)
            folder_report[folder_path][file_type]["server"].add(original_name)
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
            relative_path = (
                f"{folder_path}/{original_name}" if folder_path else original_name
            )

            main_result = await download_single_file(
                client, sem, node_id, expected_size, local_filepath, relative_path
            )

            # Track local file for this folder (if it exists after download)
            if local_filepath.exists():
                folder_report[folder_path][file_type]["local"].add(original_name)
            # Check for Live Photo video component
            is_live_photo = False
            child_asset_info = row.get("childAssetTypeInfo", [])
            for asset_info in child_asset_info:
                if (
                    isinstance(asset_info, dict)
                    and asset_info.get("assetType") == "LIVE_VIDEO"
                ):
                    is_live_photo = True
                    break

            if is_live_photo:
                # Check if we have child data in the cache
                children_data = children_cache.get(node_id)
                if not children_data:
                    try:
                        async with sem:
                            children_url = f"{self.drive_url}/nodes/{node_id}/children"
                            children_params = self.base_params
                            r = await client.get(children_url, params=children_params)
                            r.raise_for_status()
                            children_data = r.json().get("data", [])
                            # Save to cache
                            children_cache[node_id] = children_data
                            logger.info(
                                f"Cached children for node {node_id} ({len(children_data)} children)"
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to get Live Photo video component for {node_id}: {e}"
                        )
                        children_data = []
                for child in children_data:
                    if (
                        child.get("kind") == "ASSET"
                        and child.get("assetProperties", {}).get("assetType")
                        == "LIVE_VIDEO"
                    ):
                        child_id = child["id"]
                        child_size = child.get("contentProperties", {}).get("size", 0)
                        child_extension = child.get("contentProperties", {}).get(
                            "extension", "qt"
                        )
                        if original_name.lower().endswith(".heic"):
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
                        video_relative_path = (
                            f"{folder_path}/{video_name}" if folder_path else video_name
                        )
                        video_result = await download_single_file(
                            client,
                            sem,
                            child_id,
                            child_size,
                            video_filepath,
                            video_relative_path,
                        )
                        # Track server and local for Live Photo child (always photos)
                        folder_report[folder_path]["photos"]["server"].add(video_name)
                        if video_filepath.exists():
                            folder_report[folder_path]["photos"]["local"].add(
                                video_name
                            )
                        if main_result == True and video_result == False:
                            logger.warning(
                                f"Live Photo partially downloaded: {relative_path} (missing video component)"
                            )

            return main_result

        # Convert DataFrame to list of dicts for easier processing
        media_list = [row for _, row in media_df.iterrows()]
        fns = [partial(get, row=row) for row in media_list]
        results = asyncio.run(
            self.process(fns, desc="Downloading media with folders", **kwargs)
        )

        # Count successful downloads, skipped, and failures
        downloaded = sum(1 for r in results if r is True)
        skipped = sum(1 for r in results if r == "skipped")
        failed = sum(1 for r in results if r is False)
        none_results = sum(1 for r in results if r is None)  # Handle any None returns

        logger.info(
            f"Download Results: {downloaded} downloaded, {skipped} skipped, {failed} failed, {none_results} incomplete"
        )

        return {
            "timestamp": time.time_ns(),
            "nodes": [row["id"] for row in media_list],
            "downloaded": downloaded,
            "skipped": skipped,
            "failed": failed,
            "incomplete": none_results,
            "total": len(media_list),
        }
