import requests
import pandas as pd
import json
import datetime
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Tuple


def _parse_iso_datetime(s: str) -> datetime.datetime:
    """Parse ISO-like datetimes returned by the API into a naive UTC datetime. example 2025-12-19T01:03:53.01Z"""
    # Try several common formats returned by the API
    fmts = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"]
    for f in fmts:
        try:
            dt = datetime.datetime.strptime(s, f)
            # If there's a timezone info, convert to UTC naive
            if dt.tzinfo is not None:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            continue
    # Last-resort: try to trim fractional seconds and parse
    try:
        return datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        raise ValueError(f"Unrecognized datetime format: {s}")


class MoxfieldUtil:
    """Faster Moxfield scraper.

    Improvements over v1:
    - Uses requests.Session for connection reuse
    - Fetches deck detail pages concurrently (configurable)
    - Builds DataFrames in batches (avoids repeated concat)
    - Uses response.json() and robust datetime handling
    """

    def __init__(self, format: str, start_date: int, max_rows: int, filters: Optional[str] = None,
                 max_workers: int = 8, sleep_timer: float = 0.0, timeout: float = 10.0):
        self.searchurl = "https://api2.moxfield.com/v2/decks/"
        self.deckurl = "https://api2.moxfield.com/v3/decks/all/"
        self.format = format.split('_')[0]
        self.max_rows = max_rows
        self.filters = filters or ""

        # Store start_date as datetime for reliable comparisons
        self.start_date_dt = datetime.datetime.utcfromtimestamp(start_date)

        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Authorization': 'Bearer undefined',
            'Origin': 'https://www.moxfield.com',
            'Referer': 'https://www.moxfield.com/',
            'User-Agent': os.environ.get("moxfield_useragent", "moxfield-util/2"),
            'X-Moxfield-Version': '2024.01.02.1',
        }

        self.page_size = 100
        self.sort_type = 'updated'
        self.board = 'mainboard'

        self.sleep_timer = float(sleep_timer)
        self.max_workers = max(1, int(max_workers))
        self.timeout = float(timeout)

        # Session with retry/backoff could be added here if desired
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _search_page(self, page: int) -> dict:
        url = (f"{self.searchurl}search?pageNumber={page}&pageSize={self.page_size}"
               f"&sortType={self.sort_type}&sortDirection=Descending{self.filters}&board={self.board}")
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def get_decks(self) -> Tuple[pd.DataFrame, int]:
        """Return a (scraped_decks_df, new_start_date_unix).
        This collects deck ids newer than start_date and then scrapes them concurrently.
        """
        # First page to learn total_pages
        first = self._search_page(1)
        total_pages = first.get('totalPages', 1)
        print(f"Searching pages 1..{total_pages} (start_date={self.start_date_dt.isoformat()})")

        candidates: List[dict] = []

        # process first page
        for deck in first.get('data', []):
            try:
                lastupdate_dt = _parse_iso_datetime(deck['lastUpdatedAtUtc'])
            except Exception:
                continue
            if lastupdate_dt > self.start_date_dt:
                candidates.append({'id': deck['publicId'], 'lastupdated': deck['lastUpdatedAtUtc']})
                if 0 < self.max_rows <= len(candidates):
                    break
        
            print(f"Found {len(candidates)} candidate decks after page 1")

        # fetch remaining pages sequentially but quickly (the expensive part is per-deck details)
        page = 2
        pages_without_decks = 0
        while page <= total_pages and (0 >= self.max_rows or len(candidates) < self.max_rows):
            if self.sleep_timer:
                time.sleep(self.sleep_timer)
            data = self._search_page(page)
            print(f"Processing search page {page}/{total_pages}")
            found_here = False
            for deck in data.get('data', []):
                try:
                    lastupdate_dt = _parse_iso_datetime(deck['lastUpdatedAtUtc'])
                except Exception:
                    continue
                if lastupdate_dt > self.start_date_dt:
                    candidates.append({'id': deck['publicId'], 'lastupdated': deck['lastUpdatedAtUtc']})
                    found_here = True
                    if 0 < self.max_rows <= len(candidates):
                        break
            if not found_here:
                pages_without_decks += 1
            if pages_without_decks > 5:
                break
            page += 1

        # Limit candidates to max_rows
        if self.max_rows > 0:
            candidates = candidates[:self.max_rows]
        print(f"Total candidate decks to fetch: {len(candidates)} (max_rows={self.max_rows})")

        # Scrape deck details concurrently
        results = []
        ids = [c['id'] for c in candidates]

        def _fetch(deck_id: str):
            if self.sleep_timer:
                # small per-worker sleep to help rate limiting
                time.sleep(self.sleep_timer)
            url = f"{self.deckurl}{deck_id}"
            try:
                r = self.session.get(url, timeout=self.timeout)
                r.raise_for_status()
                data = r.json()
                # ensure lastUpdatedAtUtc exists
                if 'lastUpdatedAtUtc' not in data:
                    return None
                print(f"Fetched deck {deck_id} (name={data.get('name')})")
                return {'id': deck_id, 'lastupdated': data['lastUpdatedAtUtc'], 'deckdata': json.dumps(data), 'name': data.get('name')}
            except requests.RequestException:
                print(f"Error fetching deck {deck_id}")
                return None

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(_fetch, did): did for did in ids}
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    results.append(res)

        if not results:
            print("No deck details could be fetched; returning empty DataFrame")
            return pd.DataFrame(columns=['id', 'lastupdated', 'deckdata']), int(self.start_date_dt.replace(tzinfo=datetime.timezone.utc).timestamp())

        # Update start_date to the most recent lastUpdatedAtUtc we saw
        max_dt = max(_parse_iso_datetime(r['lastupdated']) for r in results)
        self.start_date_dt = max(self.start_date_dt, max_dt)

        df = pd.DataFrame(results)[['id', 'lastupdated', 'deckdata']]

        print(f"Scraped {len(results)} decks; new start_date={self.start_date_dt.isoformat()}")

        return df, int(self.start_date_dt.replace(tzinfo=datetime.timezone.utc).timestamp())

    def expand_deckdata(self, df: pd.DataFrame) -> pd.DataFrame:
        # Expand deckdata into separate columns for each object 
        # Example: {"id": "5pvbMd", "name": "Garnet PoA HB", "description": "", "format": "historicBrawl", "visibility": "public", "publicUrl":

        # Convert deckdata string to JSON
        df['deckdata'] = df['deckdata'].apply(json.loads)
        # Expand the deckdata column into separate columns
        expanded_df = pd.json_normalize(df['deckdata'])
        # Combine the original DataFrame with the expanded DataFrame
        result_df = pd.concat([df.drop(columns=['deckdata']), expanded_df], axis=1)
        return result_df

__all__ = ["MoxfieldUtil"]
