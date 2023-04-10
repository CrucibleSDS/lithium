import asyncio
from contextlib import suppress
from importlib.metadata import version
import json
from os import PathLike
from pathlib import Path
import random
from string import ascii_letters
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from types import TracebackType
from typing import Optional, Type

import aiofiles
from aiofiles.threadpool.text import AsyncTextIOWrapper
from httpx import AsyncClient
from more_itertools import chunked, collapse

WIKIDATA_CAS_NUMBER_QUERY_URL = (
    "https://query.wikidata.org/sparql?format=json&query=SELECT%20DISTINCT%20%3F"
    "casNumber%20WHERE%20%7B%0A%20%20%3Fitem%20wdt%3AP231%20%3FcasNumber.%0A%7D%"
    "0AORDER%20BY%20%3FcasNumber"
)
SIGMA_ALDRICH_CAS_NUMBER_SEARCH_QUERY = """\
  {key}: getProductSearchResults(input: {{
    searchTerm: "{cas_number}",
    pagination: {{page: 1, perPage: 1000000}},
    sort: relevance,
    group: product,
    facets: [],
    type: CAS_NUMBER
  }}) {{
    items {{
      ... on Product {{
        name
        productKey
        casNumber
        brand {{
          key
        }}
        images {{
          largeUrl
        }}
      }}
    }}
  }}
"""
SIGMA_ALDRICH_SDS_BASE_URL = "https://www.sigmaaldrich.com/US/en/sds"
USER_AGENT = f"Lithium {version('lithium')}"


class Lithium:

    def __init__(
        self,
        *,
        httpx_client: Optional[AsyncClient] = None,
        timeout: int = 300,
        max_retries: int = 5,
        lithium_dir: PathLike = ".lithium",
    ) -> None:
        self.httpx = httpx_client or AsyncClient()
        self.timeout = timeout
        self.max_retries = max_retries

        self.lithium_dir = Path(lithium_dir)
        self.lithium_dir.mkdir(exist_ok=True)
        (self.lithium_dir / "cache").mkdir(exist_ok=True)

        self._keys = set()

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ):
        await self.close()

    async def close(self):
        await self.httpx.aclose()

    async def get_cas_numbers(self):
        res = await self.httpx.get(
            WIKIDATA_CAS_NUMBER_QUERY_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=self.timeout,
        )
        res.raise_for_status()

        data = res.json()
        return [cas_number["casNumber"]["value"] for cas_number in data["results"]["bindings"]]

    def generate_key(self) -> str:
        while (key := "".join(random.sample(ascii_letters, len(ascii_letters)))) in self._keys:
            self._keys.add(key)

        return key

    def build_query(self, *cas_numbers: list[str]) -> str:
        content = "".join(
            SIGMA_ALDRICH_CAS_NUMBER_SEARCH_QUERY.format(
                key=self.generate_key(),
                cas_number=cas_number
            )
            for cas_number in cas_numbers
        )
        return f"query {{\n{content}}}"

    async def search_cas_numbers(self, *cas_numbers: list[str]) -> list[dict[str, str]] | None:
        for _ in range(self.max_retries):
            with suppress(Exception):
                res = await self.httpx.post(
                    "https://www.sigmaaldrich.com/api",
                    headers={
                        "User-Agent": USER_AGENT,
                        "x-gql-country": "US",
                        "x-gql-language": "en",
                    },
                    json={
                        "query": self.build_query(*cas_numbers),
                        "variables": None,
                    },
                    timeout=self.timeout,
                )
                res.raise_for_status()

                if res.status_code == 200:
                    break
        else:
            return None

        data = res.json()["data"]
        if data is None:
            return []

        return [
            {
                "name": item["name"],
                "brand": item["brand"]["key"],
                "product_number": item["productKey"],
                "cas_number": item["casNumber"]
            }
            for cas_number_entry in data.values()
            for item in cas_number_entry["items"]
            if item["casNumber"] is not None
        ]

    async def download_single_sds(self, sds: dict[str, str], output: PathLike) -> int:
        status_code = -1
        for _ in range(self.max_retries):
            with suppress(Exception):
                async with self.httpx.stream(
                    "GET",
                    f"{SIGMA_ALDRICH_SDS_BASE_URL}/{sds['brand']}/{sds['product_number']}",
                    headers={"User-Agent": USER_AGENT},
                    timeout=self.timeout,
                ) as res:
                    status_code = res.status_code
                    if status_code != 200:
                        continue

                    async with aiofiles.open(output, "wb") as f:
                        async for chunk in res.aiter_bytes():
                            await f.write(chunk)

                    return status_code

        return status_code

    async def download_sds_by_cas_number(self, cas_number: str, output_dir: PathLike):
        output = Path(output_dir) / cas_number
        output.mkdir(exist_ok=True, parents=True)

        sds_data = await self.search_cas_number(cas_number)

        return await asyncio.gather(*(
            self.download_single_sds(sds, output / f"{sds['brand']}_{sds['product_number']}.pdf")
            for sds in sds_data
        ))

    async def _download_single_sds_with_dir(
        self,
        sds: dict[str, str],
        output: Path,
        f: AsyncTextIOWrapper,
    ):
        output.parent.mkdir(exist_ok=True, parents=True)
        if (status := await self.download_single_sds(sds, output)) != 200:
            await f.write(json.dumps({"status": status, **sds}) + "\n")
            await f.flush()

    async def download_all_sds(
        self,
        output_dir: PathLike,
        *,
        metadata_cache_file: PathLike = ".lithium/cache/sds.json",
        cas_cache_file: PathLike = ".lithium/cache/cas.json",
        print_progress: bool = False,
        metadata_chunk_size: int = 256,
        download_chunk_size: int = 64,
    ):
        output = Path(output_dir)
        output.mkdir(exist_ok=True)

        if Path(metadata_cache_file).exists():
            print("Loading cached metadata..")
            async with aiofiles.open(metadata_cache_file, "r") as f:
                sds_data = json.loads(await f.read())
        else:
            if print_progress:
                print("Fetching SDS metadata..")

            if Path(cas_cache_file).exists():
                async with aiofiles.open(cas_cache_file, "r") as f:
                    cas_numbers = json.loads(await f.read())
            else:
                cas_numbers = await self.get_cas_numbers()
                async with aiofiles.open(cas_cache_file, "w") as f:
                    await f.write(json.dumps(cas_numbers))

            sds_data_tasks = [
                self.search_cas_numbers(*chunk)
                for chunk in chunked(cas_numbers, metadata_chunk_size)
            ]

            if print_progress:
                sds_data_tasks = tqdm(sds_data_tasks)

            sds_data = [*collapse([await task for task in sds_data_tasks], base_type=dict)]

            async with aiofiles.open(metadata_cache_file, "w") as f:
                await f.write(json.dumps(sds_data))

        if print_progress:
            print("Downloading SDS documents..")

        chunked_data = [
            (
                (sds, output / sds["cas_number"] / f"{sds['brand']}_{sds['product_number']}.pdf")
                for sds in sds_batch
            )
            for sds_batch in chunked(sds_data, download_chunk_size)
        ]

        if print_progress:
            chunked_data = tqdm(chunked_data, desc="Overall Pr")

        async with aiofiles.open(self.lithium_dir / "error.log", "w") as f:
            for i, chunk in enumerate(chunked_data):
                await tqdm_asyncio.gather(*(
                        self._download_single_sds_with_dir(*args, f) for args in chunk
                    ),
                    leave=False,
                    desc=f"Chunk {i + 1:04}"
                )
