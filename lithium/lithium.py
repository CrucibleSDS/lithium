import asyncio
from os import PathLike
from pathlib import Path
from typing import Optional

import aiofiles
from bs4 import BeautifulSoup
from httpx import AsyncClient
from more_itertools import chunked, collapse

SIGMA_ALDRICH_CAS_NUMBER_SEARCH_QUERY = "query ProductSearch($searchTerm: String, $page: Int!, $sort: Sort, $group: ProductSearchGroup, $selectedFacets: [FacetInput!], $type: ProductSearchType, $catalogType: CatalogType, $orgId: String, $region: String, $facetSet: [String]) {\n  getProductSearchResults(input: {searchTerm: $searchTerm, pagination: {page: $page}, sort: $sort, group: $group, facets: $selectedFacets, type: $type, catalogType: $catalogType, orgId: $orgId, region: $region, facetSet: $facetSet}) {\n    ...ProductSearchFields\n    __typename\n  }\n}\n\nfragment ProductSearchFields on ProductSearchResults {\n  metadata {\n    itemCount\n    setsCount\n    page\n    perPage\n    numPages\n    redirect\n    __typename\n  }\n  items {\n    ... on Substance {\n      ...SubstanceFields\n      __typename\n    }\n    ... on Product {\n      ...SubstanceProductFields\n      __typename\n    }\n    __typename\n  }\n  facets {\n    key\n    numToDisplay\n    isHidden\n    isCollapsed\n    multiSelect\n    prefix\n    options {\n      value\n      count\n      __typename\n    }\n    __typename\n  }\n  didYouMeanTerms {\n    term\n    count\n    __typename\n  }\n  __typename\n}\n\nfragment SubstanceFields on Substance {\n  _id\n  id\n  name\n  synonyms\n  empiricalFormula\n  linearFormula\n  molecularWeight\n  aliases {\n    key\n    label\n    value\n    __typename\n  }\n  images {\n    sequence\n    altText\n    smallUrl\n    mediumUrl\n    largeUrl\n    brandKey\n    productKey\n    label\n    __typename\n  }\n  casNumber\n  products {\n    ...SubstanceProductFields\n    __typename\n  }\n  match_fields\n  __typename\n}\n\nfragment SubstanceProductFields on Product {\n  name\n  productNumber\n  productKey\n  isSial\n  isMarketplace\n  marketplaceSellerId\n  marketplaceOfferId\n  cardCategory\n  cardAttribute {\n    citationCount\n    application\n    __typename\n  }\n  substance {\n    id\n    __typename\n  }\n  casNumber\n  attributes {\n    key\n    label\n    values\n    __typename\n  }\n  speciesReactivity\n  brand {\n    key\n    erpKey\n    name\n    color\n    __typename\n  }\n  images {\n    altText\n    smallUrl\n    mediumUrl\n    largeUrl\n    __typename\n  }\n  description\n  sdsLanguages\n  sdsPnoKey\n  similarity\n  paMessage\n  features\n  catalogId\n  materialIds\n  __typename\n}\n"  # noqa: E501


class Lithium:

    def __init__(self, *, httpx_client: Optional[AsyncClient] = None, timeout: int = 300) -> None:
        self.httpx = httpx_client or AsyncClient()
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self.httpx.aclose()

    async def get_cas_numbers(self):
        res = await self.httpx.get(
            "https://en.wikipedia.org/wiki/List_of_CAS_numbers_by_chemical_compound"
        )
        soup = BeautifulSoup(res.text, "html.parser")

        return [
            cas_number
            for tbody in soup.find_all("tbody")
            for tr in tbody.find_all("tr")[1:]
            if (cas_number := tr.find_all("td")[2].string.strip())
        ]

    async def search_cas_number(self, cas_number: str):
        res = await self.httpx.post(
            "https://www.sigmaaldrich.com/api",
            headers={"User-Agent": "Lithium 0.1.0", "x-gql-country": "US", "x-gql-language": "en"},
            json={
                "operationName": "ProductSearch",
                "query": SIGMA_ALDRICH_CAS_NUMBER_SEARCH_QUERY,
                "variables": {
                    "group": "product",
                    "page": 1,
                    "searchTerm": cas_number,
                    "selectedFacets": [],
                    "sort": "relevance",
                    "type": "CAS_NUMBER",
                },
            },
            timeout=self.timeout,
        )

        return [
            {
                "brand": item["brand"]["key"],
                "product_number": item["sdsPnoKey"],
                "cas_number": item["casNumber"]
            }
            for item in res.json()["data"]["getProductSearchResults"]["items"]
            if item["casNumber"] is not None
        ]

    async def download_single_sds(self, sds: dict[str, str], output: PathLike):
        async with self.httpx.stream(
            "GET",
            f"https://www.sigmaaldrich.com/US/en/sds/{sds['brand']}/{sds['product_number']}",
            headers={"User-Agent": "Lithium 0.1.0"},
            timeout=self.timeout,
        ) as res:
            async with aiofiles.open(output, "wb") as f:
                async for chunk in res.aiter_bytes():
                    await f.write(chunk)

    async def download_sds_by_cas_number(self, cas_number: str, output_dir: PathLike):
        output = Path(output_dir) / cas_number
        output.mkdir(exist_ok=True, parents=True)

        sds_data = await self.search_cas_number(cas_number)

        return await asyncio.gather(*(
            self.download_single_sds(sds, output / f"{sds['brand']}_{sds['product_number']}.pdf")
            for sds in sds_data
        ))

    def _download_single_sds_with_dir(self, sds: dict[str, str], output: Path):
        output.parent.mkdir(exist_ok=True, parents=True)
        return self.download_single_sds(sds, output)

    async def download_all_sds(
        self,
        output_dir: PathLike,
        *,
        print_progress: bool = False,
        chunks: int = 64
    ):
        output = Path(output_dir)
        output.mkdir(exist_ok=True)

        if print_progress:
            print("Gathering SDS sources..")

        sds_data = [
            await asyncio.gather(*(
                self.search_cas_number(cas_number)
                for cas_number in cas_numbers
            ))
            for cas_numbers in chunked(await self.get_cas_numbers(), chunks)
        ]

        downloaded = 0
        for sds_batch in chunked(collapse(sds_data, base_type=dict), chunks):
            await asyncio.gather(*(
                self._download_single_sds_with_dir(
                    sds,
                    output / sds["cas_number"] / f"{sds['brand']}_{sds['product_number']}.pdf"
                )
                for sds in sds_batch
            ))

            if print_progress:
                downloaded += len(sds_batch)
                print(f"{downloaded} documents downloaded..")
