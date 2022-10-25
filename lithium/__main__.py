import asyncio
import time

from lithium.lithium import Lithium


async def main():
    start = time.perf_counter()

    async with Lithium() as lithium:
        # await lithium.download_sds_by_cas_number("7732-18-5", "./output")
        await lithium.download_all_sds("./output", print_progress=True, chunks=64)

    print(time.perf_counter() - start)


if __name__ == "__main__":
    asyncio.run(main())
