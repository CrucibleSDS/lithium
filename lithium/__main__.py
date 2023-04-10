import asyncio
import time

from lithium.lithium import Lithium


async def main():
    start = time.perf_counter()

    async with Lithium() as lithium:
        await lithium.download_all_sds("./output", print_progress=True)

    print(time.perf_counter() - start, "seconds elapsed")


if __name__ == "__main__":
    asyncio.run(main())
