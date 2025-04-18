import asyncio
import os

import aiohttp

from . import env


async def fetch_file(session, url, dst_file, semaphore):
    """
    Downloads a single file from a URL to a specified destination path.

    This coroutine:
    - Uses a semaphore to limit the number of concurrent downloads.
    - Sends a HEAD request to the URL to pre-check resource.
    - Downloads the file in chunks and writes it to disk.
    - Prints a success message upon completion or raises an exception on failure.

    Parameters:
    - session (aiohttp.ClientSession): The session used for HTTP requests.
    - url (str): The full URL of the file to download.
    - dst_file (str): The full path where the file should be saved.
    - semaphore (asyncio.Semaphore): Semaphore used to control concurrency.
    """
    async with semaphore:
        async with session.head(url) as response:
            async with session.get(url) as response:
                if response.status == 200:
                    # 1KB chunks
                    with open(dst_file, "wb") as f:
                        async for chunk in response.content.iter_chunked(1024):
                            f.write(chunk)
                else:
                    raise Exception(
                        f"Failed to download {url}. Status code: {response.status}"
                    )


async def fetch_filepairs(filepairs, max_concurrent=5):
    """
    Downloads multiple files concurrently with a concurrency limit.

    This coroutine:
    - Accepts a list of (URL, destination path) pairs.
    - Ensures the directory structure for each destination file exists.
    - Constructs the full URL using an environment-specific API prefix.
    - Uses a semaphore to limit the number of concurrent downloads.

    Parameters:
    - filepairs (List[Tuple[str, str]]): List of (relative URL, local path) pairs to download.
    - max_concurrent (int): Maximum number of concurrent downloads (default is 5).
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url, dst_path in filepairs:
            # prepare service specific directories
            dst_parents = os.path.dirname(dst_path)
            if dst_parents:
                os.makedirs(dst_parents, exist_ok=True)

            # coroutine for each URL
            svc_url = f"{env.PRICE_API}{url}"
            tasks.append(fetch_file(session, svc_url, dst_path, semaphore))

        # make fetch happen
        await asyncio.gather(*tasks)
