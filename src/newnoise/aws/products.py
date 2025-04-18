import asyncio
import csv
import json
import os

import aiohttp

from . import db, env


def fetch(args):
    noises_root = args.datadir

    # prepare work dir
    os.makedirs(noises_root, exist_ok=True)

    # fetch pricing root for aws
    files = [(env.PRICE_ROOT, nr_path(noises_root, "root.json"))]
    asyncio.run(download_files(noises_root, files))

    # load pricing for all aws services
    root_path = nr_path(noises_root, "root.json")
    files = service_pairs(noises_root, root_path)
    asyncio.run(download_files(noises_root, files))

    return files


def load(args):
    noises_root = args.datadir
    db_name = args.name

    root_path = nr_path(noises_root, "root.json")
    pairs = service_pairs(noises_root, root_path)
    dbconn = db.mk_db(db_name)
    for _, resources_file in pairs:
        db.load_service(dbconn, resources_file)
    # dbconn = db.mk_db(db_name)
    # db.load_service(dbconn, "./noises/aws/AWSQueueService/resources.json")


def dump(args):
    csvfile = args.csvfile
    db_name = args.name

    dbconn = db.connect(db_name)
    with open(csvfile, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        headers = [
            "productHash",
            "sku",
            "vendorName",
            "region",
            "service",
            "productFamily",
            "attributes",
            "prices",
        ]
        writer.writerow(headers)
        for row in db.dump_products(dbconn):
            writer.writerow(row)


def nr_path(noises_root, path, parent=None):
    if parent:
        path = os.path.join(parent, path)
    return os.path.join(noises_root, path)


def service_pairs(nr, root_path):
    # load it
    root_data = json.load(open(root_path))

    # fetch pricing root for each aws service
    for service, urls in root_data["offers"].items():
        # dir for service data
        svc_dir = nr_path(nr, service)
        os.makedirs(svc_dir, exist_ok=True)

        # write prices
        # TODO: consider date based filenames for resources
        dst_file = nr_path(nr, "resources.json", parent=service)
        service_prices = urls["currentVersionUrl"]
        yield (service_prices, dst_file)


async def download_file(nr, session, url, dst_file, semaphore):
    async with semaphore:
        async with session.head(url) as response:
            svc_name = dst_file.replace(nr, "").split(os.sep)[1]

            async with session.get(url) as response:
                if response.status == 200:
                    with open(dst_file, "wb") as f:
                        async for chunk in response.content.iter_chunked(1024):
                            f.write(chunk)
                    print(f"Complete: {svc_name}")
                else:
                    raise Exception(f"Failed to download {url}. Status code: {response.status}")


async def download_files(nr, filepairs, max_concurrent=5):
    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url, dst_path in filepairs:
            dst_parents = os.path.dirname(dst_path)
            if dst_parents:
                os.makedirs(dst_parents, exist_ok=True)
            svc_url = f"{env.PRICE_API}{url}"
            tasks.append(download_file(nr, session, svc_url, dst_path, semaphore))
        await asyncio.gather(*tasks)
