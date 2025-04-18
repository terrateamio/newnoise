import asyncio
import csv
import os

from .. import download
from . import data, db, env
from . import transforms as t


def fetch(args):
    noises_root = args.datadir

    # prepare work dir
    os.makedirs(noises_root, exist_ok=True)

    # fetch pricing root for aws
    files = [(env.PRICE_ROOT, data.nr_path(noises_root, "root.json"))]
    asyncio.run(download.fetch_filepairs(files))

    # load pricing for all aws services
    root_path = data.nr_path(noises_root, "root.json")
    files = data.service_pairs(noises_root, root_path)
    asyncio.run(download.fetch_filepairs(files))

    return files


def load(args):
    noises_root = args.datadir
    db_name = args.name

    root_path = data.nr_path(noises_root, "root.json")
    pairs = data.service_pairs(noises_root, root_path)
    dbconn = db.mk_db(db_name)
    for _, resources_file in pairs:
        data.load_service(dbconn, resources_file)


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
            # reformat price column from format in db cache to csv
            (ph, sku, vn, r, s, pf, a, p) = row
            csv_prices = t.price_csv_format(p)
            row = (ph, sku, vn, r, s, pf, a, csv_prices)
            writer.writerow(row)


def init_parsers(parsers):
    # main command
    aws_parser = parsers.add_parser("aws", help="Work with AWS data")
    aws_parser.set_defaults(
        func=lambda args: args.parser.print_help(), parser=aws_parser
    )
    aws_subparsers = aws_parser.add_subparsers(dest="action")

    # fetch
    fetch_parser = aws_subparsers.add_parser("fetch", help="Fetch price data")
    fetch_parser.set_defaults(func=fetch, parser=fetch_parser)
    fetch_parser.add_argument(
        "-d",
        "--datadir",
        default=env.NOISES_ROOT,
        help="Directory path to store price data",
    )

    # load
    load_parser = aws_subparsers.add_parser("load", help="Load data into database")
    load_parser.set_defaults(func=load, parser=load_parser)
    load_parser.add_argument(
        "-d",
        "--datadir",
        default=env.NOISES_ROOT,
        help="Directory path to store price data",
    )
    load_parser.add_argument(
        "-n",
        "--name",
        default=env.NOISES_DB,
        help="Name for the SQLite database file",
    )

    # dump
    dump_parser = aws_subparsers.add_parser("dump", help="Dump data to CSV")
    dump_parser.set_defaults(func=dump, parser=dump_parser)
    dump_parser.add_argument(
        "-n",
        "--name",
        default=env.NOISES_DB,
        help="Name for the SQLite database file",
    )
    dump_parser.add_argument(
        "-c",
        "--csvfile",
        default=env.NOISES_CSV,
        help="Name of the CSV file to create",
    )
