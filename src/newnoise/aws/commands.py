from . import env, products


def init_parsers(parsers):
    # main command
    aws_parser = parsers.add_parser("aws", help="Work with AWS data")
    aws_parser.set_defaults(func=lambda args: args.parser.print_help(), parser=aws_parser)
    aws_subparsers = aws_parser.add_subparsers(dest="action")

    # fetch
    fetch_parser = aws_subparsers.add_parser("fetch", help="Fetch price data")
    fetch_parser.set_defaults(func=products.fetch, parser=fetch_parser)
    fetch_parser.add_argument(
        "-d",
        "--datadir",
        default=env.NOISES_ROOT,
        help="Directory path to store price data",
    )

    # load
    load_parser = aws_subparsers.add_parser("load", help="Load data into database")
    load_parser.set_defaults(func=products.load, parser=load_parser)
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
    dump_parser.set_defaults(func=products.dump, parser=dump_parser)
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
