import argparse

from . import aws, sheet


def run():
    parser = argparse.ArgumentParser(description="NewNoise CLI")
    subparsers = parser.add_subparsers(dest="command", help="NewNoise subcommand")

    aws.commands.init_parsers(subparsers)
    sheet.commands.init_parsers(subparsers)

    args = parser.parse_args()
    if hasattr(args, "func") and callable(args.func):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    run()
