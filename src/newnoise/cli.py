import argparse
from collections import deque

from . import data, handlers


HANDLERS = [
    handlers.InstanceHandler(),
    handlers.EC2HostHandler(),
    handlers.LoadBalancerHandler(),
    handlers.RDSInstanceHandler(),
    handlers.RDSIOPSHandler(),
    handlers.RDSStorageHandler(),
    handlers.S3OperationsHandler(),
    handlers.S3StorageHandler(),
    # handlers.SQSHandler(),
]


def run():
    parser = argparse.ArgumentParser(description="NewNoise CLI")
    parser.add_argument(
        "input", type=str, help="Path to the input CSV",
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Path to the output directory", required=False
    )
    parser.add_argument(
        "-c",
        "--currency",
        type=str,
        help="Filter for prices quoted in particular currency (USD or CNY)",
        required=False
    )
    args = parser.parse_args()

    deque(data.to_oiq(args.input, HANDLERS, output_dir=args.output, ccy=args.currency), maxlen=0)
