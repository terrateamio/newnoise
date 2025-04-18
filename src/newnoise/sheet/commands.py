from collections import deque

from . import data, handlers

HANDLERS = [
    handlers.EC2InstanceHandler(),
    handlers.EC2HostHandler(),
    handlers.LoadBalancerHandler(),
    handlers.RDSInstanceHandler(),
    handlers.RDSIOPSHandler(),
    handlers.RDSStorageHandler(),
    handlers.S3OperationsHandler(),
    handlers.S3StorageHandler(),
    handlers.SQSHandler(),
    handlers.SQSFIFOHandler(),
    handlers.LambdaHandler(),
    handlers.EBSStorageHandler(),
    handlers.EBSIOPSHandler(),
    handlers.EBSIOPSIO2Tier1Handler(),
    handlers.EBSIOPSIO2Tier2Handler(),
    handlers.EBSIOPSIO2Tier3Handler(),
    handlers.DynamoDBStorageHandler(),
    handlers.DynamoDBStorageIAHandler(),
    handlers.DynamoDBRequestsHandler(),
    handlers.DynamoDBRequestsIAHandler(),
    handlers.DynamoDBReplIAHandler(),
    handlers.DynamoDBReplIAHandler(),
    handlers.DynamoDBStreamsHandler(),
]


def sheet(args):
    input_file = args.input
    output_dir = args.output
    ccy = args.currency

    # to_oiq returns a generator. deque exhausts it.
    deque(
        data.to_oiq(input_file, HANDLERS, output_dir=output_dir, ccy=ccy),
        maxlen=0,
    )


def init_parsers(parsers):
    sheet_parser = parsers.add_parser("sheet", help="Create OIQ price sheet")
    sheet_parser.set_defaults(func=sheet, parser=sheet_parser)

    sheet_parser.add_argument(
        "input",
        type=str,
        help="Path to the input CSV",
    )
    sheet_parser.add_argument(
        "-o", "--output", type=str, help="Path to the output directory", required=False
    )
    sheet_parser.add_argument(
        "-c",
        "--currency",
        type=str,
        help="Filter for prices quoted in particular currency (USD or CNY)",
        required=False,
    )
