import os
import csv
import json
import urllib.parse



PRODUCTHASH = 0
SKU = 1
VENDORNAME = 2
REGION = 3
SERVICE = 4
PRODUCTFAMILY = 5
ATTRIBUTES = 6
PRICES = 7

OUTPUT_DIRNAME = "oiqdata"

AVAILABLE_CCY = ['USD', 'CNY']

PER_HOUR = set([
    "Hrs",
    "Hours",
    "vCPU-hour",
    "vCPU-Months",
    "vCPU-Hours",
    "ACU-Hr",
    "ACU-hour",
    "ACU-Months",
    "Bucket-Mo",
])

PER_OPERATION = set([
    "Op",
    "IOPS-Mo",
    "Requests",
    "API Requests",
    "IOs",
    "Jobs",
    "Updates",
    "CR-Hr",
    "API Calls",
])

PER_DATA = set([
    "GB-Mo",
    "MBPS-Mo",
    "GB",
    "Objects",
    "Gigabyte Month",
    "Tag-Mo",
    "GB-month",
])


def column_filters(
    productHash=None,
    sku=None,
    vendorName=None,
    region=None,
    service=None,
    productFamily=None,
):
    """
    The is a filtering mechanism for the main columns of the input CSV. For
    example, we can select all services from AWS by matching VENDORNAME with
    'aws'.

    Column matches are a list of tuples where tuple.0 is the column index and
    tuple.1 is the string to match with.
    """
    filter = []
    if productHash:
        filter.append((PRODUCTHASH, productHash))
    if sku:
        filter.append((SKU, sku))
    if vendorName:
        filter.append((VENDORNAME, vendorName))
    if region:
        filter.append((REGION, region))
    if service:
        filter.append((SERVICE, service))
    if productFamily:
        filter.append((PRODUCTFAMILY, productFamily))
    return filter


def of_csv(input_file, handlers, ccy=None, **column_query):
    """
    Returns all rows in all cases. If a handler matches a row, the matching
    handler is used to process it and create the relevant product matching,
    price matching, and actual price data.

    If a single product has more than one price, it will yield multiple times
    for each price with a duplicate copy of the product matching info. This
    means multiple matches for product data is allowed and the price data
    should be unique enough to understand what multiple matches means.
    """
    filters = column_filters(**column_query)

    with open(input_file, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)

        # consume headers
        _ = next(reader)

        for row in reader:
            # match on csv column filters
            for f_idx, f_value in filters:
                if row[f_idx] != f_value:
                    break
            # match on attrs or price via handler
            else:
                row[ATTRIBUTES] = json.loads(row[ATTRIBUTES])
                row[PRICES] = json.loads(row[PRICES])
                # can create match set
                for h in handlers:
                    if h.match(row) and h.match_currency(row, ccy=ccy):
                        (product_match_set, price_match_sets, oiq_prices) = h.process(row) 
                        for p_ms, o_p in zip(price_match_sets, oiq_prices):
                            yield (row, h, product_match_set, p_ms, o_p)


def prices_iter(row):
    """
    Simplifies navigation of the nested data structures used for price data
    """
    prices_list_container = row[PRICES].values()
    if not prices_list_container:
        return False
    prices_list = list(prices_list_container)[0]
    for price in prices_list:
        yield price


def process_oiq_price(price):
    """
    Converts structure of price from CSV to OIQ. Starts by determining what
    currency price is in, written as `{"USD": "0.0002"}` and converting it to
    `{"ccy": "USD", "price": "0.0002", "type": "h"}`
    """
    new_price = {}

    # 'ccy' and 'price'
    for known_ccy in AVAILABLE_CCY:
        if known_ccy in price:
            new_price['ccy'] = known_ccy
            new_price['price'] = price[known_ccy]
            break

    # 'type'
    p_unit = price['unit']
    if p_unit in PER_HOUR:
        new_price['type'] = 'h'
    elif p_unit in PER_OPERATION:
        new_price['type'] = 'o'
    elif p_unit in PER_DATA:
        new_price['type'] = 'd'
    else:
        raise Exception('Unknown unit: {}'.format(p_unit))

    return new_price


def process_product_skel(row, product_skel=None):
    """
    Takes a row of data and skeleton for describing the product. The values in
    a skeleton can be either primitives or a callable. Callables are expected
    to be the typical case and they do the job of retrieving particular values
    from any row to map them into the skeleton's format. If the value is not a
    callable, it is copied as-is.

    The product skeleton's structure is roughly this:

        {
            "output_key_a": process_attr(input_key),
            "output_key_b": "some value and not a constant",
            ...
        }
    """
    product_data = row[ATTRIBUTES]
    output = {}
    if product_skel is None:
        output = dict(product_data)
    else:
        for dst_key, value in product_skel.items():
            if callable(value):
                result = value(product_data)
                if result is not None:
                    output[dst_key] = result
            else:
                output[dst_key] = value
    return output


def process_price_skel(row, price_skel=None):
    """
    Takes a row of data and skeleton for describing a price. A row can have
    multiple prices, so this function loops across each price, filling out a
    skeleton's structure on each iteration.

    The skeleton's values should be values unique to the price's purpose. The
    actual price data, eg. amount, currency, and usage type, are extracted into
    OIQ price format.

    The processed skeletons are returned as a list with the corresponding OIQ
    prices in another list ordered the same way.
    """
    price_data = row[PRICES]
    product_data = row[ATTRIBUTES]

    # empty data
    if not list(price_data.keys()):
        return (None, None)

    new_price_info = []
    oiq_price_info = []

    if price_skel is None:
        for p in prices_iter(row):
            new_price_info.append(p)
            oiq_price_info.append(process_oiq_price(p))
        return (new_price_info, oiq_price_info)

    else:
        for p in prices_iter(row):
            output = {}
            for dst_key, value in price_skel.items():
                if callable(value):
                    result = value(p, product_data)
                    if result is not None:
                        output[dst_key] = result
                else:
                    output[dst_key] = value

            new_price_info.append(output)
            oiq_price_info.append(process_oiq_price(p))
        return (new_price_info, oiq_price_info)


def match_set_to_string(match_set):
    """
    Converts both product and price match sets into a URL encoded string of
    keypairs, k=v, joined by an & sign.

    Consider the following dict as input:

        {
            "foo": "fighters",
            "bar": "food",
        }

    This would create a string like:

        foo=fighters&bar=food
    """
    match_str = ""
    if match_set:
        safe_str = urllib.parse.quote
        match_keys = []
        for k, v in match_set.items():
            match_keys.append(f"{safe_str(str(k))}={safe_str(str(v))}")
        match_str = '&'.join(match_keys)
    return match_str


def to_oiq(input_file, handlers, output_dir=None, ccy=None, **kw):
    """
    This function starts the main processing done by newnoise. It processes an
    input file with a list of handlers and writes the output to `output_dir`,
    either provided as cli param or the default
    """
    output_dir = prepare_output_dir(output_dir)

    csv_writers = {}
    for row, handler, product_match_set, price_match_set, oiq_price in of_csv(
        input_file, handlers, ccy=ccy, **kw
    ):
        # TODO: we might be able to drop this whole mechanism now that we know
        # a single file is sufficient for everything
        #
        # keep file handles to each writer_key if multiple files are used
        # writer_key = row[REGION]
        writer_key = 'prices'
        csv_writer = prepare_output_writer(output_dir, writer_key, csv_writers)

        # explicit link between price and TF resource type
        product_match_set['type'] = handler.TF
        product_match_string = match_set_to_string(product_match_set)

        # explicit link to service provider, eg. aws, gcp, ...
        price_match_set['service_provider'] = handler.SERVICE_PROVIDER
        # TODO: we may want to explicitly say no region for resources
        # without one, instead of using the empty string found in input
        # explicit link to region in each region
        price_match_set['region'] = row[REGION]
        price_match_string = match_set_to_string(price_match_set)

        # write the line
        new_row = [
            row[SERVICE],
            row[PRODUCTFAMILY],
            product_match_string,
            price_match_string,
            oiq_price['price'],
            oiq_price['type'],
            oiq_price['ccy'],
        ]
        csv_writer.writerow(new_row)

        # yielding the row after write is helpful for logging during dev and
        # does not currently have a purpose beyond that
        yield new_row


def prepare_output_dir(output_dir):
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), OUTPUT_DIRNAME)
    else:
        output_dir = os.path.abspath(output_dir)

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    return output_dir


def prepare_output_writer(output_dir, identifier, writers):
    if identifier not in writers:
        path = os.path.join(output_dir, "%s.csv" % identifier)
        fh = open(path, "w")
        csv_fh = csv.writer(fh)
        writers[identifier] = csv_fh
    return writers[identifier]

