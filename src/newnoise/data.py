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
                        for (product_match_set, price_match_set, price) in h.process(row):
                            yield (row, h, product_match_set, price_match_set, price)


def process(row, product, pricing, priced_by):
    prices_list = [x for outer in row[PRICES].values() for x in outer]
    for (idx, price_attrs) in zip(range(len(prices_list)), prices_list):
        product_match_set = {}
        pricing_match_set = {}

        for k, v in product.items():
            r = v(row, price_attrs)
            if r is not None:
                product_match_set[k] = r

        for k, v in pricing.items():
            r = v(row, price_attrs)
            if r is not None:
                pricing_match_set[k] = r

        for ccy in AVAILABLE_CCY:
            if ccy in price_attrs:
                price_data = {}
                type_ = priced_by(row, price_attrs)
                if type_ is not None:
                    price_data['price'] = price_attrs[ccy]
                    price_data['type'] = type_
                    price_data['ccy'] = ccy
                    yield (product_match_set, pricing_match_set, price_data)


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

        product_match_string = match_set_to_string(product_match_set)

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

