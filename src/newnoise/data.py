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
    handler and the rowkey it generates are also returned. This provides all
    possible data for creating different structures of this data one row at a
    time.
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
                        (match_set, price_info) = h.reduce(row, ccy=ccy)
                        (match_set, price_info) = h.transform(match_set, price_info)
                        for pi in price_info:
                            yield (row, h, match_set, pi)


def prices_iter(row, required=None):
    """
    Simplifies navigation of the nested data structures used for price data
    """
    prices_list_container = row[PRICES].values()
    if not prices_list_container:
        return False
    prices_list = list(prices_list_container)[0]
    for price in prices_list:
        yield price


def reduce(row, product_attrs=None, price_attrs=None):
    """
    Takes the product and price information of a row and reduces both to
    contain only the listed keys.

    Both `product_attrs` and `price_attrs` are dicts where the key is the
    attribute name as found in pricing info and the attribute value is the name
    to use in the reduced output, eg. a way to rename fields as part of the
    reduction.
    """
    reduced_product = {}

    # product

    row_attrs = row[ATTRIBUTES]
    # no attributes reduction
    if product_attrs is None:
        reduced_product = dict(row_attrs)
    # reduce
    else:
        for name, value in row_attrs.items():
            if name in product_attrs:
                reduced_product[product_attrs[name]] = value

    # prices

    row_prices = row[PRICES]
    # no prices info available
    if not list(row_prices.keys()):
        return (reduced_product, None)

    # no prices reduction
    if price_attrs is None:
        return (reduced_product, list(prices_iter(row)))
    # reduce
    else:
        reduced_prices = []
        for price in prices_iter(row):
            price_reduction = {}
            for k, v in price.items():
                if k in price_attrs:
                    price_reduction[price_attrs[k]] = v
            reduced_prices.append(price_reduction)
        return (reduced_product, reduced_prices)


def match_set_to_string(match_set):
    match_str = ""
    if match_set:
        safe_str = urllib.parse.quote
        match_keys = []
        for k, v in match_set.items():
            match_keys.append(f"{safe_str(str(k))}={safe_str(str(v))}")
        match_str = '&'.join(match_keys)
    return match_str


def to_oiq(input_file, handlers, output_dir=None, ccy=None, **kw):
    output_dir = prepare_output_dir(output_dir)

    csv_writers = {}
    for row, handler, match_set, price_info in of_csv(input_file, handlers, ccy=ccy, **kw):
        # keep file handles to each writer_key if multiple files are used
        # writer_key = row[REGION]
        writer_key = 'prices'
        csv_writer = prepare_output_writer(output_dir, writer_key, csv_writers)

        if match_set:
            match_set['type'] = handler.TF
        match_str = match_set_to_string(match_set)

        pricing_match_str = match_set_to_string({'region': row[REGION]})

        new_row = [
            row[SERVICE],
            row[PRODUCTFAMILY],
            match_str,
            pricing_match_str,
            price_info['price'],
            price_info['type'],
        ]
        csv_writer.writerow(new_row)
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

