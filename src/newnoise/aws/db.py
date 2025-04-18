import datetime
import json
import os
import sqlite3
import sys

import json_stream

# productHash,sku,vendorName,region,service,productFamily,attributes,prices
DB_CREATE = """
CREATE TABLE products (
    sku TEXT PRIMARY KEY,
    region TEXT,
    service TEXT,
    productFamily TEXT,
    attributes TEXT,
    prices TEXT
)
"""

DB_INSERT_PRODUCT = """INSERT INTO
    products (sku, region, service, productFamily, attributes, prices)
    VALUES (?, ?, ?, ?, ?, json_array())
"""

DB_SELECT_SKUS = """SELECT sku, attributes, prices
    FROM products
    WHERE sku in (%s)
"""

DB_DUMP_ALL = """SELECT
    '', sku, 'aws', region, service,  productFamily, attributes, prices
    FROM products
"""

DB_ADD_PRICE = """UPDATE products
    SET prices = json_insert(prices, '$[#]', ?)
    WHERE sku = ?
"""

DB_SELECT_PRICES = """SELECT sku, prices
    FROM products
    WHERE sku = ?
"""

DB_REPLACE_PRICES = """UPDATE products
    SET prices = ?
    WHERE sku = ?
"""

DB_DUMP = """SELECT
    sku, region, service, productFamily, attributes, prices
    FROM products
"""


def connect(filename):
    if os.path.exists(filename):
        db = sqlite3.connect(filename, isolation_level=None)
        return db


def mk_db(filename):
    if connect(filename):
        sys.stderr.write("ERROR: db file already exists %s" % (filename))
        sys.exit(-1)
    db = sqlite3.connect(filename, isolation_level=None)
    db.execute(DB_CREATE)
    db.commit()
    return db


def get_single(d):
    """
    short hand for function that gets around excessive structure in AWS
    price data
    """
    return list(d.values())[0]


def flatten_price(price, price_type="on_demand"):
    for pid, price_dim in price["priceDimensions"].items():
        # price_dim = get_single(price["priceDimensions"])
        price_id = price_dim["rateCode"]

        new_p = {
            "effectiveDateStart": price["effectiveDate"],
            "purchaseOption": price_type,
            "unit": price_dim["unit"],
            "description": price_dim["description"],
        }

        if "beginRange" in price_dim:
            new_p["startUsageAmount"] = price_dim["beginRange"]
        if "endRange" in price_dim:
            new_p["endUsageAmount"] = price_dim["endRange"]

        ppu = price_dim["pricePerUnit"]
        if "USD" in ppu:
            new_p["USD"] = ppu["USD"]
        elif "CNY" in ppu:
            new_p["CNY"] = ppu["CNY"]

        if price_type == "reserved":
            term_attrs = price["termAttributes"]
            new_p["termLength"] = term_attrs.get("LeaseContractLength", "")
            new_p["termPurchaseOption"] = term_attrs.get("PurchaseOption", "")
            new_p["termOfferingClass"] = term_attrs.get("OfferingClass", "")

        yield {price_id: new_p}


def find_start_tier(price_dims):
    for k, v in price_dims.items():
        if "startUsageAmount" in v and v["startUsageAmount"] == "0":
            return k
    else:
        print("NO START TIER:", price_dims)
        return None


def merge_start_tier(sku_prices, new_start_tier):
    new_id, new_dim = list(new_start_tier.items())[0]

    price_dims = sku_prices[0]
    lowest_id = find_start_tier(price_dims)
    if lowest_id:
        # replace lowest tier's begin range with free tier's end range
        lowest_dim = price_dims.pop(lowest_id)
        lowest_dim["startUsageAmount"] = new_dim["endUsageAmount"]
        price_dims[lowest_id] = lowest_dim
        # save copy of free tier
        price_dims[new_id] = new_dim


def fetch_skus(db, skus):
    qmarks = ",".join(["?"] * len(skus))
    query = DB_SELECT_SKUS % qmarks
    products = db.execute(query, skus)
    for sku, prod_attrs, prices in products:
        # convert inner encoding
        p_data = json.loads(prices)
        p_data = list(map(lambda p: json.loads(p), p_data))
        yield sku, prod_attrs, p_data


def mk_insert_product(db, service):
    def handler(sku, product):
        productFamily = product.get("productFamily", "")
        region = product["attributes"].get("regionCode", "")
        attributes = json.dumps(product["attributes"])
        db.execute(DB_INSERT_PRODUCT, (sku, region, service, productFamily, attributes))

    return handler


def mk_insert_price(db, price_type):
    applies_tos = []

    def handler(sku, prices):
        for p in prices.values():
            price_dim = get_single(p["priceDimensions"])
            if "appliesTo" in price_dim and len(price_dim["appliesTo"]) > 0:
                # dont add entry for price containers (eg. list in appliesto)
                applies_tos.append(p)
            else:
                # TODO: flatten
                for flat_p in flatten_price(p, price_type):
                    db.execute(DB_ADD_PRICE, (json.dumps(flat_p), sku))

    return handler, applies_tos


def load_data(db, data, handler):
    db.execute("BEGIN TRANSACTION;")
    for idx, (sku, thing) in enumerate(data.items()):
        thing = json_stream.to_standard_types(thing)
        handler(sku, thing)
        if idx % 10000 == 0:
            db.commit()
            db.execute("BEGIN TRANSACTION;")
    print(f"{datetime.datetime.now().isoformat()} :: {idx + 1}")
    db.commit()


def load_products(db, filename):
    data = json_stream.load(open(filename))
    service = os.path.basename(os.path.dirname(filename))
    handler = mk_insert_product(db, service)
    products = data["products"]
    load_data(db, products, handler)


def load_prices_on_demand(db, filename):
    data = json_stream.load(open(filename))
    handler, applies_tos = mk_insert_price(db, "on_demand")
    # working around json_stream
    try:
        prices = data["terms"]["OnDemand"]
    except Exception:
        return
    load_data(db, prices, handler)
    load_applies(db, applies_tos, "on_demand")


def load_prices_reserved(db, filename):
    data = json_stream.load(open(filename))
    handler, _ = mk_insert_price(db, "reserved")
    # working around json_stream
    try:
        prices = data["terms"]["Reserved"]
    except Exception:
        return
    load_data(db, prices, handler)


def load_applies(db, applies_tos, price_type):
    if not applies_tos:
        return
    for ato in applies_tos:
        ato_dim = get_single(ato["priceDimensions"])
        matching_skus = ato_dim["appliesTo"]
        # TODO: flatten
        ato_flat = next(flatten_price(ato, price_type))
        db.execute("BEGIN TRANSACTION;")
        for sku, _, prices in fetch_skus(db, matching_skus):
            merge_start_tier(prices, ato_flat)
            prices = [json.dumps(p) for p in prices]
            db.execute(DB_REPLACE_PRICES, (json.dumps(prices), sku))
        db.commit()


def load_service(db, filename):
    """
    Works through resource file by entering everything into sqlite
    """
    service = os.path.basename(os.path.dirname(filename))
    print("#####", service)
    load_products(db, filename)
    load_prices_on_demand(db, filename)
    load_prices_reserved(db, filename)


def dump_products(db):
    for row in db.execute(DB_DUMP_ALL):
        (ph, sku, vn, r, s, pf, a, p) = row

        # restructure prices to match OG csv
        loaded_prices = json.loads(p)
        new_prices = []
        for lp in loaded_prices:
            lp = json.loads(lp)
            lp = get_single(lp)
            new_prices.append(lp)
        new_prices = json.dumps({"aoc_for_prez_2028": new_prices})

        yield (ph, sku, vn, r, s, pf, a, new_prices)
