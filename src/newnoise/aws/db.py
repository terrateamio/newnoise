import datetime
import json
import os
import sqlite3
import sys

import json_stream

from . import transforms as t

DB_CREATE = """
CREATE TABLE products (
    sku TEXT PRIMARY KEY,
    region TEXT,
    service TEXT,
    productFamily TEXT,
    attributes TEXT,
    prices TEXT default (json_array())
)
"""

DB_INSERT_PRODUCT = """INSERT INTO
    products (sku, region, service, productFamily, attributes)
    VALUES (?, ?, ?, ?, ?)
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


def mk_insert_product(db, service):
    def handler(sku_products):
        rows = []
        for sku, product in sku_products:
            productFamily = product.get("productFamily", "")
            region = product["attributes"].get("regionCode", "")
            attributes = json.dumps(product["attributes"])
            rows.append((sku, region, service, productFamily, attributes))
        db.executemany(DB_INSERT_PRODUCT, rows)

    return handler


def mk_insert_price(db, price_type):
    applies_tos = []

    def handler(sku_prices):
        for sku, prices in sku_prices:
            for price in prices.values():
                price_dim = t.get_single(price["priceDimensions"])
                if "appliesTo" in price_dim and len(price_dim["appliesTo"]) > 0:
                    # dont add entry for price containers (eg. list in appliesto)
                    applies_tos.append(price)
                else:
                    for flat_p in t.flatten_prices(price, price_type):
                        db.execute(DB_ADD_PRICE, (json.dumps(flat_p), sku))

    return handler, applies_tos


def load_data(db, data, handler):
    db.execute("BEGIN TRANSACTION;")
    batch = []
    for idx, (sku, thing) in enumerate(data.items()):
        batch.append((sku, json_stream.to_standard_types(thing)))
        if idx % 50000 == 0:
            handler(batch)
            batch = []
    handler(batch)
    db.commit()
    print(f"{datetime.datetime.now().isoformat()} :: {idx + 1}")


def update_applies(db, applies_tos, price_type):
    if not applies_tos:
        return
    for ato in applies_tos:
        ato_dim = t.get_single(ato["priceDimensions"])
        matching_skus = ato_dim["appliesTo"]
        ato_flat = next(t.flatten_prices(ato, price_type))
        db.execute("BEGIN TRANSACTION;")
        for sku, _, prices in find_skus(db, matching_skus):
            t.merge_start_tier(prices, ato_flat)
            prices = [json.dumps(price) for price in prices]
            db.execute(DB_REPLACE_PRICES, (json.dumps(prices), sku))
        db.commit()


def find_skus(db, skus):
    qmarks = ",".join(["?"] * len(skus))
    query = DB_SELECT_SKUS % qmarks
    products = db.execute(query, skus)
    for sku, prod_attrs, prices in products:
        # convert inner encoding
        p_data = json.loads(prices)
        p_data = list(map(lambda p: json.loads(p), p_data))
        yield sku, prod_attrs, p_data


def dump_products(db):
    for row in db.execute(DB_DUMP_ALL):
        yield row
