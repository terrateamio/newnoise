import json
import os

import json_stream

from . import db, env


def nr_path(noises_root, path, parent=None):
    if parent:
        path = os.path.join(parent, path)
    return os.path.join(noises_root, path)


def service_pairs(nr, root_path):
    # load it
    root_data = json.load(open(root_path))

    # fetch pricing root for each aws service
    for service, urls in root_data["offers"].items():
        # dir for service data
        svc_dir = nr_path(nr, service)
        os.makedirs(svc_dir, exist_ok=True)

        # write prices
        # TODO: consider date based filenames for resources
        dst_file = nr_path(nr, "resources.json", parent=service)
        prices_path = urls["currentVersionUrl"]
        prices_url = f"{env.PRICE_API}{prices_path}"
        yield (prices_url, dst_file)


def load_products(dbconn, service, products):
    handler = db.mk_insert_product(dbconn, service)
    db.load_data(dbconn, products, handler)


def load_prices_on_demand(dbconn, prices):
    handler, applies_tos = db.mk_insert_price(dbconn, "on_demand")
    db.load_data(dbconn, prices, handler)
    db.update_applies(dbconn, applies_tos, "on_demand")


def load_prices_reserved(dbconn, prices):
    handler, _ = db.mk_insert_price(dbconn, "reserved")
    db.load_data(dbconn, prices, handler)


def load_all(db, filename):
    data = json_stream.load(open(filename))

    for k, v in data.items():
        if k == 'products':
            load_products(db, os.path.basename(os.path.dirname(filename)), v)
        elif k == 'terms':
            for k, v in v.items():
                if k == 'OnDemand':
                    load_prices_on_demand(db, v)
                elif k == 'Reserved':
                    load_prices_reserved(db, v)


def load_service(db, filename):
    """
    Works through resource file by entering everything into sqlite
    """
    service = os.path.basename(os.path.dirname(filename))
    print("#####", service)
    load_all(db, filename)
    # load_products(db, filename)
    # load_prices_on_demand(db, filename)
    # load_prices_reserved(db, filename)
