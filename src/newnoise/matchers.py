from . import data


# required fields


def required_keys(data, keys):
    return len(set(keys) - set(data.keys())) == 0


def required_attrs(row, keys):
    return required_keys(row[data.ATTRIBUTES], keys)


def required_prices(row, keys):
    for price in data.prices(row):
        if required_keys(price, keys):
            return True
    return False


# product attribute matching


def product_attr(row, key, v=None, p=None):
    if key in row[data.ATTRIBUTES]:
        row_attr = row[data.ATTRIBUTES][key]
        if v and row_attr == v:
            return True
        if p and row_attr.startswith(p):
            return True
    return False


def product_servicecode(row, **kw):
    return product_attr(row, "servicecode", **kw)


def product_servicename(row, **kw):
    return product_attr(row, "servicename", **kw)


def product_operation(row, **kw):
    return product_attr(row, "operation", **kw)


def product_usagetype(row, **kw):
    return product_attr(row, "usagetype", **kw)


# price attribute matching


def price_attr(row, key, v=None, p=None):
    for price in data.prices_iter(row):
        if key in price:
            price_item = price[key]
            if v and price_item == v:
                return True
            if p and price_item.startswith(p):
                return True
    return False


def price_purchaseoption(row, **kw):
    return price_attr(row, "purchaseOption", **kw)


def price_effectivedatestart(row, **kw):
    return price_attr(row, "effectiveDateStart", **kw)


def price_unit(row, **kw):
    return price_attr(row, "unit", **kw)
