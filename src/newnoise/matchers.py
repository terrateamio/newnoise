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


def product_attr(row, key, t=None, v=None, p=None, s=None, c=None):
    """
    t: apply transform function t before checks
    v: check v exact match for attr
    p: check p is found at start of attr
    s: check s is substring of attr
    """
    if key in row[data.ATTRIBUTES]:
        row_attr = row[data.ATTRIBUTES][key]

        if callable(t):
            row_attr = t(row_attr)

        if v and row_attr == v:
            return True

        if p and row_attr.startswith(p):
            return True

        if s and row_attr.endswith(s):
            return True

        if c and c in row_attr:
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


def product_group(row, **kw):
    return product_attr(row, "group", **kw)


# price attribute matching


def price_attr(row, key, v=None, p=None, ccy=None):
    """
    v: check v exact match for attr
    p: check p is found at start of attr
    ccy: check ccy is match for at least one price (note: may not be what we want)
    """
    for price in [x for outer in row[data.PRICES].values() for x in outer]:
        # if k is in price, it matches. facilitates currency matching
        # where currency type is a key with price behind it, instead of
        # a value. currency is the only field that does that. key is
        # a required param in all cases, so key should match ccy when
        # ccy match is performed
        if ccy and key == ccy and ccy in price:
            return True

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


def price_currency(row, ccy=None):
    """
    this function works around unusual nature of price structure found in input
    csv. From the caller's POV, it operates the same way the other matchers,
    making it easy to check if a row's prices are ccy or not.
    """
    if ccy is None:
        return True
    # this syntax looks strange. that is because the currency's key
    # is the same as its value. if the price is in USD, the key 'USD'
    # is present. if price is in CNY, the key 'CNY' is present
    return price_attr(row, ccy, ccy=ccy)
