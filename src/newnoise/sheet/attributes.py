from . import data, env


def assert_usage_amount_all_or_nothing(iter):
    for product_match_set, pricing_match_set, price_data in iter:

        def has_usage(pi):
            return "start_usage_amount" in pi or "end_usage_amount" in pi

        if any([has_usage(pi) for pi in pricing_match_set]) and not all(
            [has_usage(pi) for pi in pricing_match_set]
        ):
            raise Exception(
                "Missing startUsageAmount or endUsageAmount {} {} {}".format(
                    product_match_set, pricing_match_set, price_data
                )
            )

        yield (product_match_set, pricing_match_set, price_data)


# primitive functions


def attr(key, attr_data, t=None):
    if key in attr_data:
        attr = attr_data[key]
        if t:
            attr = t(attr)
        return attr
    else:
        return None


def product(key, t=None):
    def f(row, _price_attrs):
        return attr(key, row[data.ATTRIBUTES], t=t)

    return f


def price(key, t=None):
    def f(_row, price_attrs):
        return attr(key, price_attrs, t=t)

    return f


def const(v):
    def f(_row, _price_attrs):
        return v

    return f


def with_(d, v):
    d.update(v)
    return d


# attribute specific functions


def priced_by(units):
    units = {unit.lower(): by for (unit, by) in units.items()}

    def f(_row, price_attrs):
        unit = price_attrs["unit"].lower()
        return units.get(unit)

    return f


def priced_by_time(_row, price_attrs):
    unit = price_attrs["unit"].lower()
    if unit in env.PER_TIME:
        return "t"
    elif unit in env.IGNORE_UNITS or unit in env.PER_OPERATION or unit in env.PER_DATA:
        return None
    else:
        raise Exception("price_by_hours unknoown unit: {}".format(price_attrs))


def priced_by_ops(_row, price_attrs):
    unit = price_attrs["unit"].lower()
    if unit in env.PER_OPERATION:
        return "o"
    elif unit in env.IGNORE_UNITS or unit in env.PER_TIME or unit in env.PER_DATA:
        return None
    else:
        raise Exception("price_by_operations unknoown unit: {}".format(price_attrs))


def priced_by_data(_row, price_attrs):
    unit = price_attrs["unit"].lower()
    if unit in env.PER_DATA:
        return "d"
    elif unit in env.IGNORE_UNITS or unit in env.PER_TIME or unit in env.PER_OPERATION:
        return None
    else:
        raise Exception("price_by_data unknoown unit: {}".format(price_attrs))
