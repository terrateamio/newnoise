def prefix(value, prefix=None):
    if prefix is None:
        return value
    v_parts = value.split(prefix)
    if len(v_parts) > 1:
        value = v_parts[1]
    return value


def suffix(value, suffix=None):
    if suffix is None:
        return value
    v_parts = value.split(suffix)
    if len(v_parts) > 1:
        value = v_parts[0]
    return value


def clean(attr, p=None, s=None):
    attr = prefix(attr, p)
    attr = suffix(attr, s)
    return attr


def mk_clean_fun(p=None, s=None):
    def f(attr):
        return clean(attr, p=p, s=s)
    return f

