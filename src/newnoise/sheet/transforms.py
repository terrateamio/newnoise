def prefix(value, prefix=None):
    """
    Chops the front of a string if `prefix` character is found. Operates only
    on the first instance of `prefix` and ignores all other instances.
    """
    if prefix is None:
        return value
    v_parts = value.split(prefix, 1)
    if len(v_parts) == 2:
        value = v_parts[1]
    return value


def suffix(value, suffix=None):
    """
    Chops the end of a string if `suffix` character is found. Operates only
    on the last instance of `suffix` and ignores all other instances.
    """
    if suffix is None:
        return value
    v_parts = value.split(suffix, 1)
    if len(v_parts) == 2:
        value = v_parts[0]
    return value


def clean(attr, p=None, s=None):
    """
    Shorthand for operating on a `prefix` and `suffix` at the same time.
    """
    attr = prefix(attr, p)
    attr = suffix(attr, s)
    return attr


def mk_clean_fun(p=None, s=None):
    """
    Allows for delayed execution of `clean`.
    """
    def f(attr):
        return clean(attr, p=p, s=s)
    return f

