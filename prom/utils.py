from .compat import *


def make_list(vals):
    """make vals a list, no matter what

    :param vals: list, vals should always be a list or tuple (eg, *args passed
        as args)
    """
    ret = []
    if isinstance(vals, basestring):
        ret.append(vals)

    else:
        try:
            for val in vals:
                if isinstance(val, basestring):
                    ret.append(val)

                elif isinstance(val, (list, tuple)):
                    ret.extend(val)

                else:
                    try:
                        r = list(val)
                        ret.extend(r)

                    except TypeError:
                        ret.append(val)

        except TypeError:
            # TypeError: * is not iterable
            ret.append(vals)

    return ret


def make_dict(*fields):
    """lot's of methods take a dict or kwargs, this combines those

    Basically, we do a lot of def method(fields, **kwargs) and we want to merge
    those into one super dict with kwargs taking precedence, this does that

    fields -- dict -- a passed in dict
    fields_kwargs -- dict -- usually a **kwargs dict from another function

    return -- dict -- a merged fields and fields_kwargs
    """
    ret = {}

    for d in fields:
        if d:
            ret.update(d)

    return ret

