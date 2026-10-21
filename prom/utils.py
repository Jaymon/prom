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

