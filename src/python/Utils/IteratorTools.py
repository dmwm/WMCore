#! /usr/bin/env python
from __future__ import division, print_function

import collections
from itertools import islice
from itertools import chain

def grouper(iterable, n):
    """
    :param iterable: List of other iterable to slice
    :type: iterable
    :param n: Chunk size for resulting lists
    :type: int
    :return: iterator of the sliced list

    Source: http://stackoverflow.com/questions/3992735/python-generator-that-groups-another-iterable-into-groups-of-n
    """
    iterable = iter(iterable)
    return iter(lambda: list(islice(iterable, n)), [])


def flattenList(doubleList):
    """
    Make flat a list of lists.
    """
    return list(chain.from_iterable(doubleList))

def nestedDictUpdate(d, u):
    """
    Code from Alex Matelli
    http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    """
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = nestedDictUpdate(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def convertFromUnicodeToStr(data):
    """
    code fram
    http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
    """
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(list(map(convertFromUnicodeToStr, data.iteritems())))
    elif isinstance(data, collections.Iterable):
        return type(data)(list(map(convertFromUnicodeToStr, data)))
    else:
        return data
