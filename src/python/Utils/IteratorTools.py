#! /usr/bin/env python
from __future__ import division, print_function

from future.utils import viewitems
from builtins import str, map
import collections
from itertools import islice, chain

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
    for k, v in viewitems(u):
        if isinstance(v, collections.Mapping):
            r = nestedDictUpdate(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def convertFromUnicodeToBytes(data):
    """
    code fram
    http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
    """
    if isinstance(data, str):
        return data.encode('utf-8')
    elif isinstance(data, collections.Mapping):
        return dict(list(map(convertFromUnicodeToBytes, viewitems(data))))
    elif isinstance(data, collections.Iterable):
        return type(data)(list(map(convertFromUnicodeToBytes, data)))
    else:
        return data
