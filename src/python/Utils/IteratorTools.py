#! /usr/bin/env python

from builtins import str, map
import collections.abc
from itertools import islice, chain, groupby

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


def getChunk(arr, step):
    """
    Return chunk of entries from given array and step, it is similar in behavior to grouper
    function but instead of returning new list it provides a generator iterable object.
    :param arr: input array of data
    :param step: step to iterate
    :return: generator, set of slices with number of entries equal to step of iteration
    """
    for i in range(0, len(arr), step):
        yield arr[i:i + step]


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
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
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
    elif isinstance(data, collections.abc.Mapping):
        return dict(list(map(convertFromUnicodeToBytes, list(data.items()))))
    elif isinstance(data, collections.abc.Iterable):
        return type(data)(list(map(convertFromUnicodeToBytes, data)))
    else:
        return data


def makeListElementsUnique(listObj):
    """
    Given a list of lists or a list of tuples, find all duplicate elements
    and make them unique.
    :param listObj: an unsorted list of lists or a list of tuples, e.g.:
        [[1, 1], [1, 5], [1, 1]]; or
        [(1, 1), (1, 5), (1, 1)]
    :return: the same list object but with no duplicates

    Source: https://stackoverflow.com/questions/2213923/removing-duplicates-from-a-list-of-lists
    """
    listObj.sort()
    return list(k for k, _ in groupby(listObj))
