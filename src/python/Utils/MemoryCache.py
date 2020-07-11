#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple in-memory and non-thread safe cache.
Note that this module does not support home-made object types, since there is
an explicit data type check when adding a new item to the cache.

It raises a TypeError exception if the cache data type chagens;
or if the user tries to extend the cache with an incompatible
data type.
"""

from __future__ import (print_function, division)
from time import time


class MemoryCacheException(Exception):
    def __init__(self, message):
        super(MemoryCacheException, self).__init__(message)


class MemoryCache(object):

    __slots__ = ["lastUpdate", "expiration", "_cache"]

    def __init__(self, expiration, initialData=None):
        """
        Initializes cache object

        :param expiration: expiration time in seconds
        :param initialData: initial value for the cache
        """
        self.lastUpdate = int(time())
        self.expiration = expiration
        self._cache = initialData

    def __contains__(self, item):
        """
        Check whether item is in the current cache
        :param item: a simple object (string, integer, etc)
        :return: True if the object can be found in the cache, False otherwise
        """
        return item in self._cache

    def isCacheExpired(self):
        """
        Evaluate whether the cache has already expired, returning
        True if it did, otherwise it returns False
        """
        return self.lastUpdate + self.expiration < int(time())

    def getCache(self):
        """
        Raises an exception if the cache has expired, otherwise returns
        its data
        """
        if self.isCacheExpired():
            expiredSince = int(time()) - (self.lastUpdate + self.expiration)
            raise MemoryCacheException("Memory cache expired for %d seconds" % expiredSince)
        return self._cache

    def setCache(self, inputData):
        """
        Refresh the cache with the content provided (refresh its expiration as well)
        This method enforces the user to not change the cache data type
        :param inputData: data to store in the cache
        """
        if not isinstance(self._cache, type(inputData)):
            raise TypeError("Current cache data type: %s, while new value is: %s" %
                            (type(self._cache), type(inputData)))
        self.lastUpdate = int(time())
        self._cache = inputData

    def addItemToCache(self, inputItem):
        """
        Adds new item(s) to the cache, without resetting its expiration.
        It, of course, only works for data caches of type: list, set or dict.
        :param inputItem: additional item to be added to the current cached data
        """
        if isinstance(self._cache, set) and isinstance(inputItem, (list, set)):
            # extend another list or set into a set
            self._cache.update(inputItem)
        elif isinstance(self._cache, set) and isinstance(inputItem, (int, float, str)):
            # add a simple object (integer, string, etc) to a set
            self._cache.add(inputItem)
        elif isinstance(self._cache, list) and isinstance(inputItem, (list, set)):
            # extend another list or set into a list
            self._cache.extend(inputItem)
        elif isinstance(self._cache, list) and isinstance(inputItem, (int, float, str)):
            # add a simple object (integer, string, etc) to a list
            self._cache.append(inputItem)
        elif isinstance(self._cache, dict) and isinstance(inputItem, dict):
            self._cache.update(inputItem)
        else:
            msg = "Input item type: %s cannot be added to a cache type: %s" % (type(self._cache), type(inputItem))
            raise TypeError("Cache and input item data type mismatch. %s" % msg)
