from __future__ import print_function, division
import time
import traceback
import logging


class MemoryCacheStruct(object):
    """
    WARNING: This can be used in multi thread by registering on GenericDataCache
    But this cache is not thread saft.
    """

    def __init__(self, expire, func, initCacheValue=None, kwargs=None):
        """
        expire is the seconds which cache will be refreshed when cache is older than the expire.
        func is the fuction which cache data is retrieved
        kwargs are func arguments for cache data
        """
        self.data = initCacheValue
        self.expire = expire
        self.func = func
        if kwargs == None:
            kwargs = {}
        self.kwargs = kwargs
        self.lastUpdated = -1

    def isDataExpired(self):
        if self.lastUpdated == -1:
            return True
        if (int(time.time()) - self.lastUpdated) > self.expire:
            return True
        return False

    def getData(self, noFail=True):
        if self.isDataExpired():
            try:
                self.data = self.func(**self.kwargs)
                self.lastUpdate = int(time.time())
            except Exception:
                if noFail:
                    logging.error(traceback.format_exc())
                else:
                    raise
        return self.data


class CacheExistException(Exception):
    def __init__(self, cacheName):
        Exception.__init__(self, cacheName)
        self.msg = CacheExistException.__class__.__name__
        self.error = "Cache already registered %s" % cacheName

    def __str__(self):
        return "%s: %s" % (self.msg, self.error)


class CacheWithWrongStructException(Exception):
    def __init__(self, cacheName):
        Exception.__init__(self, cacheName)
        self.msg = CacheWithWrongStructException.__class__.__name__
        self.error = "Cache should be instance of MemoryCacheStruct %s" % cacheName

    def __str__(self):
        return "%s: %s" % (self.msg, self.error)


class GenericDataCache(object):
    _dataCache = {}

    @staticmethod
    def getCacheData(cacheName):
        return GenericDataCache._dataCache[cacheName]

    @staticmethod
    def registerCache(cacheName, memoryCache):
        """
        cacheName, unique name for the cache
        memoryCache MemoryCacheStruct instance.
        """
        if cacheName in GenericDataCache._dataCache:
            raise CacheExistException(cacheName)
        elif not isinstance(memoryCache, MemoryCacheStruct):
            raise CacheWithWrongStructException(cacheName)
        else:
            GenericDataCache._dataCache[cacheName] = memoryCache
