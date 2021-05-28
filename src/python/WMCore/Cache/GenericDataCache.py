from __future__ import print_function, division
from builtins import str
from builtins import object
import time
import logging


class MemoryCacheStruct(object):
    """
    WARNING: This can be used in multi thread by registering on GenericDataCache
    But this cache is not thread safe.
    """

    def __init__(self, expire, func, initCacheValue=None, logger=None, kwargs=None):
        """
        expire is the seconds which cache will be refreshed when cache is older than the expire.
        func is the fuction which cache data is retrieved
        kwargs are func arguments for cache data
        """
        kwargs = kwargs or {}
        self.data = initCacheValue
        self.expire = expire
        self.func = func

        self.kwargs = kwargs
        self.lastUpdated = -1
        self.logger = logger if logger else logging.getLogger()

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
                self.lastUpdated = int(time.time())
            except Exception as exc:
                if noFail:
                    msg = "Passive failure while looking data up in the memory cache. Error: %s" % str(exc)
                    self.logger.warning(msg)
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
            logging.info("Creating generic cache named: %s", cacheName)
            GenericDataCache._dataCache[cacheName] = memoryCache

    @staticmethod
    def cacheExists(cacheName):
        """
        Return True if provided cache name is already cached, else False.
        :param cacheName: cache name string
        :return: boolean
        """
        return cacheName in GenericDataCache._dataCache
