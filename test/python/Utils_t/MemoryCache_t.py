#!/usr/bin/env python
"""
Unittests for MemoryCache object
"""

import unittest
from time import sleep

from Utils.MemoryCache import MemoryCache, MemoryCacheException
#from Utils.PythonVersion import PY3


class MemoryCacheTest(unittest.TestCase):
    """
    unittest for MemoryCache functions
    """

    def setUp(self):
        self.assertItemsEqual = self.assertCountEqual

    def testBasics(self):
        cache = MemoryCache(1, [])
        self.assertItemsEqual(cache.getCache(), [])
        cache.setCache(["item1", "item2"])
        self.assertItemsEqual(cache.getCache(), ["item1", "item2"])
        # wait for cache to expiry, wait for 2 secs
        sleep(2)
        self.assertRaises(MemoryCacheException, cache.getCache)
        cache.setCache(["item4"])
        # and the cache is alive again
        self.assertItemsEqual(cache.getCache(), ["item4"])

    def testCacheSet(self):
        cache = MemoryCache(2, set())
        self.assertItemsEqual(cache.getCache(), set())
        cache.setCache(set(["item1", "item2"]))
        self.assertItemsEqual(cache.getCache(), ["item1", "item2"])
        cache.addItemToCache("item3")
        self.assertItemsEqual(cache.getCache(), ["item1", "item2", "item3"])
        cache.addItemToCache(["item4"])
        self.assertItemsEqual(cache.getCache(), ["item1", "item2", "item3", "item4"])
        cache.addItemToCache(set(["item5"]))
        self.assertItemsEqual(cache.getCache(), ["item1", "item2", "item3", "item4", "item5"])
        self.assertTrue("item2" in cache)
        self.assertFalse("item222" in cache)

    def testCacheList(self):
        cache = MemoryCache(2, [])
        self.assertItemsEqual(cache.getCache(), [])
        cache.setCache(["item1", "item2"])
        self.assertItemsEqual(cache.getCache(), ["item1", "item2"])
        cache.addItemToCache("item3")
        self.assertItemsEqual(cache.getCache(), ["item1", "item2", "item3"])
        cache.addItemToCache(["item4"])
        self.assertItemsEqual(cache.getCache(), ["item1", "item2", "item3", "item4"])
        cache.addItemToCache(set(["item5"]))
        self.assertItemsEqual(cache.getCache(), ["item1", "item2", "item3", "item4", "item5"])
        self.assertTrue("item2" in cache)
        self.assertFalse("item222" in cache)

    def testCacheDict(self):
        cache = MemoryCache(2, {})
        self.assertItemsEqual(cache.getCache(), {})
        cache.setCache({"item1": 11, "item2": 22})
        self.assertItemsEqual(cache.getCache(), {"item1": 11, "item2": 22})
        cache.addItemToCache({"item3": 33})
        self.assertItemsEqual(cache.getCache(), {"item1": 11, "item2": 22, "item3": 33})
        self.assertTrue("item2" in cache)
        self.assertFalse("item222" in cache)
        # test exceptions
        self.assertRaises(TypeError, cache.addItemToCache, "item4")
        self.assertRaises(TypeError, cache.addItemToCache, ["item4"])

    def testSetDiffTypes(self):
        cache = MemoryCache(2, set())
        self.assertItemsEqual(cache.getCache(), set())
        cache.setCache({"item1", "item2"})
        self.assertRaises(TypeError, cache.setCache, ["item3"])


if __name__ == "__main__":
    unittest.main()
