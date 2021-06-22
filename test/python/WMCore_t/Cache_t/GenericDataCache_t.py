"""
_WMConfigCache_t_

Test class for the WMConfigCache
"""
from __future__ import print_function, division

import unittest
import time

from Utils.PythonVersion import PY3

from WMCore.Cache.GenericDataCache import GenericDataCache, CacheExistException, \
                          CacheWithWrongStructException, MemoryCacheStruct

from Utils.PythonVersion import PY3

class Foo(object):
    pass


class GenericDataCacheTest(unittest.TestCase):

    def setUp(self):
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testBasic(self):
        """
        _testBasic_

        Basic stuff.
        """
        mc = MemoryCacheStruct(1, lambda x: int(time.time()), kwargs={'x':1})
        self.assertIsNone(mc.data)
        self.assertEqual(mc.lastUpdated, -1)

        GenericDataCache.registerCache("test", mc)
        with self.assertRaises(CacheExistException):
            GenericDataCache.registerCache("test", mc)
        with self.assertRaises(CacheWithWrongStructException):
            GenericDataCache.registerCache("test2", {'a': 1})
        mc2 = GenericDataCache.getCacheData('test')
        before = mc2.getData()
        time.sleep(2)
        after = mc2.getData()
        self.assertFalse(before == after)
        self.assertFalse(mc2.lastUpdated == -1)

        return

    def testBasicInit(self):
        """
        _testBasicInit_

        Test init values
        """
        mc = MemoryCacheStruct(0, lambda x: x, initCacheValue=Foo())
        self.assertIsInstance(mc.data, Foo)

        mc1 = MemoryCacheStruct(0, lambda x: sum(x), [], kwargs={'x': [1, 2]})
        self.assertEqual(mc1.data, [])
        after = mc1.getData()
        self.assertEqual(after, 3)

        mc2 = MemoryCacheStruct(0, lambda x: x, {}, kwargs={'x': {'one':1, 'two':2}})
        self.assertEqual(mc2.data, {})
        after = mc2.getData()
        self.assertCountEqual(after.keys(), ['one', 'two']) if PY3 else self.assertItemsEqual(after.keys(), ['one', 'two'])

        return

    def testExists(self):
        """
        Tests whether a given cache already exists
        """
        mc = MemoryCacheStruct(1, lambda x: int(time.time()), kwargs={'x':1})
        self.assertFalse(GenericDataCache.cacheExists("tCache"))

        GenericDataCache.registerCache("tCache", mc)
        self.assertTrue(GenericDataCache.cacheExists("tCache"))
        self.assertFalse(GenericDataCache.cacheExists("tCache2"))

if __name__ == "__main__":
    unittest.main()
