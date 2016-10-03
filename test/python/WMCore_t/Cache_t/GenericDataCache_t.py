"""
_WMConfigCache_t_

Test class for the WMConfigCache
"""

import unittest
import time
from WMCore.Cache.GenericDataCache import GenericDataCache, CacheExistException, \
                          CacheWithWrongStructException, MemoryCacheStruct

class GenericDataCacheTest(unittest.TestCase):

    def testBasic(self):
        """
        _testBasic_

        Basic stuff.
        """
        mc = MemoryCacheStruct(1, lambda x: int(time.time()), {'x':1})
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
        self.assertFalse(mc2.lastUpdate == -1)
        
        return

if __name__ == "__main__":
    unittest.main()
