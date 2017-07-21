import unittest

import WMCore.Storage.Backends
import WMCore.Storage.Plugins.TestWinImpl
from WMCore.Storage.Registry import retrieveStageOutImpl


class TestRegistry(unittest.TestCase):
    def testRetrieveProperBackends(self):
        # make sure the default still gives the old behavior
        self.assertTrue(isinstance(retrieveStageOutImpl('test-win'),
                                   WMCore.Storage.Backends.UnittestImpl.WinImpl))
        # make sure you can still explicitly get the old one
        self.assertTrue(isinstance(retrieveStageOutImpl('test-win', useNewVersion=False),
                                   WMCore.Storage.Backends.UnittestImpl.WinImpl))
        # make sure that you get the new one if you want it
        self.assertTrue(retrieveStageOutImpl('test-win', useNewVersion=True),
                        WMCore.Storage.Plugins.TestWinImpl.TestWinImpl)


if __name__ == "__main__":
    unittest.main()
