#!/usr/bin/python

"""
A test of a emulator set up
"""

import unittest

from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.RequestManager.RequestManager import RequestManager


class EmulatorSetupTest(unittest.TestCase):
    """
    A test of a emulator set up
    """

    def testEmulator(self):
        """
        The the remaining old-style emulators
        """

        EmulatorHelper.setEmulators(True, False, False, True)
        self.assertEqual(PhEDEx().wrapped.__module__,
                         'WMQuality.Emulators.PhEDExClient.PhEDEx')
        self.assertEqual(RequestManager().wrapped.__module__,
                         'WMQuality.Emulators.RequestManagerClient.RequestManager')

        self.assertEqual(PhEDEx().__class__.__name__, 'PhEDEx')
        self.assertEqual(RequestManager().__class__.__name__, 'RequestManager')

        EmulatorHelper.resetEmulators()
        self.assertEqual(PhEDEx().wrapped.__module__,
                         'WMCore.Services.PhEDEx.PhEDEx')
        self.assertEqual(RequestManager().wrapped.__module__,
                         'WMCore.Services.RequestManager.RequestManager')

        self.assertEqual(PhEDEx().__class__.__name__, 'PhEDEx')
        self.assertEqual(RequestManager().__class__.__name__, 'RequestManager')


if __name__ == "__main__":
    unittest.main()
