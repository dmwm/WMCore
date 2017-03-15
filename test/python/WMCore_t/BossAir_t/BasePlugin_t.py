#!/usr/bin/python

"""
_BasePlugin_t_

BasePlugin unittests
"""

from __future__ import division

import unittest

from WMCore_t.BossAir_t.BossAir_t import BossAirTest

from WMCore.BossAir.Plugins.BasePlugin import BasePlugin


class BasePluginTest(BossAirTest):
    """
    _BasePluginTest_

    Tests for BasePlugin
    """

    def testScramArchToOS(self):
        """
        _testScramArchToOS_

        Test the conversion of the ScramArch string to requiredOS
        """
        bp = BasePlugin(config=None)

        self.assertEqual(bp.scramArchtoRequiredOS('slc5_blah_blah'),'rhel6')
        self.assertEqual(bp.scramArchtoRequiredOS('slc6_blah_blah'),'rhel6')
        self.assertEqual(bp.scramArchtoRequiredOS('slc7_blah_blah'),'rhel7')

        self.assertEqual(bp.scramArchtoRequiredOS(None),'any')

        self.assertEqual(bp.scramArchtoRequiredOS(['slc6_blah_blah','slc7_blah_blah']),'rhel6,rhel7')

        return


if __name__ == '__main__':
    unittest.main()
