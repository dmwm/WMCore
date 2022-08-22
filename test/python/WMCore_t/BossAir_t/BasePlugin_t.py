#!/usr/bin/python

"""
_BasePlugin_t_

BasePlugin unittests
"""

from __future__ import division

import unittest

from WMCore_t.BossAir_t.BossAir_t import BossAirTest

from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException


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

        self.assertEqual(bp.scramArchtoRequiredOS('slc5_blah_blah'), 'rhel6')
        self.assertEqual(bp.scramArchtoRequiredOS('slc6_blah_blah'), 'rhel6')
        self.assertEqual(bp.scramArchtoRequiredOS('slc7_blah_blah'), 'rhel7')
        self.assertEqual(bp.scramArchtoRequiredOS('cc8_blah_blah'), 'rhel8')
        self.assertEqual(bp.scramArchtoRequiredOS('cs8_blah_blah'), 'rhel8')
        self.assertEqual(bp.scramArchtoRequiredOS('alma8_blah_blah'), 'rhel8')
        self.assertEqual(bp.scramArchtoRequiredOS('el8_blah_blah'), 'rhel8')

        self.assertEqual(bp.scramArchtoRequiredOS(None), 'any')
        self.assertEqual(bp.scramArchtoRequiredOS(""), 'any')
        self.assertEqual(bp.scramArchtoRequiredOS([]), 'any')

        self.assertEqual(bp.scramArchtoRequiredOS(['slc6_blah_blah', 'slc7_blah_blah']), 'rhel6,rhel7')
        self.assertEqual(bp.scramArchtoRequiredOS(['slc6_blah_blah', 'alma8_blah_blah']), 'rhel6,rhel8')

        # unexpected case, a ScramArch being requested without the map implemented
        self.assertEqual(bp.scramArchtoRequiredOS('slc1_blah_blah'), '')
        return

    def testScramArchtoRequiredArch(self):
        """
        Test mapping of ScramArch to a given architecture
        """
        bp = BasePlugin(config=None)

        self.assertEqual(bp.scramArchtoRequiredArch('slc5_amd64_gcc481'), 'X86_64')
        self.assertEqual(bp.scramArchtoRequiredArch('slc6_amd64_gcc630'), 'X86_64')
        self.assertEqual(bp.scramArchtoRequiredArch('slc7_amd64_gcc10'), 'X86_64')
        self.assertEqual(bp.scramArchtoRequiredArch('slc7_aarch64_gcc700'), 'aarch64')
        self.assertEqual(bp.scramArchtoRequiredArch('slc7_ppc64le_gcc9'), 'ppc64le')
        self.assertIsNone(bp.scramArchtoRequiredArch(None))
        self.assertIsNone(bp.scramArchtoRequiredArch(None))
        with self.assertRaises(BossAirPluginException):
            bp.scramArchtoRequiredArch("slc7_BLAH_gcc700")

        self.assertEqual(bp.scramArchtoRequiredArch(['slc5_amd64_gcc481', 'slc6_amd64_gcc630']), 'X86_64')
        test = (bp.scramArchtoRequiredArch(['slc7_amd64_gcc10', 'slc7_aarch64_gcc700'])).split(',')
        test.sort()
        self.assertEqual(test, ['X86_64','aarch64'])
        test = (bp.scramArchtoRequiredArch(['slc7_amd64_gcc10', 'slc7_aarch64_gcc700', 'slc7_ppc64le_gcc9'])).split(',')
        test.sort()
        self.assertEqual(test, ['X86_64','aarch64','ppc64le'])
        test = (bp.scramArchtoRequiredArch(['slc7_aarch64_gcc700', 'slc7_ppc64le_gcc9'])).split(',')
        test.sort()
        self.assertEqual(test, ['aarch64', 'ppc64le'])

        return

if __name__ == '__main__':
    unittest.main()
