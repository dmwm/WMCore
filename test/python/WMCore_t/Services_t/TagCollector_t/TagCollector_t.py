'''
Created on Dec 16, 2016

'''
from __future__ import (division, print_function)

import unittest

from WMCore.Services.TagCollector.TagCollector import TagCollector


class TagCollectorTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        # using the default production server
        self.tagCollecor = TagCollector()
        return

    def testTagCollecorMethods(self):
        """
        _testTagCollecorMethods_
        """
        releases = self.tagCollecor.releases()
        architectures = self.tagCollecor.architectures()
        realsese_by_arch = self.tagCollecor.releases_by_architecture()
        microarch_by_release = self.tagCollecor.defaultMicroArchVersionNumberByRelease()
        microarch_testCMSSW_15 = self.tagCollecor.getGreaterMicroarchVersionNumber("CMSSW_15_0_0_pre3")
        microarch_testCMSSW_12_15 = self.tagCollecor.getGreaterMicroarchVersionNumber("CMSSW_12_4_0_pre2,CMSSW_15_0_0_pre3")
        microarch_testCMSSW_7_12 = self.tagCollecor.getGreaterMicroarchVersionNumber("CMSSW_7_1_10_patch2,CMSSW_12_4_0_pre2", rel_microarchs=microarch_by_release)
        self.assertIn('CMSSW_7_1_10_patch2', releases)
        self.assertIn('slc6_amd64_gcc493', architectures)
        self.assertIn('slc7_amd64_gcc620', architectures)
        self.assertEqual(len(architectures), len(realsese_by_arch))
        self.assertEqual(sorted(self.tagCollecor.releases('slc6_amd64_gcc493')),
                         sorted(realsese_by_arch.get('slc6_amd64_gcc493')))
        self.assertEqual(0, microarch_by_release['CMSSW_7_1_10_patch2'])
        self.assertEqual(3, microarch_by_release['CMSSW_15_0_0_pre3'])
        self.assertEqual(3, microarch_testCMSSW_12_15)
        self.assertEqual(3, microarch_testCMSSW_15)
        self.assertEqual(0, microarch_testCMSSW_7_12)

        return


if __name__ == '__main__':
    unittest.main()
