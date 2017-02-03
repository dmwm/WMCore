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
        self.assertIn('CMSSW_7_1_10_patch2', releases)
        self.assertIn('slc6_amd64_gcc493', architectures)
        self.assertIn('slc7_amd64_gcc620', architectures)
        self.assertEqual(len(architectures), len(realsese_by_arch))
        self.assertEqual(sorted(self.tagCollecor.releases('slc6_amd64_gcc493')),
                         sorted(realsese_by_arch.get('slc6_amd64_gcc493')))

        return


if __name__ == '__main__':
    unittest.main()
