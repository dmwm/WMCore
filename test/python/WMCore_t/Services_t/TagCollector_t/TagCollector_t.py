'''
Created on Dec 16, 2016

'''
from __future__ import (division, print_function)

import unittest
import logging

from collections import defaultdict
from WMCore.Services.TagCollector.TagCollector import TagCollector
from WMCore.Services.TagCollector.XMLUtils import xml_parser


class TagCollectorTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        # using the default production server
        self.tagCollecor = TagCollector()
        self.testReleasesMap = "/tmp/testReleasesParsing.map"
        self.testReleasesXML = "/tmp/testReleasesParsingXML"

        return

    def _getResult(self, testReleasesMap=None, testReleasesXML=None):
        """
        _getResult_

        Test the XML formatted information parsed by parseCvmfsReleasesXML
        """
        try:
            self.tagCollecor.parseCvmfsReleasesXML(releasesMap=testReleasesMap, releasesXML=testReleasesXML)
            with open(testReleasesXML, 'r', encoding='utf-8') as f:
                result = f.read()
            f.close()
        except Exception:
            logging.error('Something went wrong parsing the test releasesMap into the sample XML file')
            raise

        return result
    
    def data(self, testReleasesMap=None, testReleasesXML=None):
        """
        _data_

        Test: Fetch data from cvmfs releases.map
        """

        data = self._getResult(testReleasesMap=testReleasesMap, testReleasesXML=testReleasesXML)
        pkey = 'architecture'
        for row in xml_parser(data, pkey):
            yield row[pkey]

    def releases(self, arch=None, testReleasesMap=None, testReleasesXML=None):
        """
        _releases_

        Test: Yield CMS releases known in tag collector from cvmfs releases.map
        """
        arr = []
        for row in self.data(testReleasesMap=testReleasesMap, testReleasesXML=testReleasesXML):
            if arch:
                if arch == row['name']:
                    for item in row['project']:
                        arr.append(item['label'])
            else:
                for item in row['project']:
                    arr.append(item['label'])
        return list(set(arr))

    def architectures(self, arch=None, testReleasesMap=None, testReleasesXML=None):
        """
        _architectures_

        Test: Yield CMS architectures known in tag collector from cvfms releases.map
        """
        arr = []
        for row in self.data(testReleasesMap=testReleasesMap, testReleasesXML=testReleasesXML):
            arr.append(row['name'])
        return list(set(arr))
    
    def releases_by_architecture(self, testReleasesMap=None, testReleasesXML=None):
        """
        _releases_by_architecture_

        Test: returns CMS architectures and realease in dictionary format with cvmfs as main source
        """
        arch_dict = defaultdict(list)
        for row in self.data(testReleasesMap=testReleasesMap, testReleasesXML=testReleasesXML):
            releases = set()
            for item in row['project']:
                releases.add(item['label'])
            arch_dict[row['name']].extend(list(releases))
        return dict(arch_dict)
    
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

        releasesMap = (
                        "architecture=el8_amd64_gcc12;label=CMSSW_15_0_15_patch3;type=Production;state=Announced;prodarch=1;default_micro_arch=x86-64-v3;\n"
                        "architecture=el8_amd64_gcc12;label=CMSSW_15_0_15_patch4;type=Production;state=Announced;prodarch=1;default_micro_arch=x86-64-v3;\n"
                        "architecture=el8_amd64_gcc13;label=CMSSW_16_0_0_pre1_FASTPU;type=Development;state=Announced;prodarch=1;default_micro_arch=x86-64-v3;\n"
                        "architecture=el9_amd64_gcc12;label=CMSSW_15_0_15_patch3;type=Production;state=Announced;prodarch=1;default_micro_arch=x86-64-v3;\n"
                        "architecture=el9_amd64_gcc12;label=CMSSW_15_0_15_patch4;type=Production;state=Announced;prodarch=1;default_micro_arch=x86-64-v3;\n"
                    )
        
        with open(self.testReleasesMap, "w", encoding="utf-8") as f:
            f.write(releasesMap)

        releasesCvmfs = self.releases(testReleasesMap=self.testReleasesMap, testReleasesXML=self.testReleasesXML)
        architecturesCvmfs = self.architectures(testReleasesMap=self.testReleasesMap, testReleasesXML=self.testReleasesXML)
        realsese_by_arch_cvmfs = self.releases_by_architecture(testReleasesMap=self.testReleasesMap, testReleasesXML=self.testReleasesXML)

        self.assertIn('CMSSW_15_0_15_patch3', releasesCvmfs)
        self.assertIn('CMSSW_15_0_15_patch4', releasesCvmfs)
        self.assertIn('el8_amd64_gcc12', architecturesCvmfs)
        self.assertIn('el9_amd64_gcc12', architecturesCvmfs)
        self.assertEqual(len(architecturesCvmfs), len(realsese_by_arch_cvmfs))
        self.assertEqual(sorted(self.releases(arch='el8_amd64_gcc12', testReleasesMap=self.testReleasesMap, testReleasesXML=self.testReleasesXML)),
                         sorted(realsese_by_arch_cvmfs.get('el8_amd64_gcc12')))

        return

if __name__ == '__main__':
    unittest.main()
