#!/usr/bin/env python
"""
Test case for DDM
"""
from __future__ import print_function, division

import unittest

from nose.plugins.attrib import attr

from WMCore.Services.DDM.DDM import DDM, DDMReqTemplate
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


class DDMReqTemplateTest(EmulatedUnitTestCase):
    """
    Unit tests for DDM Request templates
    """

    def __init__(self, methodName='runTest'):
        super(DDMReqTemplateTest, self).__init__(methodName=methodName)

    def setUp(self):
        """
        Setup for unit tests
        """
        super(DDMReqTemplateTest, self).setUp()
        self.myddmReq = DDMReqTemplate('copy')
        self.myddmReq['item'] = ['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM']

    def testConstructor(self):
        # Test construct with default values:
        expectedDdmReq = {'group': 'DataOps',
                          'item': [],
                          'site': ['T2_*', 'T1_*_Disk'],
                          'n': None,
                          'cache': None}
        self.ddmReq = DDMReqTemplate('copy')
        self.assertEqual(self.ddmReq, expectedDdmReq)

        # Test bad API:
        with self.assertRaises(ValueError):
            self.ddmReq = DDMReqTemplate('coppy')

        # Test bad request keys types:
        with self.assertRaises(TypeError):
            self.ddmReq = DDMReqTemplate('copy',
                                         item='String instead of List')
        with self.assertRaises(TypeError):
            self.ddmReq = DDMReqTemplate('copy',
                                         item=[],
                                         site='String instead of List')
        with self.assertRaises(TypeError):
            self.ddmReq = DDMReqTemplate('copy',
                                         item=[],
                                         site=[],
                                         group=['List instead of String'])
        with self.assertRaises(TypeError):
            self.ddmReq = DDMReqTemplate('copy',
                                         item=[],
                                         site=[],
                                         group='',
                                         n='String instead of Int')
        with self.assertRaises(TypeError):
            self.ddmReq = DDMReqTemplate('copy',
                                         item=[],
                                         site=[],
                                         group='',
                                         n=1,
                                         cache=['List instead of String'])
        # Test unsupported keys:
        with self.assertRaises(KeyError):
            self.ddmReq = DDMReqTemplate('copy',
                                         unsupported='')

    def testStrip(self):
        expectedDdmReq = {'group': 'DataOps',
                           'item': ['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'],
                           'site': ['T2_*', 'T1_*_Disk']}
        self.myddmReq.strip()
        self.assertEqual(self.myddmReq, expectedDdmReq)

    def testIsEqual(self):
        ddmReq1 = DDMReqTemplate('copy', item=['DataSet1'],     site=['T2_*', 'T1_*_Disk'])
        ddmReq2 = DDMReqTemplate('copy', item=['DiffDataSet1'], site=['T2_*', 'T1_*_Disk'])
        ddmReq3 = DDMReqTemplate('copy', item=['DataSet1'],     site=['T2_CH_CERN'])

        # Test full (dictionary like) match - no key is excluded from the comparison:
        self.assertFalse(ddmReq1.isEqual(ddmReq2))
        self.assertFalse(ddmReq1.isEqual(ddmReq3))
        self.assertFalse(ddmReq2.isEqual(ddmReq3))

        # Test with 'item' key excluded from the comparison:
        self.assertTrue(ddmReq1.isEqual(ddmReq2, 'item'))
        self.assertFalse(ddmReq1.isEqual(ddmReq3, 'item'))
        self.assertFalse(ddmReq2.isEqual(ddmReq3, 'item'))

        # Test compare requests with equal keys, different APIS:
        ddmReq0 = DDMReqTemplate('pollcopy', item=['DataSet1'], site=['T2_*', 'T1_*_Disk'])
        self.assertFalse(ddmReq0.isEqual(ddmReq1))
        self.assertFalse(ddmReq0.isEqual(ddmReq1, 'item'))
        self.assertFalse(ddmReq0.isEqual(ddmReq2, 'item'))
        self.assertFalse(ddmReq0.isEqual(ddmReq3, 'item'))


class DDMTest(EmulatedUnitTestCase):
    """
    Unit tests for DDM Services module
    """

    def __init__(self, methodName='runTest'):
        super(DDMTest, self).__init__(methodName=methodName)

    def setUp(self):
        """
        Setup for unit tests
        """
        super(DDMTest, self).setUp()
        self.myDDM = DDM(enableDataPlacement=False)

    def testConfig(self):
        """
        Test service attributes and the override mechanism
        """
        self.assertEqual(self.myDDM['endpoint'], 'https://dynamo.mit.edu/')
        self.assertEqual(self.myDDM['cacheduration'], 1)
        self.assertEqual(self.myDDM['accept_type'], 'application/json')
        self.assertEqual(self.myDDM['content_type'], 'application/json')

        newParams = {"cacheduration": 100, "content_type": "application/text"}
        ddm = DDM(url='https://BLAH.cern.ch/',
                  configDict=newParams,
                  enableDataPlacement=False)
        self.assertEqual(ddm['endpoint'], 'https://BLAH.cern.ch/')
        self.assertEqual(ddm['cacheduration'], newParams['cacheduration'])
        self.assertEqual(ddm['content_type'], newParams['content_type'])

    def testMakeRequest(self):
        expectedResult = {
            'cache': None,
            'group': 'DataOps',
            'item': ['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'],
            'n': None,
            'site': ['T2_*', 'T1_*_Disk']
        }
        ddmReq =  DDMReqTemplate(
            'copy',
            item=['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'])

        result = self.myDDM.makeRequest(ddmReq)
        self.assertEqual(expectedResult, result)

    def testMakeAggRequest(self):
        ddmReqList = [None]*13
        ddmReqList[0] = DDMReqTemplate(
            'copy',
            item=['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'])
        ddmReqList[1] = DDMReqTemplate(
            'pollcopy',
            request_id=46458)
        ddmReqList[2] = DDMReqTemplate(
            'copy',
            item=['/RelValSingleMuPt10Extended/CMSSW_11_1_0_pre5-110X_mcRun4_realistic_v3_2026D48noPU-v1/MINIAODSIM'])
        ddmReqList[3] = DDMReqTemplate(
            'copy',
            item=['/RelValSingleMuPt10Extended/CMSSW_11_1_0_pre5-110X_mcRun4_realistic_v3_2026D48noPU-v1/MINIAODSIM'])
        ddmReqList[4] = DDMReqTemplate(
            'copy',
            item=['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'],
            site=['T2_CH_CERN', 'T2_US_MIT'])
        ddmReqList[5] = DDMReqTemplate(
            'pollcopy',
            request_id=46614)
        ddmReqList[6] = DDMReqTemplate(
            'copy',
            item=['/RelValSingleMuPt10Extended/CMSSW_11_1_0_pre5-110X_mcRun4_realistic_v3_2026D48noPU-v1/MINIAODSIM'])
        ddmReqList[7] = DDMReqTemplate(
            'copy',
            item=['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'],
            site =['T2_CH_CERN', 'T2_US_MIT'])
        ddmReqList[8] = DDMReqTemplate(
            'pollcopy',
            request_id=46627)
        ddmReqList[9] = DDMReqTemplate(
            'pollcopy',
            request_id=46628)
        ddmReqList[10] = DDMReqTemplate(
            'cancelcopy',
            request_id=46628)
        ddmReqList[11] = DDMReqTemplate(
            'copy',
            item=['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'],
            site=['T2_CH_CERN', 'T2_US_MIT'])
        ddmReqList[12] = DDMReqTemplate(
            'pollcopy',
            request_id=46633)

        expectedResult = [
            {'item': None,
             'request_id': 46633,
             'site': None,
             'status': None,
             'user': None},
            {'cache': None,
             'group': 'DataOps',
             'item': ['/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM',
                      '/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM',
                      '/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'],
             'n': None,
             'site': ['T2_CH_CERN', 'T2_US_MIT']},
            {'request_id': 46628},
            {'item': None,
             'request_id': 46628,
             'site': None,
             'status': None,
             'user': None},
            {'item': None,
             'request_id': 46627,
             'site': None,
             'status': None,
             'user': None},
            {'cache': None,
             'group': 'DataOps',
             'item': ['/RelValSingleMuPt10Extended/CMSSW_11_1_0_pre5-110X_mcRun4_realistic_v3_2026D48noPU-v1/MINIAODSIM',
                      '/RelValSingleMuPt10Extended/CMSSW_11_1_0_pre5-110X_mcRun4_realistic_v3_2026D48noPU-v1/MINIAODSIM',
                      '/RelValSingleMuPt10Extended/CMSSW_11_1_0_pre5-110X_mcRun4_realistic_v3_2026D48noPU-v1/MINIAODSIM',
                      '/LQLQToTopMuTopTau_M-1200_TuneCP5_13TeV_pythia8/RunIIFall17NanoAODv5-PU2017_12Apr2018_Nano1June2019_102X_mc2017_realistic_v7-v1/NANOAODSIM'],
             'n': None,
             'site': ['T2_*', 'T1_*_Disk']},
            {'item': None,
             'request_id': 46614,
             'site': None,
             'status': None,
             'user': None},
            {'item': None,
             'request_id': 46458,
             'site': None,
             'status': None,
             'user': None,
            }
        ]

        result = self.myDDM.makeAggRequests(ddmReqList, aggKey='item')
        self.assertEqual(expectedResult, result)


if __name__ == '__main__':
    unittest.main()
