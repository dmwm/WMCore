from __future__ import (division, print_function)
import unittest
import time
import json
import os
from WMCore.WMBase import getTestBase
from WMCore.Services.WMArchive.DataMap import createArchiverDoc

SAMPLE_FWJR = {'fallbackFiles': [],
 'skippedFiles': [],
 'steps': {'cmsRun1': {'analysis': {},
                       'cleanup': {},
                       'errors': [],
                       'input': {'source': [{'catalog': '',
                                             'events': 6893,
                                             'guid': 'E8099605-8853-E011-A848-0030487A18F2',
                                             'input_source_class': 'PoolSource',
                                             'input_type': 'primaryFiles',
                                             'lfn': '/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root',
                                             'module_label': 'source',
                                             'pfn': 'root://eoscms.cern.ch//eos/cms/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root',
                                             'runs': {'160960': [164,
                                                                 165]}}]},
                       'logs': {},
                       'output': {'ALCARECOStreamDtCalib': [{'InputPFN': '/pool/condor/dir_2661042/glide_qqd8kp/execute/dir_5444/job/WMTaskSpace/cmsRun1/ALCARECOStreamDtCalib.root',
                                                             'OutputPFN': 'root://eoscms.cern.ch//eos/cms/store/unmerged/CMSSW_7_0_0_pre11/Cosmics/ALCARECO/DtCalib-RECOCOSD_TaskChain_Data_pile_up_test-v1/00000/ECCFE421-08CB-E511-9F4C-02163E017804.root',
                                                             'StageOutCommand': 'rfcp-CERN',
                                                             'acquisitionEra': 'CMSSW_7_0_0_pre11',
                                                             'async_dest': None,
                                                             'branch_hash': 'c1e135af4ac2eb2b803bb6487be2c80f',
                                                             'catalog': '',
                                                             'checksums': {'adler32': 'e503b8b9',
                                                                           'cksum': '2641269665'},
                                                             'configURL': 'https://cmsweb.cern.ch/couchdb;;reqmgr_config_cache;;5f4811e9ccd63d563cd62572350f0db8',
                                                             'dataset': {'applicationName': 'cmsRun',
                                                                         'applicationVersion': 'CMSSW_7_0_0_pre11',
                                                                         'dataTier': 'ALCARECO',
                                                                         'primaryDataset': 'Cosmics',
                                                                         'processedDataset': 'CMSSW_7_0_0_pre11-DtCalib-RECOCOSD_TaskChain_Data_pile_up_test-v1'},
                                                             'events': 0,
                                                             'globalTag': 'GR_R_62_V3::All',
                                                             'guid': 'ECCFE421-08CB-E511-9F4C-02163E017804',
                                                             'input': ['/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root'],
                                                             'inputPath': '/Cosmics/Run2011A-v1/RAW',
                                                             'inputpfns': ['root://eoscms.cern.ch//eos/cms/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root'],
                                                             'lfn': '/store/unmerged/CMSSW_7_0_0_pre11/Cosmics/ALCARECO/DtCalib-RECOCOSD_TaskChain_Data_pile_up_test-v1/00000/ECCFE421-08CB-E511-9F4C-02163E017804.root',
                                                             'location': None,
                                                             'merged': False,
                                                             'module_label': 'ALCARECOStreamDtCalib',
                                                             'ouput_module_class': 'PoolOutputModule',
                                                             'pfn': '/pool/condor/dir_2661042/glide_qqd8kp/execute/dir_5444/job/WMTaskSpace/cmsRun1/ALCARECOStreamDtCalib.root',
                                                             'prep_id': 'None',
                                                             'processingStr': 'RECOCOSD_TaskChain_Data_pile_up_test',
                                                             'processingVer': 1,
                                                             'runs': {'160960': {'164': 100, '165': 150}},
                                                             'size': 647376,
                                                             'user_dn': None,
                                                             'user_vogroup': 'DEFAULT',
                                                             'user_vorole': 'DEFAULT',
                                                             'validStatus': 'PRODUCTION'}],
                                  'ALCARECOStreamMuAlCalIsolatedMu': [{'InputPFN': '/pool/condor/dir_2661042/glide_qqd8kp/execute/dir_5444/job/WMTaskSpace/cmsRun1/ALCARECOStreamMuAlCalIsolatedMu.root',
                                                                       'OutputPFN': 'root://eoscms.cern.ch//eos/cms/store/unmerged/CMSSW_7_0_0_pre11/Cosmics/ALCARECO/MuAlCalIsolatedMu-RECOCOSD_TaskChain_Data_pile_up_test-v1/00000/9665EB21-08CB-E511-9F4C-02163E017804.root',
                                                                       'StageOutCommand': 'rfcp-CERN',
                                                                       'acquisitionEra': 'CMSSW_7_0_0_pre11',
                                                                       'async_dest': None,
                                                                       'branch_hash': '1569b89a7f6b4a5a6cbeae5b8fccea94',
                                                                       'catalog': '',
                                                                       'checksums': {'adler32': '3379c136',
                                                                                     'cksum': '1828182610'},
                                                                       'configURL': 'https://cmsweb.cern.ch/couchdb;;reqmgr_config_cache;;5f4811e9ccd63d563cd62572350f0db8',
                                                                       'dataset': {'applicationName': 'cmsRun',
                                                                                   'applicationVersion': 'CMSSW_7_0_0_pre11',
                                                                                   'dataTier': 'ALCARECO',
                                                                                   'primaryDataset': 'Cosmics',
                                                                                   'processedDataset': 'CMSSW_7_0_0_pre11-MuAlCalIsolatedMu-RECOCOSD_TaskChain_Data_pile_up_test-v1'},
                                                                       'events': 0,
                                                                       'globalTag': 'GR_R_62_V3::All',
                                                                       'guid': '9665EB21-08CB-E511-9F4C-02163E017804',
                                                                       'input': ['/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root'],
                                                                       'inputPath': '/Cosmics/Run2011A-v1/RAW',
                                                                       'inputpfns': ['root://eoscms.cern.ch//eos/cms/store/data/Run2011A/Cosmics/RAW/v1/000/160/960/E8099605-8853-E011-A848-0030487A18F2.root'],
                                                                       'lfn': '/store/unmerged/CMSSW_7_0_0_pre11/Cosmics/ALCARECO/MuAlCalIsolatedMu-RECOCOSD_TaskChain_Data_pile_up_test-v1/00000/9665EB21-08CB-E511-9F4C-02163E017804.root',
                                                                       'location': None,
                                                                       'merged': False,
                                                                       'module_label': 'ALCARECOStreamMuAlCalIsolatedMu',
                                                                       'ouput_module_class': 'PoolOutputModule',
                                                                       'pfn': '/pool/condor/dir_2661042/glide_qqd8kp/execute/dir_5444/job/WMTaskSpace/cmsRun1/ALCARECOStreamMuAlCalIsolatedMu.root',
                                                                       'prep_id': 'None',
                                                                       'processingStr': 'RECOCOSD_TaskChain_Data_pile_up_test',
                                                                       'processingVer': 1,
                                                                       'runs': {'160960': [164,
                                                                                           165]},
                                                                       'size': 665701,
                                                                       'user_dn': None,
                                                                       'user_vogroup': 'DEFAULT',
                                                                       'user_vorole': 'DEFAULT',
                                                                       'validStatus': 'PRODUCTION'}],
                                  'analysis': []}
                       },
                'logArch1': {'analysis': {},
                        'cleanup': {},
                        'errors': [],
                        'input': {},
                        'logs': {},
                        'output': {'analysis': [],
                                   'logArchive': [{'checksums': {'adler32': '6588e920',
                                                                 'cksum': '2315739066'},
                                                   'events': 0,
                                                   'lfn': '/store/unmerged/logs/prod/2016/2/4/sryu_TaskChain_Data_wq_testt_160204_061048_5587/RECOCOSD/0000/0/7d7d41dc-cb02-11e5-833c-02163e00efd5-88-0-logArchive.tar.gz',
                                                   'location': None,
                                                   'merged': False,
                                                   'module_label': 'logArchive',
                                                   'pfn': 'root://eoscms.cern.ch//eos/cms/store/unmerged/logs/prod/2016/2/4/sryu_TaskChain_Data_wq_testt_160204_061048_5587/RECOCOSD/0000/0/7d7d41dc-cb02-11e5-833c-02163e00efd5-88-0-logArchive.tar.gz',
                                                   'runs': {},
                                                   'size': 0}]},
                        'parameters': {},
                        'performance': {'storage': {'readAveragekB': 77.8474891246,
                                                    'readCachePercentageOps': 0.0,
                                                    'readMBSec': 0.0438598972596,
                                                    'readMaxMSec': 4832.84,
                                                    'readNumOps': 97620.0,
                                                    'readPercentageOps': 1.00032780168,
                                                    'readTotalMB': 7423.792,
                                                    'readTotalSecs': 0,
                                                    'writeTotalMB': 357.624,
                                                    'writeTotalSecs': 575158.0},
                                        "multicore": {},
                                        "memory": {
                                                   "PeakValueRss": 0,
                                                   "PeakValueVsize": 0
                                                   },
                                        "cpu": {
                                                "TotalJobCPU": 0.39894,
                                                "AvgEventCPU": "-nan", #convert to -2.0
                                                "MaxEventCPU": 0,
                                                "AvgEventTime": "inf", #convert to -1.0
                                                "MinEventCPU": 0,
                                                "TotalEventCPU": 0,
                                                "TotalJobTime": 26.4577,
                                                "MinEventTime": 0.0,
                                                "MaxEventTime": 0.0,
                                                'EventThroughput': '0.0952297',
                                                'TotalLoopCPU': '1962.28'
                                                }},
                        'site': 'T2_CH_CERN',
                        'start': 1454569735,
                        'status': 0,
                        'stop': 1454569736},
           'stageOut1': {'analysis': {},
                         'cleanup': {},
                         'errors': [],
                         'input': {},
                         'logs': {},
                         'output': {'analysis': []},
                         'parameters': {},
                         'performance': {'cpu': {},
                                         'memory': {},
                                         'multicore': {},
                                         'storage': {}},
                         'site': 'T2_CH_CERN',
                         'start': 1454569727,
                         'status': 0,
                         'stop': 1454569735}},
 'task': '/sryu_TaskChain_Data_wq_testt_160204_061048_5587/RECOCOSD'}

class DataMap_t(unittest.TestCase):

    def testConvertToArchiverFormat(self):

        job = {}
        job["id"] = "1-0"
        job['doc'] = {"fwjr": SAMPLE_FWJR, "jobtype": "Processing",
                      "jobstate": "success", "timestamp": int(time.time())}
        newData = createArchiverDoc(job)
        from pprint import pprint
        pprint(newData)

        #outputModules = set([a['outputModule'] for a in newData['steps']['cmsRun1']['output']])
        #outModules = set(SAMPLE_FWJR['steps']['cmsRun1']['output'].keys())
        #self.assertEqual(outputModules - outModules, set())

        run = SAMPLE_FWJR['steps']['cmsRun1']['output']['ALCARECOStreamMuAlCalIsolatedMu'][0]['runs']
        for step in newData['steps']:
            if step['name'] == 'cmsRun1':
                runInfo = step['output'][0]['runs'][0]
        # we no longer ship the lumis and eventsPerLumi lists to WMArchive. Hard-wired to []
        self.assertEqual(runInfo['lumis'], [])
        self.assertEqual(runInfo['eventsPerLumi'], [])
        fwjrSamples = ["ErrorCodeFail.json",
                       "FailedByAgent.json",
                       "HarvestSuccessFwjr.json",
                       "LogCollectFailedFwjr.json", "LogCollectSuccessFwjr.json",
                       "MergeFailedFwjr.json", "MergeSuccessFwjr.json",
                       "NoJobReportFail.json",
                       "ProcessingFailedFwjr.json", "ProcessingPerformanceFailed.json", "ProcessingSuccessFwjr.json",
                       "ProductionFailedFwjr.json", "ProductionSuccessFwjr.json",
                       "SkimSuccessFwjr.json"]
        for sample in fwjrSamples:
            sPath = os.path.join(getTestBase(),
                          "WMCore_t/Services_t/WMArchive_t/FWJRSamples/%s" % sample)
            with open(sPath, 'r') as infile:
                fwjr = json.load(infile)
            job = {}
            job["id"] = fwjr["_id"]
            job['doc'] = {"fwjr": fwjr["fwjr"], "jobtype": fwjr["jobtype"],
                      "jobstate": fwjr['jobstate'], "timestamp": fwjr["timestamp"]}
            newData =createArchiverDoc(job)
            print("\n\n==========\n%s" % sPath)
            pprint(newData)

if __name__ == '__main__':
    unittest.main()
