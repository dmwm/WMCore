from WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner import InputDataRucioRuleCleaner

from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

import cherrypy

# WMCore modules
from WMCore.Services.Rucio import Rucio

from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.Emulators.WMSpecGenerator.WMSpecGenerator import WMSpecGenerator
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner

import json
# system modules
import os
import time

import unittest


class DummyREST:
    def __init__(self):
        self.logger = None  # Optional: add logger if needed
        self.config = None

#MSRuleCleaner requires plain dictionary to be passed as config while CherryPyPeriodic requires attributes, so we create a DictWithAttrs class
class DictWithAttrs(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'") from e

class InputDataRucioRuleCleanerTest(EmulatedUnitTestCase):

    def setUp(self):   
        self.config = {}
        self.msRuleCleaner = {"verbose": True,
                         "interval": 1 * 60,
                         "services": ['ruleCleaner'],
                         "rucioAccount": 'wma_test',
                         'reqmgr2Url': 'https://cmsweb-testbed.cern.ch/reqmgr2',
                         'msOutputUrl': 'https://cmsweb-testbed.cern.ch/ms-output',
                         'reqmgrCacheUrl': 'https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache',
                         'phedexUrl': 'https://cmsweb-testbed.cern.ch/phedex/datasvc/json/prod',
                         'dbsUrl': 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader',
                         'rucioUrl': 'http://cms-rucio-int.cern.ch',
                         'rucioAuthUrl': 'https://cms-rucio-auth-int.cern.ch',
                         "wmstatsUrl": "https://cmsweb-testbed.cern.ch/wmstatsserver",
                         "logDBUrl": "https://cmsweb-testbed.cern.ch/couchdb/wmstats_logdb",
                         'logDBReporter': 'reqmgr2ms_ruleCleaner',
                         'archiveDelayHours': 8,
                         'archiveAlarmHours': 24,
                         'enableRealMode': True}
        
        self.creds = {"client_cert": os.getenv("X509_USER_CERT", "Unknown"),
                      "client_key": os.getenv("X509_USER_KEY", "Unknown")}
        self.rucioConfigDict = {"rucio_host": self.msRuleCleaner['rucioUrl'],
                                "auth_host": self.msRuleCleaner['rucioAuthUrl'],
                                "auth_type": "x509",
                                "account": self.msRuleCleaner['rucioAccount'],
                                "ca_cert": False,
                                "timeout": 30,
                                "request_retries": 3,
                                "creds": self.creds}

                
        self.specGenerator = WMSpecGenerator("WMSpecs")
        self.schema = []
        self.couchApps = ["WorkQueue"]
        self.testInit = TestInitCouchApp('WorkQueueServiceTest')
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules=self.schema,
                                useDefault=False)
        self.testInit.setupCouch('workqueue_t', *self.couchApps)
        self.testInit.setupCouch('workqueue_t_inbox', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t', *self.couchApps)
        self.testInit.setupCouch('local_workqueue_t_inbox', *self.couchApps)
        self.testInit.generateWorkDir()

        self.msRuleCleaner.update({'QueueURL':self.testInit.couchUrl})
           
        self.queueParams = {}
        self.queueParams['log_reporter'] = "Services_WorkQueue_Unittest"
        self.queueParams['rucioAccount'] = self.msRuleCleaner['rucioAccount']
        self.queueParams['rucioAuthUrl'] = "http://cms-rucio-int.cern.ch"
        self.queueParams['rucioUrl'] = "https://cms-rucio-auth-int.cern.ch"
        self.queueParams['_internal_name'] = 'GlobalWorkQueueTest'
        self.queueParams['log_file'] = 'test.log'

                
        print("X509_USER_CERT:", os.getenv("X509_USER_CERT"))
        print("X509_USER_KEY:", os.getenv("X509_USER_KEY"))
      
        # Create config object with attributes
        self.config_obj = DictWithAttrs(self.config)
        #additional attributes needed by cherrypy periodic task
        self.config_obj._internal_name = "GlobalWorkQueueTest"
        self.config_obj.log_file = "test.log"
        #additional attributes needed by global workqueue
        self.config_obj.queueParams = self.queueParams
        #additional attributes needed by MSRuleCleaner
        self.config_obj.msRuleCleaner = self.msRuleCleaner
        #duration for the periodic task in seconds
        self.config_obj.cleanInputDataRucioRuleDuration = 10

        super(InputDataRucioRuleCleanerTest, self).setUp()
    
    def testInputDataRucioRuleCleaner(self):
        """
        Test the InputDataRucioRuleCleaner task
        """
        #Get workflow description. ReRecoWorkloadFactory.getTestArguments() is used in createReRecoSpec below, 
        #so the workflow description here and the one used in creating workqueue is the same
        specName = "RerecoSpec"
        inputdataset = {"InputDataset": "/JetHT/Run2012C-v1/RAW"}
                
        #Create ReRecoSpec as stored in GlobalQueue       
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=inputdataset["InputDataset"])          
        
        #cleaner = InputDataRucioRuleCleaner(rest=self.mockRest, config=self.config_obj)
        cleaner = InputDataRucioRuleCleaner(rest=DummyREST(), config=self.config_obj)
        
        #Make GlobalQueue
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, logger=cleaner.logger, **self.queueParams)
        globalQ.queueWork(specUrl, specName, "teamA")
        cleaner.globalQ = globalQ

        #Make MSRuleCleaner
        msRuleCleaner = MSRuleCleaner(self.config_obj.msRuleCleaner,logger=cleaner.logger)
        msRuleCleaner.resetCounters()
        msRuleCleaner.rucio = Rucio.Rucio(self.msRuleCleaner['rucioAccount'],
                                               hostUrl=self.rucioConfigDict['rucio_host'],
                                               authUrl=self.rucioConfigDict['auth_host'],
                                               configDict=self.rucioConfigDict)
        
        cleaner.msRuleCleaner = msRuleCleaner        
        
        #Let try to modify the element in GlobalQueue to have PercentComplete and PercentSuccess set to 100
        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #Use this instead of wqService.getWQElementsByWorkflow(workflowName) to have the element'id'
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        
        print("Elements in GlobalQueue:")
        elements = data.get('rows', [])
        print(json.dumps(elements, indent=2))
        
        #let update the PercentComplete and PercentSuccess and Status='Done' of the first elements
        element_id = [elements[0]['id']]  # Get the first element's ID
        print("Updating element:", element_id)
        wqService.updateElements(*element_id, PercentComplete=100, PercentSuccess=100, Status='Done')
        
        #create a rule and inject it in wma_test account
        blockNames = list(elements[0]['value']['Inputs'].keys())  # Get the block name from the first element
        print("Block Name:", blockNames[0])
        
        #need to create rule here otherwise we do not know which element was updated since the element order changes each time re-fetching (of course we can use the element_id)
        rule_id = cleaner.msRuleCleaner.rucio.createReplicationRule(
            names=blockNames[0],
            rseExpression="T2_US_Nebraska",
            copies=1,
            grouping="DATASET",
            lifetime=360,
            account="wma_test",
            ask_approval=False,
            activity="Production Input",
            comment="WMCore test block rule creation"
        )

        print("Created Rucio rule with ID:", rule_id)
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        print(rule_info)

        # Re-fetch the elements to see the update
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        #element order changes each time, so we need to re-fetch the elements
        elements = data.get('rows', [])
        #elements=wqService.getWQElementsByWorkflow(specName)
        print("Updated Elements in GlobalQueue:")
        for e in elements:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
            #print(e["id"], e['Status'], e["PercentComplete"], e["PercentSuccess"])
              
        
        results = cleaner.cleanRucioRules(self.config_obj)
        print("Results from cleanRucioRules:", json.dumps(results, indent=2))
        #now make sure the rule is cleaned
        #keep deleting until success or timeout
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        delResult = False
        timeleft = 0
        start_time = time.time()
        while rule_info and not delResult and timeleft < 300:
            #now delete it
            print('Manually deleting rucio rules: ', blockNames[0], cleaner.msRuleCleaner.rucio.listDataRules(blockNames[0], account=self.msRuleCleaner['rucioAccount']))
            delResult = cleaner.msRuleCleaner.rucio.deleteRule(rule_id[0])
            print("Deleted Rucio rule with ID:", rule_id, delResult)
            if delResult: break
            time.sleep(60)
            timeleft = time.time() - start_time
        
        if not delResult and timeleft >= 300:
            print("Failed to delete the rule after 5 minutes, exiting...")
       
        self.assertTrue(results['CleanupStatus']['Current'])
       

    def testInputDataRucioRuleCleanerWithThreading(self):
        """
        Test the InputDataRucioRuleCleaner task with threading
        """
        
        #cleaner = InputDataRucioRuleCleaner(rest=self.mockRest, config=self.config_obj)
        cleaner = InputDataRucioRuleCleaner(rest=DummyREST(), config=self.config_obj)
        
        #Get workflow description. ReRecoWorkloadFactory.getTestArguments() is used in createReRecoSpec below, 
        #so the workflow description here and the one used in creating workqueue is the same
        specName = "RerecoSpec"
        inputdataset = {"InputDataset": "/JetHT/Run2012C-v1/RAW"}
                
        #Create ReRecoSpec as stored in GlobalQueue       
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=inputdataset["InputDataset"])

        #Make GlobalQueue
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, logger=cleaner.logger, **self.queueParams)
        globalQ.queueWork(specUrl, specName, "teamA")
        cleaner.globalQ = globalQ

        #Make MSRuleCleaner
        msRuleCleaner = MSRuleCleaner(self.config_obj.msRuleCleaner,logger=cleaner.logger)
        msRuleCleaner.resetCounters()
        msRuleCleaner.rucio = Rucio.Rucio(self.msRuleCleaner['rucioAccount'],
                                               hostUrl=self.rucioConfigDict['rucio_host'],
                                               authUrl=self.rucioConfigDict['auth_host'],
                                               configDict=self.rucioConfigDict)
        cleaner.msRuleCleaner = msRuleCleaner        
        
        # Start CherryPy engine
        print('CherryPy engine starting...')
        cherrypy.engine.start()
        # Give CherryPy a moment to start and modify the element in GlobalQueue after 5 seconds and before the next run of the periodic task
        time.sleep(5)
        
        #Let try to modify the element in GlobalQueue to have PercentComplete and PercentSuccess set to 100
        wqService = WorkQueueDS(self.testInit.couchUrl, 'workqueue_t')
        #Use this instead of wqService.getWQElementsByWorkflow(workflowName) to have the element'id'
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})
        
        print("Elements in GlobalQueue:")
        elements = data.get('rows', [])
        print(json.dumps(elements, indent=2))
        
        #let update the PercentComplete and PercentSuccess and Status='Done' of the first elements
        element_id = [elements[0]['id']]  # Get the first element's ID
        print("Updating element:", element_id)
        wqService.updateElements(*element_id, PercentComplete=100, PercentSuccess=100, Status='Done')
        
        #create a rule and inject it in wma_test account
        blockNames = list(elements[0]['value']['Inputs'].keys())  # Get the block name from the first element
                
        #need to create rule here otherwise we do not know which element was updated since the element order changes each time re-fetching (of course we can use the element_id)
        rule_id = cleaner.msRuleCleaner.rucio.createReplicationRule(
            names=blockNames[0],
            rseExpression="T2_US_Nebraska",
            copies=1,
            grouping="DATASET",
            lifetime=360,
            account="wma_test",
            ask_approval=False,
            activity="Production Input",
            comment="WMCore test block rule creation"
        )

        print("Created Rucio rule with ID:", rule_id)
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        print(rule_info)

        # Re-fetch the elements to see the update
        data = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName], 'endkey': [specName, {}],
                                  'reduce': False})

        elements = data.get('rows', [])
        print("Updated Elements in GlobalQueue:")
        for e in elements:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])
    
        time.sleep(20)
        
        print('CherryPy engine exiting...')
        cherrypy.engine.exit()

        #now continuously check the rule status until it is cleaned and exit after 10 minutes
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        timeleft = 0
        start_time = time.time()
        while rule_info and timeleft < 600:  # Check for 10 minutes
            print("Rule still exists:", rule_id[0], rule_info)
            time.sleep(60)
            timeleft = time.time() - start_time
            rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        
        rule_info_for_check = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        
        #now make sure the rule should be cleaned (note that the rule may not be cleaned immediately after the periodic task execution (~5 mins), but we just clean it again here)
        rule_info = cleaner.msRuleCleaner.rucio.getRule(rule_id[0])
        delResult = False
        if not rule_info:
            print("Rule not found.")
        
        #keep deleting until success or timeout
        timeleft = 0
        start_time = time.time()
        while rule_info and not delResult and timeleft < 300:
            #now delete it
            print('Manually deleting rucio rules: ', blockNames[0], cleaner.msRuleCleaner.rucio.listDataRules(blockNames[0], account=self.msRuleCleaner['rucioAccount']))
            delResult = cleaner.msRuleCleaner.rucio.deleteRule(rule_id[0])
            print("Deleted Rucio rule with ID:", rule_id, delResult)
            if delResult: break
            time.sleep(60)
            timeleft = time.time() - start_time
        
        if not delResult and timeleft >= 300:
            print("Failed to delete the rule after 5 minutes, exiting...")
        
        self.assertTrue(not rule_info_for_check, "Rule not deleted successfully after periodic task execution.")
        
if __name__ == '__main__':
    unittest.main()