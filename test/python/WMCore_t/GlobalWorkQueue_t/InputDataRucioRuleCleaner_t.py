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
from WMCore.ReqMgr.Web.ReqMgrService import getdata

import json
# system modules
import os
import time

import unittest

from urllib.parse import parse_qs, urlparse
#from unittest.mock import patch
from mock import mock

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
    

    @mock.patch('WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner.InputDataRucioRuleCleaner.getRequestForInputDataset')
    def testInputDataRucioRuleCleaner(self, mock_getRequestForInputDataset):
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
              
        
        # Define a variable to hold the dynamic RequestName and RequestStatus
        self.dynamicRequestName = specName
        self.dynamicRequestStatus = "running-open"  # Default value
        self.ReferenceInputDatasets = ["/JetHT/Run2012C-v1/RAW"]
        
        def mock_getRequestForInputDataset_side_effect(inputdataset, reqmgr2Url):
            if inputdataset in self.ReferenceInputDatasets: #only respond to the input data set that is used by a request
                # Simulate retrieving the workflow details
                return {
                    "result": [
                        {self.dynamicRequestName:{
                            "RequestName": self.dynamicRequestName,  # Use the dynamic RequestName
                            "RequestStatus": self.dynamicRequestStatus,  # Use the dynamic RequestStatus
                           }
                        }
                    ]
                }
            else:
                #return {"status": "error", "message": "Invalid request: inputdataset not found"}
                return {"result": []}
        
        # Assign the side effect to the mock object
        mock_getRequestForInputDataset.side_effect = mock_getRequestForInputDataset_side_effect

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
       
        #self.assertTrue(False)
        self.assertTrue(results)

    @mock.patch('WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner.InputDataRucioRuleCleaner.getRequestForInputDataset')
    def testInputDataRucioRuleCleanerTwoWorkflowSameInputdata(self, mock_getRequestForInputDataset):
        """
        Test the InputDataRucioRuleCleaner task with two workflows using the same input data set
        """
        #Get workflow description. ReRecoWorkloadFactory.getTestArguments() is used in createReRecoSpec below, 
        #so the workflow description here and the one used in creating workqueue is the same
        specName = "RerecoSpec"
        inputdataset = {"InputDataset": "/JetHT/Run2012C-v1/RAW"}
                
        #Create ReRecoSpec as stored in GlobalQueue       
        specUrl = self.specGenerator.createReRecoSpec(specName, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=inputdataset["InputDataset"])          
        
        #Second workflow using the same input data set
        specName1 = "RerecoSpec1"       
        specUrl1 = self.specGenerator.createReRecoSpec(specName1, "file",
                                                      assignKwargs={'SiteWhitelist':["T2_XX_SiteA"]},InputDataset=inputdataset["InputDataset"])

        #cleaner = InputDataRucioRuleCleaner(rest=self.mockRest, config=self.config_obj)
        cleaner = InputDataRucioRuleCleaner(rest=DummyREST(), config=self.config_obj)
        
        #Make GlobalQueue
        globalQ = globalQueue(DbName='workqueue_t',
                              QueueURL=self.testInit.couchUrl,
                              UnittestFlag=True, logger=cleaner.logger, **self.queueParams)
        globalQ.queueWork(specUrl, specName, "teamA")
        globalQ.queueWork(specUrl1, specName1, "teamB")
        

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
        
        print(f"Elements in GlobalQueue {specName}:")
        elements = data.get('rows', [])
        print(json.dumps(elements, indent=2))
        
        data1 = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName1], 'endkey': [specName1, {}],
                                  'reduce': False})
        
        print(f"Elements in GlobalQueue {specName1}:")
        elements1 = data1.get('rows', [])
        print(json.dumps(elements1, indent=2))

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
              
        
        # Define variables to hold the dynamic RequestName and RequestStatus
        self.dynamicRequestName = specName
        self.dynamicRequestName1 = specName1
        self.dynamicRequestStatus = "running-open"
        self.dynamicRequestStatus1 = "running-open"
        self.ReferenceInputDatasets = ["/JetHT/Run2012C-v1/RAW"]
        
        def mock_getRequestForInputDataset_side_effect(inputdataset, reqmgr2Url):
            if inputdataset in self.ReferenceInputDatasets: #only respond to the input data set that is used by a request
                # Simulate retrieving the workflow details
                return {
                    "result": [
                        {self.dynamicRequestName:{
                            "id": 123,
                            "RequestName": self.dynamicRequestName,  # Use the dynamic RequestName
                            "RequestStatus": self.dynamicRequestStatus,  # Use the dynamic RequestStatus
                           },
                        self.dynamicRequestName1:{
                            "id": 456,
                            "RequestName": self.dynamicRequestName1,  # Use the dynamic RequestName
                            "RequestStatus": self.dynamicRequestStatus1,  # Use the dynamic RequestStatus
                           }
                        }
                    ]
                }
            else:
                #return {"status": "error", "message": "Invalid request: inputdataset not found"}
                return {"result": []}
        
        # Assign the side effect to the mock object
        mock_getRequestForInputDataset.side_effect = mock_getRequestForInputDataset_side_effect
        
        #First test to clean the rule. It should not be successful since the second workflow is still running
        results = cleaner.cleanRucioRules(self.config_obj)
        print("Results from cleanRucioRules:", json.dumps(results, indent=2))
        self.assertTrue(not results)
        
        #Second test, change the second workflow element to 100% and try to clean the rule again
        #now change the percentage of workqueue of the second workflow to 100%
        #find the id that corresponds to blockname
        print("Block Name:", blockNames[0])
        element_id1 = [elements1[0]['id']]  # Get the first element's ID
        for e in elements1:
            inputs = list(e['value']['Inputs'].keys())
            if blockNames == inputs:
                print("Found matching element in second workflow:", e['id'])
                element_id1 = [e['id']]
                break

        print(f"Updating element {element_id1} to (PercentComplete=100, PercentSuccess=100, Status='Done')")
        wqService.updateElements(*element_id1, PercentComplete=100, PercentSuccess=100, Status='Done')
        # Re-fetch the elements to see the update
        data1 = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName1], 'endkey': [specName1, {}],
                                  'reduce': False})
        #element order changes each time, so we need to re-fetch the elements
        elements1 = data1.get('rows', [])
        print("Updated Elements in GlobalQueue (workflow 1):")
        for e in elements1:
            print(e["id"], e['value']['Status'], e['value']["PercentComplete"], e['value']["PercentSuccess"])

        #now try to clean the rule again. It should be successful this time
        results = cleaner.cleanRucioRules(self.config_obj)
        print("Results from cleanRucioRules with elements from other workflow complete:", json.dumps(results, indent=2))
        self.assertTrue(results)
        
        #Third test, change the second workflow to aborted and its elements to 0%. It should be able to clean the rule
        #now change back the percentage of workqueue of the second workflow to 0%
        print(f"Updating element {element_id1} to (PercentComplete=0, PercentSuccess=0, Status='Done')")
        wqService.updateElements(*element_id1, PercentComplete=0, PercentSuccess=0, Status='Available')
        #test the status of other request is aborted
        self.dynamicRequestStatus1 = "aborted"
        results = cleaner.cleanRucioRules(self.config_obj)
        print("Results from cleanRucioRules with other request is aborted:", json.dumps(results, indent=2))
        self.assertTrue(results) #should be true since other request already aborted

        #Fourth test, now testing workflow in staging status and no workqueue is created. It should not clean the rule
        globalQ.backend.deleteWQElementsByWorkflow([specName1])
        data1 = wqService.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                 {'startkey': [specName1], 'endkey': [specName1, {}],
                                  'reduce': False})
        
        print(f"Elements in GlobalQueue {specName1}:")
        elements1 = data1.get('rows', [])
        print(json.dumps(elements1, indent=2))
        self.dynamicRequestStatus1 = "staging"
        results = cleaner.cleanRucioRules(self.config_obj)
        print("Results from cleanRucioRules with other request is staging:", json.dumps(results, indent=2))
        self.assertTrue(not results) #this should be false since staging request using the same data should not trigger rule deletion


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
       
    '''
    #@mock.patch('WMCore.GlobalWorkQueue.CherryPyThreads.InputDataRucioRuleCleaner.InputDataRucioRuleCleaner.getRequestForInputDataset')
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
    '''
        
if __name__ == '__main__':
    unittest.main()