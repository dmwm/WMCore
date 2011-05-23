"""
WMCore/HTTPFrontEnd/WorkQueue/Services/WorkQueueMonitorService.py

REST interface to WorkQueue monitoring capabilities, output in
    DAS-compatible format.
Provides monitoring of WorkQueue elements, using DAO, DAO classes
    residing in WorkQueue/Database/

requirements:
https://twiki.cern.ch/twiki/bin/viewauth/CMS/WMCoreDiscussT1Rollout
Provide monitoring information for the request as it propagates down to the agents

installation:
https://twiki.cern.ch/twiki/bin/view/CMS/WorkQueueInstallation

writing unittests / testing details, hints:
https://twiki.cern.ch/twiki/bin/view/CMS/RESTModelUnitTest
WMCore/Services/WorkQueue/WorkQueue.py
WMCore_t/Webtools_t/ and WMCore_t/Services_t/WorkQueue_t/WorkQueue_t.py

TODO:
-more monitoring requirements (e.g. statistics, etc) - not clear now
"""






import os
import time
import logging # import WMCore.WMLogging

from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface
from WMCore.DAOFactory import DAOFactory
from WMCore.WorkQueue.Database import States

class WorkQueueMonitorService(ServiceInterface):
    _myClass = "short cut to the class name for logging purposes"
    
    def register(self):
        self._myClass = self.__class__.__name__ 
        
        

        ###############################################
        ## only in localqueue,  add these mehtods          ##
        ###############################################
        if self.model.config.level == "LocalQueue":
            #External couch call
            self.model._addMethod("GET", "jobsummary", self.getJobSummary)
            self.model._addMethod("GET", "jobstatebysite", self.getJobStateBySite)
            # batch system status
            bossAirDAOFactory = DAOFactory(package = "WMCore.BossAir",
                                       logger = self.model,
                                       dbinterface = self.model.dbi)

            self.model._addDAO('GET', 'batchjobstatus', "JobStatusForMonitoring",
                        daoFactory = bossAirDAOFactory)

            self.model._addDAO('GET', 'batchjobstatusbysite',
                               "JobStatusByLocation",
                               daoFactory = bossAirDAOFactory)

            wmbsDAOFactory = DAOFactory(package = "WMCore.WMBS",
                                       logger = self.model,
                                       dbinterface = self.model.dbi)
            self.model._addDAO('GET', 'listsites', "Locations.List",
                               daoFactory = wmbsDAOFactory)
        # DAO stuff
        # RESTModel._addDAO() see COMP/T0/src/python/T0/DAS/Tier0RESTModel.py
        # (within WMCore no addDAO() example except for WebTools_t/DummyRESTModel.py ...)
        
        self.model.daofactory = DAOFactory(package = "WMCore.WorkQueue.Database",
                                           logger = self.model,
                                           dbinterface = self.model.dbi)
        
        #############################
        ##  Element related view   ##
        #############################
        self.model._addDAO("GET", "elementsinfo", "Monitor.Elements.ElementsInfo")
        self.model._addDAO("GET", "elementsbyworkflow", "Monitor.Elements.ElementsInfoByWorkflow",
                          args = ["workflow"])
        self.model._addDAO("GET", "elementsinfowithlimit", "Monitor.Elements.ElementsInfoWithLimit",
                          args = ["startIndex", "results"], validation = [self.validateInt])
        
        self.model._addDAO("POST", "elementsbystate", "Monitor.Elements.ElementsByState",
                           args = ["status"], validation = [self.validateState])
        self.model._addDAO("POST", "elementsbyid", "Monitor.Elements.ElementsById",
                           args = ["id"], validation = [self.validateId])
        
        #############################
        ##  Workload related view  ##
        #############################
        # overview of wm workload status (that is wmspec - getting data from wq_wmspec table)
        self.model._addDAO("GET", "workloads", "Monitor.Workloads.Workloads")
        self.model._addDAO("GET", "workloadprogress", "Monitor.Workloads.WorkloadsWithProgress")
        
        
        self.model._addDAO("POST", "workloadsbyid", "Monitor.Workloads.WorkloadsById",
                          args = ["id"], validation = [self.validateId])
        self.model._addDAO("POST", "workloadsbyname", "Monitor.Workloads.WorkloadsByName",
                          args = ["name"])
        self.model._addDAO("POST", "workloadsbyowner", "Monitor.Workloads.WorkloadsByOwner",
                          args = ["owner"])
        
        
        #############################
        ##  Summary  related view  ##
        #############################
        self.model._addDAO("GET", "statusstat", "Monitor.Summary.StatusStatistics")
        
        # workloadID can be either workload id number or wildcard (*) 
        self.model._addDAO("GET", "statusstatbyworkload", "Monitor.Summary.StatusStatByWorkload",
                          args = ['workloadID'])
        self.model._addDAO("GET", "jobstatusstat", "Monitor.Summary.JobStatusStat")
        self.model._addDAO("GET", "childqueues", "Monitor.Summary.GetChildQueues")
        self.model._addDAO("GET", "childqueuesbyrequest", "Monitor.Summary.GetAssignedLocalQueueByRequest")
        self.model._addDAO("GET", "jobstatusbyrequest", "Monitor.Summary.JobStatusByRequest")
        self.model._addDAO("GET", "jobsbyrequest", "Monitor.Summary.TopLevelJobsByRequest")
        
        ###############################################
        ## To do:  Revisit this usage  related view  ##
        ###############################################
        
        self.model._addDAO("GET", "taskprogress", "Monitor.TasksWithProgress")
        self.model._addDAO("GET", "sites", "Monitor.Sites")
        self.model._addDAO("GET", "data", "Monitor.Data")
        self.model._addDAO("GET", "datasitemap", "Monitor.DataSiteMap")
        
        
        logging.info("%s initialised." % self._myClass)        
        
    def getJobSummary(self):
        from WMCore.HTTPFrontEnd.WMBS.External.CouchDBSource import JobInfo
        return JobInfo.getJobSummaryByWorkflow(self.model.config.couchConfig)

    def getJobStateBySite(self):
        from WMCore.HTTPFrontEnd.WMBS.External.CouchDBSource import JobInfo
        return JobInfo.getJobStateBySite(self.model.config.couchConfig)

    def validateInt(self, input):
        """
        validate status function and do the type conversion if the argument 
        requires non string 
        """
        input["startIndex"] = int(input["startIndex"])
        input["results"] = int(input["results"])
        return input
    
    def validateState(self, inpt):
        """Validate inpt argument state - only element states as defined in
           States (WMCore.WorkQueue.Database) are accepted (i.e. only states
           designated by respective string names, not by integer indices).
        """
        state = inpt["status"]
        try:
            try:
                int(state)
            except:
                pass
            else:
                raise ValueError # is integer - fail
            States[state]
        except (ValueError, KeyError, TypeError):
            m = "Incorrect input - unknown WorkQueue element state '%s'" % state
            raise AssertionError, m
        else:
            return inpt
        
        
    
    def validateId(self, inpt):
        """Validate inpt argument id - only positive integers allowed."""
        id = inpt["id"]
        try:
            if int(id) < 0:
                raise ValueError
        except (ValueError, TypeError):
            m = "Incorrect input - id must be positive integer ('%s')" % id            
            raise AssertionError, m
        else:
            return inpt
                
        

    # -----------------------------------------------------------------------
    # dummy tests, database connection testing / experiments stuff
    # to be removed later (2010-02-05)  
    
    
    def _testDbReadiness(self):
        """town table used by a WebTools tutorial example"""
        logging.debug("%s doing database readiness test." % self._myClass)
        try:
            sql = "create table towns (name varchar(20), country varchar(20))"
            self.model.dbi.processData(sql)
        except Exception, ex:
            logging.error("database error, reason: '%s'" % ex)
            
        try:
            sql = "insert into towns (name, country) values ('SomeTown', 'SomeCountry')"
            self.model.dbi.processData(sql)
        except Exception, ex:
            logging.error("database error, reason: '%s'" % ex)
            
        logging.debug("%s database readiness test finished." % self._myClass)



    def testMethod(self):
        """testMethot - returns simple data - current time"""
        format = "%d %b %Y %H:%M:%S %Z"
        r = "date/time: %s" % time.strftime(format, time.localtime())
        return r

