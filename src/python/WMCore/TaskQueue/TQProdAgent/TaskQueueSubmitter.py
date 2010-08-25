#!/usr/bin/env python
"""
_TaskQueueSubmitter_

TaskQueue Submitter implementation. It gets jobs and enqueues them into the TQ.

"""

__revision__ = "$Id: TaskQueueSubmitter.py,v 1.2 2009/09/29 12:23:04 delgadop Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import os

from MessageService.MessageService import MessageService

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.Configuration import loadProdAgentConfiguration


from TQComp.Apis.TQSubmitApi import TQSubmitApi
from TQComp.Apis.TQApiData import Task

from WMCore import Configuration as WMCoreConfig

class TaskQueueSubmitter(BulkSubmitterInterface):
    """
    _TaskQueueSubmitter_

    """
    def __init__(self):
        """
        Overload constructor to add TaskQueue API initialization.
        """
        BulkSubmitterInterface.__init__(self)

        logging.debug("<<<<< Creating TaskQueueSubmitter>>>>>")

        # Try to get the TaskQueue conf file from ProdAgentConfig.xml
        what = "TaskQueue"
        try:
            cfg = loadProdAgentConfiguration()
            self.tqConfig = cfg.getConfig(what)
        except StandardError, ex:
            msg = "%s.Config:" % what
            msg += "Unable to load ProdAgent Config for " + what
            msg += "%s\n" % ex
            logging.warning(msg)

        # Now load the TaskQueue API
        confFile = self.tqConfig['TaskQueueConfFile']
        myconfig = WMCoreConfig.loadConfigurationFile(confFile)
        self.tqApi = TQSubmitApi(logging, myconfig, None)
        logging.debug("<<<<< TaskQueueSubmitter created! >>>>>")
        logging.debug("<<<<< Creating TaskQueueSubmitter >>>>>")

 
    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """

        logging.debug("<<<<< TaskQueueSubmitter >>>>>")

        # Get basic parameters
        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.jobType = self.primarySpecInstance.parameters['JobType']
        if not self.primarySpecInstance.parameters.has_key('BulkInputSandbox'):
            msg="There is no BulkInputSandbox defined in the JobSpec. Submission cant go on..."
            logging.error(msg)
            return
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']
        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.specSandboxName = None
        self.singleSpecName = None
        self.reqs = ""


        ####  REQUIREMENTS  ####

        # CMSSW arch requirements
        swarch = None
        creatorPluginConfig = loadPluginConfig("JobCreator", "Creator")
        if creatorPluginConfig['SoftwareSetup'].has_key('ScramArch'):
            if creatorPluginConfig['SoftwareSetup']['ScramArch'].find("slc4")>=0:
                swarch=creatorPluginConfig['SoftwareSetup']['ScramArch']

        if swarch:
            if self.reqs: self.reqs += " and "
            self.reqs += "(swarch == '%s')" % swarch


        # Software version requirements
        if not (self.jobType in ("CleanUp", "LogCollect")):
            if len(self.applicationVersions)>0:
                if self.reqs: self.reqs += " and "
                self.reqs += reduce(lambda x,y: "%s and %s" % (x,y), \
                                    map(lambda x: "('%s' in cmssw)" % x, \
                                        self.applicationVersions))
            else:
                raise ProdAgentException("No CMSSW version found!")


        # Whitelist requirements
        self.req_se = None
        if len(self.whitelist)>0:
            # First remove duplicates from the list and sort
            uniqWhitelist = dict(zip(self.whitelist, self.whitelist)).keys()
            uniqWhitelist.sort()
            if self.reqs: self.reqs += " and "
            self.reqs += "(se in %s)" % (uniqWhitelist)
            # Each task needs to include also a list of required SEs (extract
            # from the rest of reqs in the form "se1,se2,se3" (no blanks))
            self.req_se = reduce(lambda x,y: "%s,%s"%(x,y), uniqWhitelist)
                

        # TODO: Blacklist requirements (is it used?)
        # ==> I think they were already considered to create the final whitelist
        # we're using, so we're fine ignoring it



        ####  TASK LIST CREATION  ####

        # Now, we're ready to build the list of tasks
        self.taskList = []
       
        # For multiple bulk jobs there will be a list of specs (forget tar)
        if self.isBulk:
            for jobSpecId, cacheDir in self.toSubmit.items():
            
                logging.info("Enqueuing: %s" % self.specFiles[jobSpecId])
                
                task = {}
                task['id'] = jobSpecId
                task['spec'] = self.specFiles[jobSpecId]
                task['wkflow'] = self.workflowName
                task['sandbox'] = self.mainSandbox
                task['type'] = self.jobType
                task['req_se'] = self.req_se
                if self.reqs:
                    task['reqs'] = self.reqs
                else:
                    task['reqs'] = None
                self.taskList.append(Task(task))
            
            logging.info("taskList 1: %s" % (self.taskList))

        # For single jobs there will be just one job spec
        # (put it in the list anyway)
        else:
            logging.info("Enqueuing single: %s" % \
                    (self.specFiles[self.mainJobSpecName]))

            task = {}
            task['id'] = self.mainJobSpecName
            task['spec'] = self.specFiles[self.mainJobSpecName]
            task['wkflow'] = self.workflowName
            task['sandbox'] = self.mainSandbox
            task['type'] = self.jobType
            task['req_se'] = self.req_se
            if self.reqs:
                task['reqs'] = self.reqs
            else:
                task['reqs'] = None
            self.taskList.append(Task(task))

            logging.info("taskList 2: %s" % (self.taskList))


        ####  TASK LIST INSERTION  ####
            
        try:
            self.tqApi.insertTaskBulk(self.taskList)
            logging.info("tasks inserted")
        except Exception, ex:
            logging.error(">>> Error when inserting: str(ex)")
            raise JSException(str(ex), FailureList = map(lambda x: x['id'], \
                                                       self.taskList.keys()))

        # TODO: What about the whole dashboard thing?
        return




#    def checkPluginConfig(self):
#        """
#        _checkPluginConfig_

#        Make sure config has what is required for this submitter

#        """
#        if self.pluginConfig == None:
#            msg = "Failed to load Plugin Config for:\n"
#            msg += self.__class__.__name__
#            return
#        if not ('TaskQueueConfFile' in self.pluginConfig['GLITE'].keys()):
#            msg = "Failed to find TaskQueueConfFile in Plugin Config for:\n"
#            msg += self.__class__.__name__
#            return
            

registerSubmitter(TaskQueueSubmitter, TaskQueueSubmitter.__name__)
