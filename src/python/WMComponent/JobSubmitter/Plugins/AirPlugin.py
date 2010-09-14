#!/usr/bin/env python
#pylint: disable-msg=W0102, W6501, E1103, C0301
# W0102: We want to pass blank lists by default
# for the whitelist and the blacklist
# W6501: pass information to logging using string arguments
# E1103: The thread will have a logger and a dbi before it gets here
# C0301: There are too many long string arguments to cut them down
#   without making things unreadable.

"""
_AirPlugin_

A plug-in that uses BossAir


if hasattr(self.config, 'BossAir'):
            configDict['pluginNames'] = config.BossAir.pluginNames
            configDict['pluginDir']  = config.BossAir.pluginDir
            configDict['gLiteConf']  = config.JobSubmitter.gLiteConf

"""

import os
import os.path
import logging
import threading

from subprocess import Popen, PIPE

from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit     import getWMBASE

from WMComponent.JobSubmitter.Plugins.PluginBase import PluginBase

from WMCore.BossAir.BossAirAPI  import BossAirAPI
from WMCore.Agent.Configuration import Configuration

class AirPlugin(PluginBase):
    """
    _AirPlugin_

    Use bossAir to do everything
    """


    def __init__(self, **configDict):

        PluginBase.__init__(self, config = configDict)


        config = Configuration()

        config.section_('Agent')
        config.Agent.agentName = configDict.get('agentName', 'test')

        config.component_("BossAir")
        config.BossAir.pluginNames = configDict.get('pluginNames', ['TestPlugin'])
        config.BossAir.pluginDir   = configDict.get('pluginDir', 'WMCore.BossAir.Plugins')

        config.component_("JobSubmitter")
        config.JobSubmitter.submitDir     = configDict.get('submitDir')
        config.JobSubmitter.submitNode    = configDict.get('submitNode')
        config.JobSubmitter.submitScript  = configDict.get('submitScript')
        config.JobSubmitter.gLiteConf     = configDict.get('gLiteConf')

        self.bossAir = BossAirAPI(config = config)


        myThread = threading.currentThread()        
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)

        self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")


        self.locationDict = {}

        return



    def __call__(self, parameters):
        """
        __call__
        
        Send everything directly through bossAir
        """

        if parameters == []:
            return {'NoResult': [0]}


        successList = []
        failList    = []

        listOfJobs = []
        for entry in parameters:

            logging.info("Have entry in parameters")
            logging.info(entry)
            
            jobList            = entry.get('jobs', [])
            info               = {}
            info['packageDir'] = entry.get('packageDir', None)
            info['index']      = entry.get('index', 0)
            info['sandbox']    = entry.get('sandbox', None)

            for job in jobList:
                job['location'], job['plugin'] = self.getSiteInfo(job['custom']['location'])

            logging.info("About to send to submit")
            logging.info(jobList)
            logging.info(info)
            
            localSuccess, localFail = self.bossAir.submit(jobs = jobList, info = info)
            successList.extend(localSuccess)
            failList.extend(localFail)


        logging.info("We have submitted %i jobs through BossAir" % (len(successList)))

        if len(failList) > 0:
            logging.error("BossAir reports that %i jobs have failed submission" % (len(failList)))
                     

        # Deal with ChangeState pass/fail
        if len(successList) > 0:
            self.passJobs(jobList = successList)
        if len(failList) > 0:
            self.failJobs(jobList = failList)
            


        return len(successList)



    def getSiteInfo(self, jobSite):
        """
        _getSiteInfo_

        This is how you get the name of a CE and the plugin for a job
        """

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName = jobSite)
            self.locationDict[jobSite] = siteInfo[0]
        return (self.locationDict[jobSite].get('ce_name'),
                self.locationDict[jobSite].get('plugin'))
                
