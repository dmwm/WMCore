#!/bin/env python
#pylint: disable=E1103, C0103
# E1103: Threads have things attached to them
# C0103: We use readable names for method names
"""
Unittest to see if the main configs need updating

"""

import re
import os.path
import logging
import unittest
import threading

from nose.plugins.attrib import attr

import WMCore.WMInit
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

from WMCore.Agent.Configuration import loadConfigurationFile



# Now comes the ugly work
from WMComponent.JobCreator.JobCreator          import JobCreator
from WMComponent.JobSubmitter.JobSubmitter      import JobSubmitter
from WMComponent.JobAccountant.JobAccountant    import JobAccountant
from WMComponent.JobTracker.JobTracker          import JobTracker
from WMComponent.JobArchiver.JobArchiver        import JobArchiver
from WMComponent.TaskArchiver.TaskArchiver      import TaskArchiver
from WMComponent.ErrorHandler.ErrorHandler      import ErrorHandler
from WMComponent.RetryManager.RetryManager      import RetryManager
from WMComponent.DBS3Buffer.DBS3Upload          import DBS3Upload


class ConfigTest(unittest.TestCase):
    """
    Test class for the WMAgent configs

    """


    def setUp(self):
        """
        _setUp_

        Create the whole database
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase = True)
        self.testInit.setSchema(customModules = ["WMCore.WMBS",
                                                 "WMComponent.DBS3Buffer",
                                                 'WMCore.ResourceControl',
                                                 'WMCore.BossAir'],
                                useDefault = False)
        self.dbName = 'config_t'
        self.testInit.setupCouch("%s/jobs" % self.dbName, "JobDump")
        self.testInit.setupCouch("%s/fwjrs" % self.dbName, "FWJRDump")
        self.testInit.setupCouch("%s/acdc" % self.dbName, "GroupUser", "ACDC")
        self.testInit.setupCouch("%s/workqueue" % self.dbName, 'WorkQueue')
        self.testInit.setupCouch("%s/workloadsummary" % self.dbName,
                                 "WorkloadSummary")

        self.testDir = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        _tearDown_

        Tear things down and go home
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        return


    def initComponents(self, configPath):
        """
        _initComponents_

        Start up the various components using the config
        """

        if os.path.isfile(configPath):
            # Read the config
            config = loadConfigurationFile(configPath)
        else:
            msg = "No config file at desired location"
            logging.error(msg)
            raise Exception(msg)


        masterConfig = self.testInit.getConfiguration()
        config.Agent.useHeartbeat = False
        config.CoreDatabase.socket     = masterConfig.CoreDatabase.socket
        config.CoreDatabase.connectUrl = masterConfig.CoreDatabase.connectUrl


        # Have to do this because the agent hard codes its dirs
        oldWorkDir = config.General.workDir

        for compName in (config.listComponents_() + config.listWebapps_()):
            component = getattr(config, compName)
            for var in component.listSections_():
                value = getattr(component, var)
                if type(value) == str:
                    if re.search(oldWorkDir, value):
                        # Then set it
                        setattr(component, var, value.replace(oldWorkDir, self.testDir))
                elif type(value) == list:
                    # Go through it one component at a time
                    for element in value:
                        if type(element) == str and re.search(oldWorkDir, element):
                            index = value.index(element)
                            value.remove(element)
                            value.insert(index, value.replace(oldWorkDir, self.testDir))
                    setattr(component, var, value)
                elif type(value) == dict:
                    for key in value.keys():
                        if type(value[key]) == str and re.search(oldWorkDir, value[key]):
                            value[key] = value[key].replace(oldWorkDir, self.testDir)
                    setattr(component, var, value)


        compList = (config.listComponents_() + config.listWebapps_())
        components = []

        config.JobStateMachine.couchurl                = os.environ['COUCHURL']
        config.JobStateMachine.couchDBName             = self.dbName
        config.ACDC.couchurl                           = os.environ['COUCHURL']
        config.ACDC.database                           = '%s/acdc' % self.dbName
        config.TaskArchiver.workloadSummaryCouchDBName = '%s/workloadsummary' % self.dbName
        config.TaskArchiver.workloadSummaryCouchURL    = os.environ['COUCHURL']

        if hasattr(config, 'WorkQueueManager'):
            config.WorkQueueManager.couchurl   = os.environ['COUCHURL']
            config.WorkQueueManager.dbname     = '%s/workqueue' % self.dbName

        if hasattr(config, 'WorkloadSummary'):
            config.WorkloadSummary.couchurl    = os.environ['COUCHURL']
            config.WorkloadSummary.database    = '%s/workloadsummary' % self.dbName


        # Get all components


        components.append(JobCreator(config = config))
        components.append(JobSubmitter(config = config))
        components.append(JobTracker(config = config))
        components.append(JobAccountant(config = config))
        components.append(JobArchiver(config = config))
        components.append(TaskArchiver(config = config))
        components.append(ErrorHandler(config = config))
        components.append(RetryManager(config = config))
        components.append(DBS3Upload(config = config))

        # Init threads:
        for component in components:
            component.initInThread()

        # preInitialize
        for component in components:
            component.preInitialization()

        for component in components:
            component.prepareToStop()

        return


    @attr('integration')
    def testA_WMAgentConfig(self):
        """
        _WMAgentConfig_

        Test the WMAgentConfig file in WMCORE/etc
        """

        # Get the config
        configPath = os.path.join(WMCore.WMInit.getWMBASE(),
                                  'etc', 'WMAgentConfig.py')


        self.initComponents(configPath = configPath)


        return

if __name__ == "__main__":
    unittest.main()
