#!/bin/env python
#pylint: disable-msg=E1103, C0103
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

import WMCore.WMInit


from WMQuality.TestInit import TestInit

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
from WMComponent.DBSUpload.DBSUpload            import DBSUpload
from WMComponent.PhEDExInjector.PhEDExInjector  import PhEDExInjector


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
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS", 'WMCore.MsgService',
                                                 'WMCore.ResourceControl',
                                                 'WMCore.BossLite'],
                                useDefault = False)


        self.testDir = self.testInit.generateWorkDir()

        return

    def tearDown(self):
        """
        _tearDown_

        Tear things down and go home
        """

        self.testInit.clearDatabase()

        #self.testInit.delWorkDir()

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
                    for element in list:
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


        # Get all components
        

        components.append(JobCreator(config = config))
        components.append(JobSubmitter(config = config))
        components.append(JobTracker(config = config))
        components.append(JobAccountant(config = config))
        components.append(JobArchiver(config = config))
        components.append(TaskArchiver(config = config))
        components.append(ErrorHandler(config = config))
        components.append(RetryManager(config = config))
        components.append(DBSUpload(config = config))


        # Now the optional ones
        if 'PhEDExInjector' in compList:
            components.append(PhEDExInjector(config = config))


        # Init threads:
        for component in components:
            component.initInThread()

        # preInitialize
        for component in components:
            component.preInitialization()

        for component in components:
            component.prepareToStop()



        

        return



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


    def testB_WMAgentPromptSkimConfig(self):
        """
        _WMAgentConfig_

        Test the WMAgentConfig file in WMCORE/etc
        """

        # Get the config
        configPath = os.path.join(WMCore.WMInit.getWMBASE(),
                                  'etc', 'WMAgentPromptSkimConfig.py')

    
        self.initComponents(configPath = configPath)


        return



if __name__ == "__main__":

    unittest.main() 
