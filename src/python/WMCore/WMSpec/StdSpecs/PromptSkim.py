#!/usr/bin/env python
"""
_PromptSkim_

Standard PromptSkimming Workflow
"""

import os
import sys
import tempfile
import urllib
import shutil
import logging

from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessingWorkloadFactory
from WMCore.WMRuntime.Tools.Scram import Scram
from WMCore.WMInit import getWMBASE
from WMCore.Cache.WMConfigCache import ConfigCache

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required 
    by the standard ReReco workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """
    arguments = {
        "AcquisitionEra": "WMAgentCommissioning10",
        "Requestor": "sfoulkes@fnal.gov",
        "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
        "CMSSWVersion": "CMSSW_3_9_5",

        "SkimConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/HeavyIonsAnalysis/Configuration/test/centralSkimsHI_SKIM.py?revision=1.7",
        "BlockName": ["SomeBlock"],
        "CustodialSite": ["SomeSite"],
        "InitCommand": ". /uscmst1/prod/sw/cms/setup/shrc prod",

        "ScramArch": "slc5_amd64_gcc434",
        "ProcessingVersion": "v3",
        "GlobalTag": "GR10_P_V12::All",
        
        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "scf_wmagent_configcache",
        }

    return arguments

class PromptSkimWorkloadFactory(DataProcessingWorkloadFactory):
    """
    _PromptSkimWorkloadFactory_

    Stamp out PromptSkim workflows.
    """
    def __init__(self):
        DataProcessingWorkloadFactory.__init__(self)
        return

    def injectIntoConfigCache(self, frameworkVersion, scramArch, initCommand,
                              configUrl, configLabel, couchUrl, couchDBName):
        """
        _injectIntoConfigCache_

        """
        logging.error("Injecting to config cache.\n");
        configTempDir = tempfile.mkdtemp()
        configPath = os.path.join(configTempDir, "cmsswConfig.py")
        configString = urllib.urlopen(configUrl).read(-1)
        configFile = open(configPath, "w")
        configFile.write(configString)
        configFile.close()

        scramTempDir = tempfile.mkdtemp()
        wmcoreBase = getWMBASE()
        envPath = os.path.normpath(os.path.join(getWMBASE(), "../../../../../../../../apps/wmagent/etc/profile.d/init.sh"))
        scram = Scram(version = frameworkVersion, architecture = scramArch,
                      directory = scramTempDir, initialise = initCommand,
                      envCmd = "source %s" % envPath)
        scram.project()
        scram.runtime()

        scram("python2.6 %s/../../../bin/inject-to-config-cache %s %s PromptSkimmer cmsdataops %s %s None" % (wmcoreBase,
                                                                                                     couchUrl,
                                                                                                     couchDBName,
                                                                                                     configPath,
                                                                                                     configLabel))

        shutil.rmtree(configTempDir)
        shutil.rmtree(scramTempDir)
        return
    
    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a PromptSkimming workload with the given parameters.
        """
        self.injectIntoConfigCache(arguments["CMSSWVersion"], arguments["ScramArch"],
                                   arguments["InitCommand"], arguments["SkimConfig"], workloadName,
                                   arguments["CouchURL"], arguments["CouchDBName"])

        try:
            configCache = ConfigCache(arguments["CouchURL"], arguments["CouchDBName"])
            arguments["ProcConfigCacheID"] = configCache.getIDFromLabel(workloadName)
        except Exception, ex:
            logging.error("There was an exception loading the config out of the")
            logging.error("ConfigCache.  Check the scramOutput.log file in the")
            logging.error("PromptSkimScheduler directory to find out what went")
            logging.error("wrong.")
            raise
        
        workload = DataProcessingWorkloadFactory.__call__(self, workloadName, arguments)

        # We need to strip off "MSS" as that causes all sorts of problems.
        if arguments["CustodialSite"].find("MSS") != -1:
            site = arguments["CustodialSite"][:-4]
        else:
            site = arguments["CustodialSite"]
            
        workload.setSiteWhitelist(site)
        workload.setBlockWhitelist(arguments["BlockName"])
        return workload

def promptSkimWorkload(workloadName, arguments):
    """
    _promptSkimWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myPromptSkimFactory = PromptSkimWorkloadFactory()
    return myPromptSkimFactory(workloadName, arguments)
