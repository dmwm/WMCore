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
        configTempDir = tempfile.mkdtemp()
        configPath = os.path.join(configTempDir, "cmsswConfig.py")
        configString = urllib.urlopen(configUrl).read(-1)
        configFile = open(configPath, "w")
        configFile.write(configString)
        configFile.close()

        scramTempDir = tempfile.mkdtemp()
        scram = Scram(version = frameworkVersion, architecture = scramArch,
                      directory = scramTempDir, initialise = initCommand)
        scram.project()
        scram.runtime()

        wmcoreBase = getWMBASE()
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

        configCache = ConfigCache(arguments["CouchURL"], arguments["CouchDBName"])
        arguments["ProcConfigCacheID"] = configCache.getIDFromLabel(workloadName)
        
        workload = DataProcessingWorkloadFactory.__call__(self, workloadName, arguments)
        workload.setSiteWhitelist(arguments["CustodialSite"])
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
