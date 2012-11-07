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
import re

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
        "CMSSWVersion": "CMSSW_5_2_5_patch1",

        "SkimConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_MET.py?revision=1.4",
        "BlockName": ["SomeBlock"],
        "CustodialSite": "SomeSite",
        "InitCommand": ". /uscmst1/prod/sw/cms/setup/shrc prod",

        "ScramArch": "slc5_amd64_gcc462",
        "ProcessingVersion": "PromptSkim-v1",
        "GlobalTag": "GR10_P_V12::All",

        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "scf_wmagent_configcache",

        "DashboardHost": "127.0.0.1",
        "DashboardPort": 8884,

        "RunNumber" : 195099,
        }

    return arguments

def fixCVSUrl(url):
    """
    _fixCVSUrl_

    Checks the url, if it looks like a cvs url then make sure it has no
    view option in it, so it can be downloaded correctly
    """
    cvsPatt = '(http://cmssw\.cvs\.cern\.ch.*\?).*(revision=[0-9]*\.[0-9]*).*'
    cvsMatch = re.match(cvsPatt, url)
    if cvsMatch:
        url = cvsMatch.groups()[0] + cvsMatch.groups()[1]
    return url

def injectIntoConfigCache(frameworkVersion, scramArch, initCommand,
                          configUrl, configLabel, couchUrl, couchDBName,
                          envPath = None, binPath = None):
    """
    _injectIntoConfigCache_
    """
    logging.info("Injecting to config cache.\n")
    configTempDir = tempfile.mkdtemp()
    configPath = os.path.join(configTempDir, "cmsswConfig.py")
    configString = urllib.urlopen(fixCVSUrl(configUrl)).read(-1)
    configFile = open(configPath, "w")
    configFile.write(configString)
    configFile.close()

    scramTempDir = tempfile.mkdtemp()
    wmcoreBase = getWMBASE()
    if not envPath:
        envPath = os.path.normpath(os.path.join(wmcoreBase, "../../../../../../../../apps/wmagent/etc/profile.d/init.sh"))
    scram = Scram(version = frameworkVersion, architecture = scramArch,
                  directory = scramTempDir, initialise = initCommand,
                  envCmd = "source %s" % envPath)
    scram.project()
    scram.runtime()

    if not binPath:
        scram("python2.6 %s/../../../bin/inject-to-config-cache %s %s PromptSkimmer cmsdataops %s %s None" % (wmcoreBase,
                                                                                                              couchUrl,
                                                                                                              couchDBName,
                                                                                                              configPath,
                                                                                                              configLabel))
    else:
        scram("python2.6 %s/inject-to-config-cache %s %s PromptSkimmer cmsdataops %s %s None" % (binPath,
                                                                                                 couchUrl,
                                                                                                 couchDBName,
                                                                                                 configPath,
                                                                                                 configLabel))

    shutil.rmtree(configTempDir)
    shutil.rmtree(scramTempDir)
    return

def parseT0ProcVer(procVer, procString = None):
    compoundProcVer = r"^(((?P<ProcString>[a-zA-Z0-9_]+)-)?v)?(?P<ProcVer>[0-9]+)$"
    match = re.match(compoundProcVer, procVer)
    if match:
        return {'ProcString' : match.group('ProcString') or procString,
                'ProcVer' : int(match.group('ProcVer'))}
    logging.error('Processing version %s is not compatible'
                                % procVer)
    raise Exception

class PromptSkimWorkloadFactory(DataProcessingWorkloadFactory):
    """
    _PromptSkimWorkloadFactory_

    Stamp out PromptSkim workflows.
    """
    def __init__(self):
        DataProcessingWorkloadFactory.__init__(self)
        return

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a PromptSkimming workload with the given parameters.
        """
        injectIntoConfigCache(arguments["CMSSWVersion"], arguments["ScramArch"],
                              arguments["InitCommand"], arguments["SkimConfig"], workloadName,
                              arguments["CouchURL"], arguments["CouchDBName"],
                              arguments.get("EnvPath", None), arguments.get("BinPath", None))

        try:
            configCache = ConfigCache(arguments["CouchURL"], arguments["CouchDBName"])
            arguments["ProcConfigCacheID"] = configCache.getIDFromLabel(workloadName)
            if not arguments["ProcConfigCacheID"]:
                logging.error("The configuration was not uploaded to couch")
                raise Exception
        except Exception:
            logging.error("There was an exception loading the config out of the")
            logging.error("ConfigCache.  Check the scramOutput.log file in the")
            logging.error("PromptSkimScheduler directory to find out what went")
            logging.error("wrong.")
            raise

        parsedProcVer = parseT0ProcVer(arguments["ProcessingVersion"],
                                       'PromptSkim')
        arguments["ProcessingString"] = parsedProcVer["ProcString"]
        arguments["ProcessingVersion"] = parsedProcVer["ProcVer"]

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
