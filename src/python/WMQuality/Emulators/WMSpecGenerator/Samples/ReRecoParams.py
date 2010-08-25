# this is for actual test
from WMCore.WMSpec.StdSpecs.ReReco import getTestArguments

def getParams():
    testArgs = getTestArguments()
    # remove couch config
    testArgs.update({'CouchUrl': None, 'CouchDBName': None})
    
    return testArgs

# remove couch config


# This for the emulator test.
#MinBiasWithoutEmulator = {
#    "CmsPath": "/uscmst1/prod/sw/cms",
#    "AcquisitionEra": "WMAgentCommissioning10",
#    "Requester": "sfoulkes@fnal.gov",
#    "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
#    "CMSSWVersion": "CMSSW_3_5_8_patch3",
#    "ScramArch": "slc5_ia32_gcc434",
#    "ProcessingVersion": "v20scf",
#    "SkimInput": "output",
#    "GlobalTag": "GR10_P_v4::All",
#    
#    "ProcessingConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/rereco_FirstCollisions_MinimumBias_35X.py?revision=1.8",
#    "SkimConfig": "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1",
#    
#    "CouchUrl": None,
#    "CouchDBName": None,
#    "Scenario": ""
##     "scenario": "cosmics",
##     "processingOutputModules": {"outputRECORECO": {"dataTier": "RECO", "filterName": ""},
##                                 "outputALCARECOALCARECO": {"dataTier": "ALCARECO", "filterName": ""}},
##     "skimOutputModules": {},
##     "processingConfig": "",
##     "skimConfig": ""
#    
#    }