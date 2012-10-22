#!/usr/bin/env python
# encoding: utf-8
"""
TaskChainRequest_t.py

Created by Dave Evans on 2011-07-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest

from WMCore.RequestManager.RequestMaker.Processing.TaskChainRequest import TaskChainRequest, TaskChainSchema

class TaskChainRequestTest(unittest.TestCase):
    def setUp(self):
        pass

    def testA(self):
        """test with generation TaskChain"""

        request = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": "v1",
            "GlobalTag": "GR10_P_v4::All",
            "CouchURL": "http://couchserver.cern.ch",
            "CouchDBName": "config_cache",
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "TaskChain" : 3,
        }


        request['Task1'] = {
            "TaskName" : "GenSim",
            "ConfigCacheID" : "ConfigIDHere",
            "SplittingAlgorithm"  : "EventBased",
            "SplittingArguments" : {"events_per_job" : 250},
            "RequestSizeEvents" : 10000,
            "Seeding" : "Automatic",
            "PrimaryDataset" : "RelValTTBar",
        }

        request['Task2'] = {
            "TaskName" : "DigiHLT",
              "InputTask" : "GenSim",
              "InputFromOutputModule" : "writeGENSIM",
              "ConfigCacheID" : "17612875182763812763812",
              "SplittingAlgorithm" : "FileBased",
              "SplittingArguments" : {"files_per_job" : 1 }
        }

        request['Task3'] = {
             "TaskName" : "Reco",
               "InputTask" : "DigiHLT",
               "InputFromOutputModule" : "writeDIGIRECO",
               "ConfigCacheID" : "17612875182763812763812",
               "SplittingAlgorithm" : "FileBased",
               "SplittingArguments" : {"files_per_job" : 1 }
        }


        schema = TaskChainSchema()
        schema.update(request)
        schema.validate()

    def testB(self):
        """test with processing TaskChain"""

        request = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": "v1",
            "GlobalTag": "GR10_P_v4::All",
            "CouchURL": "http://couchserver.cern.ch",
            "CouchDBName": "config_cache",
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "TaskChain" : 3,
        }


        request['Task1'] = {
            "TaskName" : "ReadStuff",
            "ConfigCacheID" : "ConfigIDHere",
            "SplittingAlgorithm"  : "EventBased",
            "SplittingArguments" : {"events_per_job" : 250},
            "InputDataset" : "/Minbias/CollisionsAndStuff/RAW"
        }

        request['Task2'] = {
            "TaskName" : "DigiHLT",
              "InputTask" : "ReadStuff",
              "InputFromOutputModule" : "writeStuff",
              "ConfigCacheID" : "17612875182763812763812",
              "SplittingAlgorithm" : "FileBased",
              "SplittingArguments" : {"files_per_job" : 1 }
        }

        request['Task3'] = {
             "TaskName" : "Reco",
               "InputTask" : "DigiHLT",
               "InputFromOutputModule" : "writeDIGIRECO",
               "ConfigCacheID" : "17612875182763812763812",
               "SplittingAlgorithm" : "FileBased",
               "SplittingArguments" : {"files_per_job" : 1 }
        }


        schema = TaskChainSchema()
        schema.update(request)
        schema.validate()

if __name__ == '__main__':
    unittest.main()
