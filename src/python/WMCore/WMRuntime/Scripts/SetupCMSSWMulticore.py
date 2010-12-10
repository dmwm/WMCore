#!/usr/bin/env python
# encoding: utf-8
"""
SetupCMSSWMulticore.py

Created by Dave Evans on 2010-11-12.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import json
import subprocess
import FWCore.ParameterSet.Config as cms

from WMCore.WMRuntime.ScriptInterface import ScriptInterface

class SetupCMSSWMulticore(ScriptInterface):
    """
    _SetupCMSSWMulticore_
    
    runtime Util to prime CMSSW multicore job
    
    """
    def __call__(self):
        """
        Setup for Multicore
        
        """
        step = self.step.data
        multicoreSettings = step.application.multicore
        self.jsonfile = None
        self.files = {}
        
        self.buildManifest()
        self.readManifest()
        self.buildPSet()
        
        return 0
        
        
        
    def buildManifest(self):
        """
        _buildManifest_
    
        Generate the JSON file describing the input files
        """
        utilProcess = subprocess.Popen(
            ["/bin/bash"], shell=True, cwd=self.step.data.builder.workingDir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            )
        utilProcess.stdin.write("%s\n" % self.step.data.application.multicore.edmFileUtil)
        stdout, stderr = utilProcess.communicate()
        retCode = utilProcess.returncode
        print stdout, stderr
        if retCode != 0:
            #ToDo: Raise exception
            return retCode
    
        self.jsonfile = os.path.join(self.step.data.builder.workingDir, self.step.data.application.multicore.inputmanifest)
        if not os.path.exists(self.jsonfile):
            return 1000001
        return 0
    
    def readManifest(self):
        """
        _readManifest_
    
        Read the JSON manifest file and store the information about each input file
        """
        jsondata = json.load(open(self.jsonfile, 'r'))
        for x in jsondata:
            data = {}
            lfn = str(x[u'file'])
            for k,v in x.items():
                data[str(k)] = v
            self.files[lfn] = data
        return
    
    
    def buildPSet(self):
        """
        _buildPSet_
    
        Build the Multicore PSet in options setting the number of cores
        and the number of events per core 
    
        """
        #ToDo: Adjust this if the job has an event mask
        eventTotal =  eventcount(self.files.values())
        numCores = self.step.data.application.multicore.numberOfCores
        procCount = (eventTotal + numCores - 1) / numCores
    
    
        pset = self.loadPSet()
    
        options = getattr(pset, "options", None)
        if options == None:
            pset.options = cms.untracked.PSet()
            options = getattr(pset, "options")
        multiProcesses = getattr(options, "multiProcesses", None)
        if multiProcesses == None:
            options.multiProcesses = cms.untracked.PSet()
            multiProcesses = getattr(options, "multiProcesses")
    
        multiProcesses.maxSequentialEventsPerChild = cms.untracked.uint32(procCount)
        multiProcesses.maxChildProcesses = cms.untracked.int32(numCores)
    
        configFile = self.step.data.application.command.configuration
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        handle.write(pset.dumpPython())
        handle.close()
    
    
    def loadPSet(self):
        """
        _loadPSet_
    
        Load a PSet that was shipped with the job sandbox.
        """
        psetModule = "WMTaskSpace.%s.PSet" % self.step.data._internal_name
    
        try:
            processMod = __import__(psetModule, globals(), locals(), ["process"], -1)
            process = processMod.process
        except ImportError, ex:
            msg = "Unable to import process from %s:\n" % psetModule
            msg += str(ex)
            print msg
            return 1
    
        return process
    
            

        
        
        
            