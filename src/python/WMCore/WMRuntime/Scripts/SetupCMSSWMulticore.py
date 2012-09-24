#!/usr/bin/env python
# encoding: utf-8
"""
SetupCMSSWMulticore.py

Created by Dave Evans on 2010-11-12.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import pickle
import traceback
import sys
import os
import json
import subprocess
import FWCore.ParameterSet.Config as cms

from WMCore.WMRuntime.ScriptInterface import ScriptInterface
eventcount = lambda x: sum( [j['events'] for j in x ])

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
        try:
            self.buildManifest()
        except Exception, ex:
            print 'Exception building manifest:\n%s' % str(ex)
            return 1
        try:
            self.readManifest()
        except Exception, ex:
            print 'Exception reading manifest:\n%s' % str(ex)
            return 2
        self.buildPSet()

        return 0



    def buildManifest(self):
        """
        _buildManifest_

        Generate the JSON file describing the input files
        """
        utilProcess = subprocess.Popen(
            ["/bin/bash"], shell=True, cwd=self.stepSpace.location,
            env = os.environ,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            )
        utilProcess.stdin.write("%s\n" % self.step.data.application.multicore.edmFileUtil)
        stdout, stderr = utilProcess.communicate()
        retCode = utilProcess.returncode
        if retCode != 0:
            msg = "Error running manifest builder:\n"
            msg += "Executing command: \n%s\n" % self.step.data.application.multicore.edmFileUtil
            msg += "In environment: \n%s\n" % os.environ
            msg += "Stdout: \n%s\n" % stdout
            msg += "Stderr: \n%s\n" % stderr
            print msg
            raise RuntimeError, msg

        self.jsonfile = os.path.join(self.stepSpace.location, self.step.data.application.multicore.inputmanifest)
        if not os.path.exists(self.jsonfile):
            return 1000001
        return 0

    def readManifest(self):
        """
        _readManifest_

        Read the JSON manifest file and store the information about each input file
        """
        try:
            jsondata = json.load(open(self.jsonfile, 'r'))
        except Exception, ex:
            msg = "Unable to read JSON Manifest file produced by edmFileUtil\n"
            msg += str(ex)
            msg += "\n manifest file contents:\n"
            msg += open(self.jsonfile, 'r').read()
            msg += "\n\n"
            print msg
            raise RuntimeError, msg

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
        if numCores == "auto":
            p1 = subprocess.Popen("egrep \"^processor\" /proc/cpuinfo", shell = True,
                                  stdout=subprocess.PIPE)
            p2 = subprocess.Popen("wc -l", stdin=p1.stdout, stdout=subprocess.PIPE, shell = True)
            output = p2.communicate()[0]
            numCores = int(output)
        else:
            numCores = int(numCores)

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

        #multiProcesses.maxSequentialEventsPerChild = cms.untracked.uint32(procCount)
        #revlimiter
        multiProcesses.maxSequentialEventsPerChild = cms.untracked.uint32(2)
        multiProcesses.maxChildProcesses = cms.untracked.int32(numCores)

        configFile = self.step.data.application.command.configuration
        configPickle = getattr(self.step.data.application.command, "configurationPickle", "PSet.pkl")
        workingDir = self.stepSpace.location
        handle = open("%s/%s" % (workingDir, configFile), 'w')
        pHandle = open("%s/%s" % (workingDir, configPickle), 'wb')
        try:
            pickle.dump(pset, pHandle)
            handle.write("import FWCore.ParameterSet.Config as cms\n")
            handle.write("import pickle\n")
            handle.write("handle = open('%s', 'rb')\n" % configPickle)
            handle.write("process = pickle.load(handle)\n")
            handle.write("handle.close()\n")
        except Exception, ex:
            print "Error writing out PSet:"
            print traceback.format_exc()
            raise ex
        finally:
            handle.close()
            pHandle.close()

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
