#!/usr/bin/env python
"""
_Step.Executor.CMSSW_

Implementation of an Executor for a CMSSW step

"""
__revision__ = "$Id: CMSSW.py,v 1.8 2009/11/19 11:59:58 evansde Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMException import WMException
from WMCore.WMSpec.WMStep import WMStepHelper
from WMCore.WMRuntime.Tools.Scram import Scram

from WMCore.FwkJobReport.Report import Report

import tempfile
import subprocess
import sys
import os
import select



getStepName = lambda step: WMStepHelper(step).name()

class CMSSW(Executor):
    """
    _CMSWW_

    Execute a CMSSW Step

    """


    def pre(self, emulator = None):
        """
        _pre_

        Pre execution work.

        Determine where the configuration is coming and add the appropriate
        tools to get it

        """
        if (emulator != None):
            return emulator.emulatePre( self.step )

        if hasattr(self.step.application.configuration, 'configCacheUrl'):
            # means we have a configuration & tweak in the sandbox
            psetFile = self.step.application.command.configuration
            psetTweak = self.step.application.command.psetTweak
            self.stepSpace.getFromSandbox(psetFile)
            self.stepSpace.getFromSandbox(psetTweak)
            self.step.runtime.scramPreScripts.append(
                "WMCore.WMRuntime.Scripts.InstallPSetTweak")


        #TODO: Handle case where we have a config library instead

        return None



    def execute(self, emulator = None):
        """
        _execute_


        """


        stepModule = "WMTaskSpace.%s" % self.stepName
        if (emulator != None):
            return emulator.emulate( self.step, self.job )





        # write the wrapper script to a temporary location
        # I don't pass it directly through os.system because I don't
        # trust that there won't be shell-escape shenanigans with
        # arbitrary input files
        scramCommand   = self.step.application.setup.scramCommand
        scramProject   = self.step.application.setup.scramProject
        cmsswVersion   = self.step.application.setup.cmsswVersion
        jobReportXML   = self.step.output.jobReport
        cmsswCommand   = self.step.application.command.executable
        cmsswConfig    = self.step.application.command.configuration
        cmsswArguments = self.step.application.command.arguments

        #
        # scram bootstrap
        #
        scram = Scram(
            command = scramCommand,
            version = cmsswVersion,
            initialisation = self.step.application.setup.softwareEnvironment,
            directory = self.step.builder.workingDir
            )

        projectOutcome = scram.project()
        if projectOutcome > 0:
            msg = scram.diagnostic()
            self.report.addError(60513, "ScramSetupFailure", msg)
            return
        runtimeOutcome = scram.runtime()
        if runtimeOutcome > 0:
            msg = scram.diagnostic()
            self.report.addError(60513, "ScramSetupFailure", msg)
            return


        #
        # pre scripts
        #
        for script in self.step.runtime.preScripts:
            # TODO: Exception handling and error handling & logging
            scriptProcess = subprocess.Popen(
                ["/bin/bash"], shell=True, cwd=self.step.builder.workingDir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                )
            invokeCommand = "%s -m WMCore.WMRuntime.ScriptInvoke %s %s \n" % (
                sys.executable,
                stepModule,
                script)

            scriptProcess.stdin.write(invokeCommand)
            stdout, stderr = scriptProcess.communicate()
            retCode = scriptProcess.returncode
            print stdout, stderr, retCode


        #
        # pre scripts with scram
        #
        for script in self.step.runtime.scramPreScripts:
            "invoke scripts with scram()"
            invokeCommand = "%s -m WMCore.WMRuntime.ScriptInvoke %s %s \n" % (
                sys.executable,
                stepModule,
                script)


            retCode = scram(invokeCommand)
            print scram.stdout, scram.stderr, retCode

        configPath = "%s/%s-main.sh" % (self.step.builder.workingDir, helper.name())
        handle = open(configPath, 'w')
        handle.write(configBlob)
        handle.close()
        # spawn this new process
        # the script looks for:
        # <SCRAM_COMMAND> <SCRAM_PROJECT> <CMSSW_VERSION> <JOB_REPORT> <EXECUTABLE>
        #    <CONFIG>
        args = ['/bin/bash', configPath, scramCommand,
                                         scramProject,
                                         cmsswVersion,
                                         jobReportXML,
                                         cmsswCommand,
                                         cmsswConfig,
                                         cmsswArguments]
        spawnedChild = subprocess.Popen( args, 0, None, None, subprocess.PIPE,
                                                         subprocess.PIPE )
        # open the output files
        stdoutHandle = open( self.step.output.stdout , 'w')
        stderrHandle = open( self.step.output.stderr , 'w')

        # loop and collect the data
        while (1):
            (rdready, wrready, errready) = select.select([spawnedChild.stdout,
                                                 spawnedChild.stderr],[],[])
            if stdoutHandle in rdready:
                ourbuffer = spawnedChild.stdout.read(-1)
                stdoutHandle.write(buffer)
            if stderrHandle in rdready:
                ourbuffer = spawnedChild.stderr.read(-1)
                stderrHandle.write(buffer)

            # see if the process is still running
            spawnedChild.poll()
            if (spawnedChild.returncode != None):
                break
            # give the process some time to fill a buffer
            select.select([], [], [], .1)

        spawnedChild.wait()
        # the spawned CMSSW shell has returned, let's interpret return calls
        # I'm avoiding the codes from
        # https://twiki.cern.ch/twiki/bin/view/CMS/JobExitCodes
        # 70 we called the script with too few arguments
        # 71 scram project failure
        # 72 chdir failure
        # 73 scram runtime fail
        # FIXME python doesn't have a switch construct, is there a nicer
        #    way to do this?
        argsDump = { 'arguments': args}
        if (spawnedChild.returncode == 70):
            raise WMException("Wrong number of arguments to cmssw wrapper"
                              ,None,**argsDump)
        elif (spawnedChild.returncode == 71):
            raise WMException("Failure in scram project"
                              ,None,**argsDump)
        elif (spawnedChild.returncode == 72):
            raise WMException("Failed to chdir to the cmssw directory"
                              ,None,**argsDump)
        elif (spawnedChild.returncode == 73):
            raise WMException("Failed to execute the scram runtime"
                              ,None,**argsDump)
        elif (spawnedChild.returncode != 0):
            raise WMException("Unknown error in cmsRun. Code: %i" % spawnedChild.returncode, None, **argsDump)

        step.execution.exitStatus = spawnedChild.returncode
        return


    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        print "Steps.Executors.CMSSW.post called"
        if (emulator != None):
            return emulator.emulatePost( self.step )

        return None

configBlob = """#!/bin/bash

# Check to make sure the argument count is correct
REQUIRED_ARGUMENT_COUNT=5
if [ $# -lt $REQUIRED_ARGUMENT_COUNT ]
then
    echo "Usage: `basename $0` <SCRAM_COMMAND> <SCRAM_PROJECT> <CMSSW_VERSION>\
                 <JOB_REPORT> <EXECUTABLE> <CONFIG> [Arguments for cmsRun]"
    exit 70
fi

# Extract the required arguments out, leaving an unknown number of
#  cmsRun arguments
SCRAM_COMMAND=$1
SCRAM_PROJECT=$2
CMSSW_VERSION=$3
JOB_REPORT=$4
EXECUTABLE=$5
CONFIGURATION=$6
shift;shift;shift
shift;shift;shift

# do the actual executing
$SCRAM_COMMAND $SCRAM_PROJECT $CMSSW_VERSION
if [ $? -ne 0] then echo "***\nScram failed: $?\n"; exit 71; fi
cd $CMSSW_VERSION
if [ $? -ne 0] then echo "***\nCouldn't chdir: $?\n"; exit 72; fi
eval `$SCRAM_COMMAND runtime -sh`
if [ $? -ne 0] then echo "***\nCouldn't get scram runtime: $?\n*"; exit 73; fi
cd ..
$EXECUTABLE "$@" -j $JOB_REPORT $CONFIG &
PROCID=$!
echo $PROCID > process.id
wait $PROCID
EXIT_STATUS=$?
echo "process id is $PROCID status is $EXIT_STATUS"
exit $EXIT_STATUS

"""

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    #unittest.main()
    import WMCore.WMSpec.WMStep as WMStep
    tmpstep = WMStep.makeWMStep('runstep')
    test = CMSSW()
    test.pre( tmpstep )
    test.execute( tmpstep, tmpstep )
    test.post( tmpstep )
