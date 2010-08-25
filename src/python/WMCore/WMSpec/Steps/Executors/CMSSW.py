#!/usr/bin/env python
"""
_Step.Executor.CMSSW_

Implementation of an Executor for a CMSSW step

"""
__revision__ = "$Id: CMSSW.py,v 1.10 2009/12/03 22:42:26 evansde Exp $"
__version__ = "$Revision: 1.10 $"

from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
from WMCore.WMRuntime.Tools.Scram import Scram

from WMCore.FwkJobReport.Report import Report

import tempfile
import subprocess
import sys
import os
import select





class CMSSW(Executor):
    """
    _CMSWW_

    Execute a CMSSW Step

    """


    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks

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
                "InstallPSetTweak")


        if hasattr(self.step.application.configuration, 'scenario'):
            self.step.runtime.scramPreScripts.append(
                "InstallScenario")


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
            #self.report.addError(60513, "ScramSetupFailure", msg)
            print msg
            raise WMExecutionFailure(60513, "ScramSetupFailure", msg)
        runtimeOutcome = scram.runtime()
        if runtimeOutcome > 0:
            msg = scram.diagnostic()
            #self.report.addError(60513, "ScramSetupFailure", msg)
            print msg
            raise WMExecutionFailure(60513, "ScramSetupFailure", msg)


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
            if retCode > 0:
                msg = "Error running command\n%s\n" % invokeCommand
                msg += "%s\n %s\n %s\n" % (retCode, stdout, stderr)
                raise WMExecutionFailure(60514, "PreScriptFailure", msg)


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
            if retCode > 0:
                msg = "Error running command\n%s\n" % invokeCommand
                msg += "%s\n " % retCode
                msg += scram.diagnostic()
                raise WMExecutionFailure(60515, "PreScriptScramFailure", msg)


        configPath = "%s/%s-main.sh" % (self.step.builder.workingDir,
                                        self.stepName)
        handle = open(configPath, 'w')
        handle.write(configBlob)
        handle.close()
        # spawn this new process
        # the script looks for:
        # <SCRAM_COMMAND> <SCRAM_PROJECT> <CMSSW_VERSION> <JOB_REPORT> <EXECUTABLE>
        #    <CONFIG>
        # open the output files
        stdoutHandle = open( self.step.output.stdout , 'w')
        stderrHandle = open( self.step.output.stderr , 'w')

        args = ['/bin/bash', configPath, scramCommand,
                                         scramProject,
                                         cmsswVersion,
                                         jobReportXML,
                                         cmsswCommand,
                                         cmsswConfig,
                                         cmsswArguments]
        spawnedChild = subprocess.Popen( args, 0, None, None, stdoutHandle,
                                         stderrHandle )

        # loop and collect the data
        while True:
            (rdready, wrready, errready) = select.select(
                [stdoutHandle.fileno(),
                 stderrHandle.fileno()],[],[])
            # see if the process is still running
            spawnedChild.poll()
            if (spawnedChild.returncode != None):
                break
            # give the process some time to fill a buffer
            select.select([], [], [], .1)

        spawnedChild.wait()
        stdoutHandle.close()
        stderrHandle.close()

        self.step.execution.exitStatus = spawnedChild.returncode
        argsDump = { 'arguments': args}

        if spawnedChild.returncode != 0:
            msg = "Error running cmsRun\n%s\n" % argsDump
            msg += "%s\n" % spawnedChild.returncode
            raise WMExecutionFailure(spawnedChild.returncode,
                                     "CmsRunFailure", msg)

        #TODO: Move all this stuff to the CMSSW diagnostic
        ##if (spawnedChild.returncode == 70):
##            raise WMException("Wrong number of arguments to cmssw wrapper"
##                              ,None,**argsDump)
##        elif (spawnedChild.returncode == 71):
##            raise WMException("Failure in scram project"
##                              ,None,**argsDump)
##        elif (spawnedChild.returncode == 72):
##            raise WMException("Failed to chdir to the cmssw directory"
##                              ,None,**argsDump)
##        elif (spawnedChild.returncode == 73):
##            raise WMException("Failed to execute the scram runtime"
##                              ,None,**argsDump)
##        elif (spawnedChild.returncode != 0):
##            raise WMException("Unknown error in cmsRun. Code: %i" % spawnedChild.returncode, None, **argsDump)


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

echo "$SCRAM_COMMAND $SCRAM_PROJECT"

# do the actual executing
$SCRAM_COMMAND project $SCRAM_PROJECT $CMSSW_VERSION
if [ $? -ne 0 ]; then echo "Scram failed"; exit 71; fi
cd $CMSSW_VERSION
if [ $? -ne 0 ]; then echo "***\nCouldn't chdir: $?\n"; exit 72; fi
eval `$SCRAM_COMMAND runtime -sh`
if [ $? -ne 0 ]; then echo "***\nCouldn't get scram runtime: $?\n*"; exit 73; fi
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
