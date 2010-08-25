#!/usr/bin/env python
"""
_Step.Executor.CMSSW_

Implementation of an Executor for a CMSSW step

"""
__revision__ = "$Id: CMSSW.py,v 1.3 2009/06/11 15:56:56 meloam Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMSpec.Steps.Executor import Executor
import WMCore.Cache.ConfigCache as ConfigCache
import tempfile
import subprocess
import os
import select

class CMSSW(Executor):
    """
    _CMSWW_

    Execute a CMSSW Step

    """


    def pre(self, step, emulator = None):
        """
        _pre_

        Pre execution checks

        """
        if (emulator != None):
            return emulator.emulatePre( step )
        
        cache        = ConfigCache.WMConfigCache('testdb2')
        handle       = open(step.application.command.configuration, 'w')
        configHash   = step.application.command.configurationHash
        configString = cache.getConfigByHash( configHash )
        handle.write( configString )
        handle.close() 
        print "Steps.Executors.CMSSW.pre called"
        return None


    def execute(self, step, wmbsJob, emulator = None):
        """
        _execute_


        """
        if (emulator != None):
            return emulator.emulate( step )
            

        # write the wrapper script to a temporary location
        # I don't pass it directly through os.system because I don't
        # trust that there won't be shell-escape shenanigans with
        # arbitrary input files
        scramCommand   = step.application.setup.scramCommand
        scramProject   = step.application.setup.scramProject
        cmsswVersion   = step.application.setup.cmsswVersion
        jobReportXML   = step.output.jobReport
        cmsswCommand   = step.application.command.executable
        cmsswConfig    = step.application.command.configuration
        cmsswArguments = step.application.command.arguments
        
        (handle, configPath) = tempfile.mkstemp('.sh')
        os.write( handle, configBlob )
        os.close( handle )
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
        stdoutHandle = open( step.output.stdout , 'w')
        stderrHandle = open( step.output.stderr , 'w') 
        
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
        step.section_("execution")
        step.execution.exitStatus = spawnedChild.returncode
        
        print "Steps.Executors.CMSSW.execute called"


    def post(self, step, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        print "Steps.Executors.CMSSW.post called"
        if (emulator != None):
            return emulator.emulatePost( step )
        
        return None

configBlob = """#!/bin/bash

# Check to make sure the argument count is correct
REQUIRED_ARGUMENT_COUNT=5
if [ $# -lt $REQUIRED_ARGUMENT_COUNT ]
then
    echo "Usage: `basename $0` <SCRAM_COMMAND> <SCRAM_PROJECT> <CMSSW_VERSION> <JOB_REPORT> <EXECUTABLE> <CONFIG> [Arguments for cmsRun]"
    exit 1
fi

# Extract the required arguments out, leaving an unknown number of cmsRun arguments
# <SCRAM_COMMAND> <SCRAM_PROJECT> <CMSSW_VERSION> <JOB_REPORT> <EXECUTABLE> <CONFIG>
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
cd $CMSSW_VERSION
eval `$SCRAM_COMMAND runtime -sh`
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
