#!/usr/bin/env python
#pylint: disable-msg=W1201
# W1201: Allow string formatting in logging messages
"""
_Step.Executor.CMSSW_

Implementation of an Executor for a CMSSW step.
"""

import os
import subprocess
import sys
import select
import logging
import time

from WMCore.FwkJobReport.Report import addAttributesToFile, addFiles
from WMCore.WMSpec.Steps.Executor import Executor
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure
from WMCore.WMRuntime.Tools.Scram import Scram
from WMCore.WMSpec.WMStep import WMStepHelper

from WMCore.FwkJobReport.MulticoreReader import readMultiJobReports
import WMCore.FwkJobReport.MulticoreUtils as ReportUtils
from WMCore.WMRuntime.MergeBucket import MergeBucket


def analysisFileLFN(fileName, lfnBase, job):
    """
    Construct an LFN for a user file
    """

    junk, base = os.path.split(fileName)
    root, ext = os.path.splitext(base)

    newBase = '{base}_{count:04d}{ext}'.format(base=root, ext=ext, count=job['counter'])
    lfn = os.path.join(lfnBase, job['workflow'], 'output', newBase)

    return lfn

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
        logging.info("Pre-executing CMSSW step")
        if hasattr(self.step.application.configuration, 'configCacheUrl'):
            # means we have a configuration & tweak in the sandbox
            psetFile = self.step.application.command.configuration
            psetTweak = self.step.application.command.psetTweak
            self.stepSpace.getFromSandbox(psetFile)

            if psetTweak:
                self.stepSpace.getFromSandbox(psetTweak)

        if hasattr(self.step, "pileup"):
            self.stepSpace.getFromSandbox("pileupconf.json")

        # if step is multicore Enabled, add the multicore setup
        # that generates the input file manifest and per process splitting
        stepHelper = WMStepHelper(self.step)
        typeHelper = stepHelper.getTypeHelper()
        if typeHelper.multicoreEnabled():
            self.step.runtime.scramPreScripts.append("SetupCMSSWMulticore")

        # add in ths scram env PSet manip script whatever happens
        self.step.runtime.scramPreScripts.append("SetupCMSSWPset")
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
        scramSetup     = self.step.application.setup.softwareEnvironment
        scramCommand   = self.step.application.setup.scramCommand
        scramProject   = self.step.application.setup.scramProject
        scramArch      = self.step.application.setup.scramArch
        cmsswVersion   = self.step.application.setup.cmsswVersion
        jobReportXML   = self.step.output.jobReport
        cmsswCommand   = self.step.application.command.executable
        cmsswConfig    = self.step.application.command.configuration
        cmsswArguments = self.step.application.command.arguments
        userTarball    = ','.join(self.step.user.inputSandboxes)
        userFiles      = ','.join(self.step.user.userFiles)
        logging.info('User files are %s' % userFiles)
        logging.info('User sandboxes are %s' % userTarball)



        multicoreSettings = self.step.application.multicore
        multicoreEnabled  = multicoreSettings.enabled
        if multicoreEnabled:
            numberOfCores = multicoreSettings.numberOfCores
            logging.info("***Multicore CMSSW Enabled***")
            logging.info("Multicore configured for %s cores" % numberOfCores)
            #create input file list used to generate manifest
            filelist = open(multicoreSettings.inputfilelist,'w')
            for inputFile in self.job['input_files']:
                filelist.write("%s\n" % inputFile['lfn'])
            filelist.close()


        logging.info("Executing CMSSW step")

        #
        # set any global environment variables
        #
        try:
            os.environ['FRONTIER_ID'] = 'wmagent_%i_%s_%s_%s' % (self.job['id'], self.task.name(),
                                                                 self.job['retry_count'],
                                                                 cmsswVersion)
        except Exception, ex:
            logging.error('Have critical error in setting FRONTIER_ID: %s' % str(ex))
            logging.error('Continuing, as this is not a critical function yet.')
            pass

        #
        # scram bootstrap
        #
        scram = Scram(
            command = scramCommand,
            version = cmsswVersion,
            initialise = self.step.application.setup.softwareEnvironment,
            directory = self.step.builder.workingDir,
            architecture = scramArch,
            )

        logging.info("Runing SCRAM")
        try:
            projectOutcome = scram.project()
        except Exception, ex:
            msg =  "Exception raised while running scram.\n"
            msg += str(ex)
            logging.critical("Error running SCRAM")
            logging.critical(msg)
            raise WMExecutionFailure(50513, "ScramSetupFailure", msg)

        if projectOutcome > 0:
            msg = scram.diagnostic()
            #self.report.addError(60513, "ScramSetupFailure", msg)
            logging.critical("Error running SCRAM")
            logging.critical(msg)
            raise WMExecutionFailure(50513, "ScramSetupFailure", msg)
        runtimeOutcome = scram.runtime()
        if runtimeOutcome > 0:
            msg = scram.diagnostic()
            #self.report.addError(60513, "ScramSetupFailure", msg)
            logging.critical("Error running SCRAM")
            logging.critical(msg)
            raise WMExecutionFailure(50513, "ScramSetupFailure", msg)


        #
        # pre scripts
        #
        logging.info("Running PRE scripts")
        for script in self.step.runtime.preScripts:
            # TODO: Exception handling and error handling & logging
            scriptProcess = subprocess.Popen(
                ["/bin/bash"], shell=True, cwd=self.step.builder.workingDir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                )
            # BADPYTHON
            scriptProcess.stdin.write("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib\n")
            invokeCommand = "%s -m WMCore.WMRuntime.ScriptInvoke %s %s \n" % (
                sys.executable,
                stepModule,
                script)
            logging.info("    Invoking command: %s" % invokeCommand)
            scriptProcess.stdin.write(invokeCommand)
            stdout, stderr = scriptProcess.communicate()
            retCode = scriptProcess.returncode
            if retCode > 0:
                msg = "Error running command\n%s\n" % invokeCommand
                msg += "%s\n %s\n %s\n" % (retCode, stdout, stderr)
                logging.critical("Error running command")
                logging.critical(msg)
                raise WMExecutionFailure(60514, "PreScriptFailure", msg)


        #
        # pre scripts with scram
        #
        logging.info("RUNNING SCRAM SCRIPTS")
        for script in self.step.runtime.scramPreScripts:
            #invoke scripts with scram()
            invokeCommand = "%s -m WMCore.WMRuntime.ScriptInvoke %s %s \n" % (
                sys.executable,
                stepModule,
                script)
            logging.info("    Invoking command: %s" % invokeCommand)
            retCode = scram(invokeCommand)
            if retCode > 0:
                msg = "Error running command\n%s\n" % invokeCommand
                msg += "%s\n " % retCode
                msg += scram.diagnostic()
                logging.critical(msg)
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
        applicationStart = time.time()
        args = ['/bin/bash', configPath, scramSetup,
                                         scramArch,
                                         scramCommand,
                                         scramProject,
                                         cmsswVersion,
                                         jobReportXML,
                                         cmsswCommand,
                                         cmsswConfig,
                                         userTarball,
                                         userFiles,
                                         cmsswArguments]
        logging.info("Executing CMSSW. args: %s" % args)
        spawnedChild = subprocess.Popen( args, 0, None, None, stdoutHandle,
                                             stderrHandle )
        #(stdoutData, stderrData) = spawnedChild.communicate()
        # the above line replaces the bottom block. I'm unsure of why
        # nobody used communicate(), but I'm leaving this just in case
        # AMM Jul 4th, /2010
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
            msg += "Return code: %s\n" % spawnedChild.returncode
            logging.critical(msg)
            raise WMExecutionFailure(spawnedChild.returncode,
                                     "CmsRunFailure", msg)

        try:
            self.report.parse(jobReportXML, stepName = self.stepName)
        except Exception, ex:
            # Catch it if something goes wrong
            raise WMExecutionFailure(50115, "BadJobReportXML", str(ex))


        #
        # If multicore is enabled, merged the output files and reports
        #
        if multicoreEnabled:
            self.multicoreMerge(scram,applicationStart)

        stepHelper = WMStepHelper(self.step)
        typeHelper = stepHelper.getTypeHelper()

        acquisitionEra = self.task.getAcquisitionEra()
        processingVer  = self.task.getProcessingVersion()
        processingStr  = self.task.getProcessingString()
        validStatus    = self.workload.getValidStatus()
        inputPath      = self.task.getInputDatasetPath()
        globalTag      = typeHelper.getGlobalTag()
        custodialSite  = self.workload.getCustodialSite()
        cacheUrl, cacheDB, configID = stepHelper.getConfigInfo()

        self.report.setValidStatus(validStatus = validStatus)
        self.report.setGlobalTag(globalTag = globalTag)
        self.report.setInputDataset(inputPath = inputPath)
        self.report.setCustodialSite(custodialSite = custodialSite)
        self.report.setAcquisitionProcessing(acquisitionEra = acquisitionEra,
                                             processingVer = processingVer,
                                             processingStr = processingStr)
        self.report.setConfigURL(configURL = "%s;;%s;;%s" % (cacheUrl,
                                                             cacheDB,
                                                             configID))

        # Attach info to files
        self.report.addInfoToOutputFilesForStep(stepName = self.stepName, step = self.step)

        self.report.checkForAdlerChecksum(stepName = self.stepName)

        if self.step.output.keep != True:
            self.report.killOutput()
        else:
            #Check that we only keep the desired output
            for module in stepHelper.getIgnoredOutputModules():
                self.report.deleteOutputModuleForStep(stepName = self.stepName, moduleName = module)

        # Add stageout LFN to existing TFileService files
        reportAnalysisFiles = self.report.getAnalysisFilesFromStep(self.stepName)
        for reportAnalysisFile in reportAnalysisFiles:
            newLFN = analysisFileLFN(reportAnalysisFile.fileName, self.step.user.lfnBase, self.job)
            addAttributesToFile(reportAnalysisFile, pfn=reportAnalysisFile.fileName, lfn=newLFN, validate=False)

        # Add analysis file entries for additional files listed in workflow
        for fileName in stepHelper.listAnalysisFiles():
            analysisFile = stepHelper.getAnalysisFile(fileName)
            if os.path.isfile(analysisFile.fileName):
                newLFN = analysisFileLFN(analysisFile.fileName, analysisFile.lfnBase, self.job)
                self.report.addAnalysisFile(analysisFile.fileName, lfn=newLFN, Source='UserDefined',
                                            pfn=os.path.join(os.getcwd(), analysisFile.fileName), validate=False)

        return

    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        logging.info("Steps.Executors.CMSSW.post called")

        if (emulator != None):
            return emulator.emulatePost( self.step )

        if self.report.getStepErrors(self.stepName) != {}:
            # Then we had errors
            # Go directly to spot specified in WMStep
            return self.errorDestination

        return None

    def multicoreMerge(self, scramRef, applicationStart):
        """
        If this step is a multicore configured step, the output from the subprocesses needs to be merged
        together and the job report updated.

        """
        logging.info("***Multicore Post processing started***")
        #  //
        # // Now go through the reports and merge the outputs
        #//
        outputReports = readMultiJobReports( self.step.output.jobReport , self.stepName, self.step.builder.workingDir)
        self.report = None
        mergeBuckets = {}
        inputFiles = {}
        aggregator = ReportUtils.Aggregator()
        #  //
        # // loop over the sub reports and combine the inputs for recording and outputs for merges
        #//
        for o in outputReports:
            # use one of the reports as the template for the master report
            if self.report == None:
                self.report = o
            aggregator.add(o.report.performance)
            # store list of all unique input files into the job, adding them together if they
            # are fragments of the same file
            for inp in o.getAllInputFiles():
                lfn = inp['lfn']
                if not inputFiles.has_key(lfn):
                    inputFiles[lfn] = inp
                else:
                    addFiles(inputFiles[lfn], inp)
            #  //
            # // process the output modules from each subreport
            #//
            for f in o.getAllFiles():
                lfn = f['outputModule']
                if not mergeBuckets.has_key(lfn):
                    mergeBuckets[lfn] = MergeBucket(f['lfn'], f['outputModule'], self.stepName, self.step.builder.workingDir)
                mergeBuckets[lfn].append(f)

        # clean up the master report from the template
        reportData = getattr(self.report.data, self.stepName)
        for src in reportData.input.listSections_():
            delattr(reportData.input, src)
        for omod in reportData.outputModules:
            delattr(reportData.output, omod)
            reportData.outputModules.remove(omod)

        #  //
        # // Add the reduced list of input files to the master report
        #//
        for f in inputFiles.values():
            self.report.addInputFile(f['module_label'], **f)

        #  //
        # // Now roll through the merges, run the merge and edit the master job report with the outcome
        #//
        mergesStart = time.time()
        mergeTiming = []
        for b in mergeBuckets.values():
            # write the merge config file
            thisMergeStarts = time.time()
            b.writeConfig()
            # run the merge as a scram enabled command
            logging.info("    Invoking command: %s" % b.merge_command)
            logfile = "%s.log" % b.merge_pset_file
            retCode = scramRef(b.merge_command, False, logfile, self.step.builder.workingDir)
            if retCode > 0:
                msg = "Error running merge job:\n%s\n" % b.merge_command
                msg += "Merge Config:\n%s\n" % b.mergeConfig()
                msg += "%s\n " % retCode
                msg += scram.diagnostic()
                logging.critical(msg)
                raise WMExecutionFailure(60666, "MulticoreMergeFailure", msg)
            #  //
            # // add report from merge to master
            #//  ToDo: try/except here in case report is missing
            b.editReport(self.report)
            thisMergeTime = time.time() - thisMergeStarts
            mergeTiming.append(thisMergeTime)
        mergesComplete = time.time()
        totalJobTime = mergesComplete - applicationStart
        self.report.report.performance = aggregator.aggregate()
        ReportUtils.updateMulticoreReport(self.report, len(mergeBuckets), mergesStart , mergesComplete,
                                          totalJobTime , *mergeTiming)

        logging.info("***Multicore Post processing complete***")
        return




configBlob = """#!/bin/bash

# Check to make sure the argument count is correct
REQUIRED_ARGUMENT_COUNT=5
if [ $# -lt $REQUIRED_ARGUMENT_COUNT ]
then
    echo "Usage: `basename $0` <SCRAM_SETUP>  <SCRAM_ARCH> <SCRAM_COMMAND> <SCRAM_PROJECT> <CMSSW_VERSION>\
                 <JOB_REPORT> <EXECUTABLE> <CONFIG> <USER_TARBALLS> <USER_FILES> [Arguments for cmsRun]"
    exit 70
fi

# Extract the required arguments out, leaving an unknown number of
#  cmsRun arguments
SCRAM_SETUP=$1
SCRAM_ARCHIT=$2
SCRAM_COMMAND=$3
SCRAM_PROJECT=$4
CMSSW_VERSION=$5
JOB_REPORT=$6
EXECUTABLE=$7
CONFIGURATION=$8
USER_TARBALL=$9
shift;shift;shift;shift;shift;
shift;shift;shift;shift;
# Can only do nine parameters at a time
USER_FILES=$1
shift;
echo "Beginning CMSSW wrapper script"
echo "$SCRAM_SETUP $SCRAM_ARCHIT $SCRAM_COMMAND $SCRAM_PROJECT"

echo "Performing SCRAM setup..."
$SCRAM_SETUP
echo "Completed SCRAM setup"

export SCRAM_ARCH=$SCRAM_ARCHIT

echo "Retrieving SCRAM project..."
# do the actual executing
$SCRAM_COMMAND project $SCRAM_PROJECT $CMSSW_VERSION
if [ $? -ne 0 ]; then echo "Scram failed"; exit 71; fi
cd $CMSSW_VERSION
if [ $? -ne 0 ]; then echo "***\nCouldn't chdir: $?\n"; exit 72; fi

eval `$SCRAM_COMMAND runtime -sh`
if [ $? -ne 0 ]; then echo "***\nCouldn't get scram runtime: $?\n*"; exit 73; fi
if [ -n "$USER_TARBALL" ] ; then
    python2.6 -m WMCore.WMRuntime.Scripts.UnpackUserTarball $USER_TARBALL $USER_FILES
    if [ $? -ne 0 ]; then echo "***\nCouldn't untar sandbox: $?\n"; exit 74; fi
fi
echo "Completed SCRAM project"
cd ..
echo "Executing CMSSW"
echo "$EXECUTABLE  -j $JOB_REPORT $CONFIGURATION"
$EXECUTABLE  -j $JOB_REPORT $CONFIGURATION 2>&1 &
PROCID=$!
echo $PROCID > process.id
wait $PROCID
EXIT_STATUS=$?
echo "Complete"
echo "process id is $PROCID status is $EXIT_STATUS"
exit $EXIT_STATUS

"""
