#!/usr/bin/env python
"""
_Step.Executor.StageOut_

Implementation of an Executor for a StageOut step

"""
from __future__ import print_function

import logging
import os
import os.path
import signal
import sys

from WMCore.Algorithms.Alarm import Alarm, alarmHandler
from WMCore.FwkJobReport.Report import Report
from WMCore.Lexicon import lfn     as lfnRegEx
from WMCore.Lexicon import userLfn as userLfnRegEx
from WMCore.Storage.FileManager import StageOutMgr as FMStageOutMgr
from WMCore.Storage.StageOutMgr import StageOutMgr
from WMCore.WMSpec.Steps.Executor import Executor


class StageOut(Executor):
    """
    _StageOut_

    Execute a StageOut Step

    """

    def pre(self, emulator=None):
        """
        _pre_

        Pre execution checks

        """

        # Are we using an emulator?
        if emulator is not None:
            return emulator.emulatePre(self.step)

        logging.info("Steps.Executors.%s.pre called", self.__class__.__name__)
        return None

    def execute(self, emulator=None):
        """
        _execute_


        """
        # Are we using emulators again?
        if emulator is not None:
            return emulator.emulate(self.step, self.job)

        logging.info("Steps.Executors.%s.execute called", self.__class__.__name__)

        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()
        # propagete upstream cmsRun outcome such that we can decide whether to
        # stage files out or not
        self.failedPreviousStep = overrides.get('previousCmsRunFailure', False)

        # Set wait to two hours per retry
        # this alarm leaves a subprocess behing that may cause trouble, see #6273
        waitTime = overrides.get('waitTime', 7200 * self.step.retryCount)

        logging.info("StageOut override is: %s ", self.step)

        # Pull out StageOutMgr Overrides

        # switch between old stageOut behavior and new, fancy stage out behavior
        useNewStageOutCode = False
        if getattr(self.step, 'newStageout', False) or \
                ('newStageOut' in overrides and overrides.get('newStageOut')):
            useNewStageOutCode = True

        stageOutCall = {}
        if "command" in overrides and "option" in overrides \
                and "phedex-node" in overrides \
                and "lfn-prefix" in overrides:
            logging.critical('using override in StageOut')
            stageOutCall['command'] = overrides.get('command')
            stageOutCall['option'] = overrides.get('option')
            stageOutCall['phedex-node'] = overrides.get('phedex-node')
            stageOutCall['lfn-prefix'] = overrides.get('lfn-prefix')

        # naw man, this is real
        # iterate over all the incoming files
        if not useNewStageOutCode:
            # old style
            manager = StageOutMgr(**stageOutCall)
            manager.numberOfRetries = self.step.retryCount
            manager.retryPauseTime = self.step.retryDelay
        else:
            # new style
            logging.critical("STAGEOUT IS USING NEW STAGEOUT CODE")
            manager = FMStageOutMgr(retryPauseTime=self.step.retryDelay,
                                    numberOfRetries=self.step.retryCount,
                                    **stageOutCall)

        # We need to find a list of steps in our task
        # And eventually a list of jobReports for out steps

        # Search through steps for report files
        filesTransferred = []

        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                # Don't try to parse your own report; it's not there yet
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s", step)
            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s", step, stepLocation)
                continue
            # First, get everything from a file and 'unpersist' it
            stepReport = Report()
            stepReport.unpersist(reportLocation, step)

            # Don't stage out files from bad steps. Each step has its own Report.pkl file
            # We need to check all steps executed so far, otherwise it might stage out
            # files for chained steps when the overall job has already failed to process
            # one of them
            if not stepReport.stepSuccessful(step) or self.failedPreviousStep:
                msg = "Either the step did not succeed or an upstream step failed. "
                msg += "Skipping stage out of any root output files in this job."
                logging.warning(msg)
                continue

            # Okay, time to start using stuff
            # Now I'm a bit confused about this; each report should ONLY
            # Have the results of that particular step in it,
            # So getting all the files should get ONLY the files
            # for that step; or so I hope
            files = stepReport.getAllFileRefsFromStep(step=step)
            for fileName in files:

                # make sure the file information is consistent
                if hasattr(fileName, 'pfn') and (not hasattr(fileName, 'lfn') or not hasattr(fileName, 'module_label')):
                    msg = "Not a valid file: %s" % fileName
                    logging.error(msg)
                    continue

                # Figuring out if we should do straight to merge
                #  - should we do straight to merge at all ?
                #  - is straight to merge disabled for this output ?
                #  - are we over the size threshold
                #  - are we over the event threshold ?
                straightToMerge = False
                if not getattr(fileName, 'merged', False):
                    if hasattr(fileName, 'dataset') and fileName.dataset.get('dataTier', "") in ["NANOAOD",
                                                                                                 "NANOAODSIM"]:
                        logging.info("NANOAOD and NANOAODSIM files never go straight to merge!")
                    elif fileName.module_label not in getattr(self.step.output, 'forceUnmergedOutputs', []):
                        if hasattr(self.step.output, 'minMergeSize') and getattr(fileName, 'size',
                                                                                 0) >= self.step.output.minMergeSize:
                            logging.info("Sending %s straight to merge due to minMergeSize", fileName.lfn)
                            straightToMerge = True
                        elif getattr(fileName, 'events', 0) >= getattr(self.step.output, 'maxMergeEvents', sys.maxsize):
                            logging.info("Sending %s straight to merge due to maxMergeEvents", fileName.lfn)
                            straightToMerge = True

                if straightToMerge:
                    try:
                        fileName = self.handleLFNForMerge(mergefile=fileName, step=step)
                    except Exception as ex:
                        logging.info("minMergeSize: %s", getattr(self.step.output, 'minMergeSize', None))
                        logging.info("maxMergeEvents: %s", getattr(self.step.output, 'maxMergeEvents', None))
                        logging.error("Encountered error while handling LFN for merge %s", fileName)
                        logging.error(str(ex))
                        manager.cleanSuccessfulStageOuts()
                        stepReport.addError(self.stepName, 60401, "DirectToMergeFailure", str(ex))

                # Save the input PFN in case we need it
                # Undecided whether to move fileName.pfn to the output PFN
                fileName.InputPFN = fileName.pfn
                lfn = fileName.lfn
                fileSource = getattr(fileName, 'Source', None)
                if fileSource in ['TFileService', 'UserDefined']:
                    userLfnRegEx(lfn)
                else:
                    lfnRegEx(lfn)

                fileForTransfer = {'LFN': lfn,
                                   'PFN': getattr(fileName, 'pfn'),
                                   'PNN': None,
                                   'StageOutCommand': None,
                                   'Checksums': getattr(fileName, 'checksums', None)}

                signal.signal(signal.SIGALRM, alarmHandler)
                signal.alarm(waitTime)
                try:
                    manager(fileForTransfer)
                    # Afterwards, the file should have updated info.
                    filesTransferred.append(fileForTransfer)
                    fileName.StageOutCommand = fileForTransfer['StageOutCommand']
                    fileName.location = fileForTransfer['PNN']
                    fileName.OutputPFN = fileForTransfer['PFN']
                except Alarm:
                    msg = "Indefinite hang during stageOut of logArchive"
                    logging.error(msg)
                    manager.cleanSuccessfulStageOuts()
                    stepReport.addError(self.stepName, 60403, "StageOutTimeout", msg)
                    # well, if it fails for one file, it fails for the whole job...
                    break
                except Exception as ex:
                    manager.cleanSuccessfulStageOuts()
                    stepReport.addError(self.stepName, 60307, "StageOutFailure", str(ex))
                    stepReport.persist(reportLocation)
                    raise

                signal.alarm(0)

            # Am DONE with report. Persist it
            stepReport.persist(reportLocation)

        # Done with all steps, and should have a list of
        # stagedOut files in fileForTransfer
        logging.info("Transferred %i files", len(filesTransferred))
        return

    def post(self, emulator=None):
        """
        _post_

        Post execution checkpointing

        """
        # Another emulator check
        if emulator is not None:
            return emulator.emulatePost(self.step)

        logging.info("Steps.Executors.%s.post called", self.__class__.__name__)

        for step in self.stepSpace.taskSpace.stepSpaces():

            if step == self.stepName:
                # Don't try to parse your own report; it's not there yet
                continue

            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s", step)

            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s", step, stepLocation)
                continue

            # First, get everything from a file and 'unpersist' it
            stepReport = Report(step)
            stepReport.unpersist(reportLocation)

            # Don't stage out files from bad steps.
            if not stepReport.stepSuccessful(step):
                continue

            files = stepReport.getAllFileRefsFromStep(step=step)
            for fileInfo in files:
                if hasattr(fileInfo, 'lfn') and hasattr(fileInfo, 'location') and hasattr(fileInfo, 'guid'):
                    fileInfo.user_dn = getattr(self.step, "userDN", None)
                    fileInfo.async_dest = getattr(self.step, "asyncDest", None)
                    fileInfo.user_vogroup = getattr(self.step, "owner_vogroup", '')
                    fileInfo.user_vorole = getattr(self.step, "owner_vorole", '')

            stepReport.persist(reportLocation)

        return None

    # Accessory methods
    def handleLFNForMerge(self, mergefile, step):
        """
        _handleLFNForMerge_

        Digs up unmerged LFN out of WMStep outputModule and
        changes the current file to match.
        Requires a mergedLFNBase in the WMSpec output module
        """

        # First get the output module
        # Do this by finding the name in the step report
        # And then finding that module in the WMStep Helper

        outputName = getattr(mergefile, 'module_label', None)
        if not outputName:
            logging.error("Attempt to merge directly failed due to " \
                          + "No module_label in file.")
        if outputName.lower() == "merged":
            # Don't skip merge for merged files!
            return mergefile
        stepHelper = self.task.getStep(stepName=step)
        outputMod = stepHelper.getOutputModule(moduleName=outputName)

        if not outputMod:
            # Then we couldn't get the output module
            logging.error("Attempt to directly merge failed " \
                          + "due to no output module %s in WMStep" \
                          % (outputName))
            return mergefile

        # Okay, now we should have the output Module
        # Now we just need a second LFN

        newBase = getattr(outputMod, 'mergedLFNBase', None)
        oldBase = getattr(outputMod, 'lfnBase', None)

        if not newBase:
            # Then we don't have a base to change it to
            logging.error("Direct to Merge failed due to no mergedLFNBase in %s", outputName)
            return mergefile

        # Replace the actual LFN base
        oldLFN = getattr(mergefile, 'lfn')
        newLFN = oldLFN.replace(oldBase, newBase)

        # Set the file attributes
        setattr(mergefile, 'lfn', newLFN)
        setattr(mergefile, 'merged', True)

        # Return the file
        return mergefile
