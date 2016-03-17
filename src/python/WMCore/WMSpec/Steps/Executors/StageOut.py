#!/usr/bin/env python
"""
_Step.Executor.StageOut_

Implementation of an Executor for a StageOut step

"""
from __future__ import print_function

import os
import os.path
import logging
import signal

from WMCore.WMSpec.Steps.Executor           import Executor
from WMCore.FwkJobReport.Report             import Report

from WMCore.Storage.StageOutMgr import StageOutMgr
from WMCore.Storage.FileManager import StageOutMgr as FMStageOutMgr

from WMCore.Lexicon                  import lfn     as lfnRegEx
from WMCore.Lexicon                  import userLfn as userLfnRegEx

from WMCore.Algorithms.Alarm import Alarm, alarmHandler

class StageOut(Executor):
    """
    _StageOut_

    Execute a StageOut Step

    """

    def pre(self, emulator = None):
        """
        _pre_

        Pre execution checks

        """

        #Are we using an emulator?
        if (emulator != None):
            return emulator.emulatePre( self.step )



        print("Steps.Executors.StageOut.pre called")
        return None


    def execute(self, emulator = None):
        """
        _execute_


        """
        #Are we using emulators again?
        if (emulator != None):
            return emulator.emulate( self.step, self.job )


        overrides = {}
        if hasattr(self.step, 'override'):
            overrides = self.step.override.dictionary_()

        # Set wait to two hours per retry
        # this alarm leaves a subprocess behing that may cause trouble, see #6273
        waitTime = overrides.get('waitTime', 7200 * self.step.retryCount)

        logging.info("StageOut override is: %s " % self.step)

        # Pull out StageOutMgr Overrides

        # switch between old stageOut behavior and new, fancy stage out behavior
        useNewStageOutCode = False
        if getattr(self.step, 'newStageout', False) or \
            ('newStageOut' in overrides and overrides.get('newStageOut')):
            useNewStageOutCode = True


        stageOutCall = {}
        if "command" in overrides and "option" in overrides \
               and "se-name" in overrides and "phedex-node" in overrides \
               and"lfn-prefix" in overrides:
            logging.critical('using override in StageOut')
            stageOutCall['command']    = overrides.get('command')
            stageOutCall['option']     = overrides.get('option')
            stageOutCall['se-name']    = overrides.get('se-name')
            stageOutCall['phedex-node']= overrides.get('phedex-node')
            stageOutCall['lfn-prefix'] = overrides.get('lfn-prefix')

        # naw man, this is real
        # iterate over all the incoming files
        if not useNewStageOutCode:
            # old style
            manager = StageOutMgr(**stageOutCall)
            manager.numberOfRetries = self.step.retryCount
            manager.retryPauseTime  = self.step.retryDelay
        else:
            # new style
            logging.critical("STAGEOUT IS USING NEW STAGEOUT CODE")
            print("STAGEOUT IS USING NEW STAGEOUT CODE")
            manager = FMStageOutMgr(retryPauseTime  = self.step.retryDelay,
                                    numberOfRetries = self.step.retryCount,
                                    **stageOutCall)

        # We need to find a list of steps in our task
        # And eventually a list of jobReports for out steps

        # Search through steps for report files
        filesTransferred = []

        for step in self.stepSpace.taskSpace.stepSpaces():
            if step == self.stepName:
                #Don't try to parse your own report; it's not there yet
                continue
            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s" % (step))
            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s" \
                              % (step, stepLocation))
                continue
            # First, get everything from a file and 'unpersist' it
            stepReport = Report()
            stepReport.unpersist(reportLocation, step)

            # Don't stage out files from bad steps.
            if not stepReport.stepSuccessful(step):
                continue

            # Okay, time to start using stuff
            # Now I'm a bit confused about this; each report should ONLY
            # Have the results of that particular step in it,
            # So getting all the files should get ONLY the files
            # for that step; or so I hope
            files = stepReport.getAllFileRefsFromStep(step = step)
            for fileName in files:
                if not hasattr(fileName, 'lfn') and hasattr(fileName, 'pfn'):
                    # Then we're truly hosed on this file; ignore it
                    msg = "Not a file: %s" % fileName
                    logging.error(msg)
                    continue
                # Support direct-to-merge
                # This requires pulling a bunch of stuff from everywhere
                # First check if it's needed
                if hasattr(self.step.output, 'minMergeSize') and hasattr(fileName, 'size') \
                    and not getattr(fileName, 'merged', False):

                    # We need both of those to continue, and we don't
                    # direct-to-merge
                    if getattr(self.step.output, 'doNotDirectMerge', False):
                        # Then we've been told explicitly not to do direct-to-merge
                        continue
                    if fileName.size >= self.step.output.minMergeSize:
                        # Then this goes direct to merge
                        try:
                            fileName = self.handleLFNForMerge(mergefile = fileName, step = step)
                        except Exception as ex:
                            logging.error("Encountered error while handling LFN for merge due to size.\n")
                            logging.error(str(ex))
                            logging.debug(fileName)
                            logging.debug("minMergeSize: %s" % self.step.output.minMergeSize)
                            manager.cleanSuccessfulStageOuts()
                            stepReport.addError(self.stepName, 60401, "DirectToMergeFailure", str(ex))
                    elif getattr(self.step.output, 'maxMergeEvents', None) != None \
                        and getattr(fileName, 'events', None) != None and not getattr(fileName, 'merged', False):
                        # Then direct-to-merge due to events if
                        # the file is large enough:
                        if fileName.events >= self.step.output.maxMergeEvents:
                            # straight to merge
                            try:
                                fileName = self.handleLFNForMerge(mergefile = fileName, step = step)
                            except Exception as ex:
                                logging.error("Encountered error while handling LFN for merge due to events.\n")
                                logging.error(str(ex))
                                logging.debug(fileName)
                                logging.debug("maxMergeEvents: %s" % self.step.output.maxMergeEvents)
                                manager.cleanSuccessfulStageOuts()
                                stepReport.addError(self.stepName, 60402, "DirectToMergeFailure", str(ex))

                # Save the input PFN in case we need it
                # Undecided whether to move fileName.pfn to the output PFN
                fileName.InputPFN = fileName.pfn
                lfn = getattr(fileName, 'lfn')
                fileSource = getattr(fileName, 'Source', None)
                if fileSource in ['TFileService', 'UserDefined']:
                    userLfnRegEx(lfn)
                else:
                    lfnRegEx(lfn)
                fileForTransfer = {'LFN': lfn,
                                   'PFN': getattr(fileName, 'pfn'),
                                   'SEName' : None,
                                   'PNN' : None,
                                   'StageOutCommand': None,
                                   'Checksums' : getattr(fileName, 'checksums', None)}

                signal.signal(signal.SIGALRM, alarmHandler)
                signal.alarm(waitTime)
                try:
                    manager(fileForTransfer)
                    #Afterwards, the file should have updated info.
                    filesTransferred.append(fileForTransfer)
                    fileName.StageOutCommand = fileForTransfer['StageOutCommand']
                    fileName.location        = fileForTransfer['PNN']
                    fileName.OutputPFN       = fileForTransfer['PFN']
                except Alarm:
                    msg = "Indefinite hang during stageOut of logArchive"
                    logging.error(msg)
                    manager.cleanSuccessfulStageOuts()
                    stepReport.addError(self.stepName, 60403, "StageOutTimeout", msg)
                    stepReport.setStepStatus(self.stepName, 1)
                    # well, if it fails for one file, it fails for the whole job...
                    break
                except Exception as ex:
                    manager.cleanSuccessfulStageOuts()
                    stepReport.addError(self.stepName, 60307, "StageOutFailure", str(ex))
                    stepReport.setStepStatus(self.stepName, 1)
                    stepReport.persist(reportLocation)
                    raise

                signal.alarm(0)

            # Am DONE with report. Persist it
            stepReport.persist(reportLocation)

        #Done with all steps, and should have a list of
        #stagedOut files in fileForTransfer
        logging.info("Transferred %i files" %(len(filesTransferred)))
        return


    def post(self, emulator = None):
        """
        _post_

        Post execution checkpointing

        """
        #Another emulator check
        if (emulator != None):
            return emulator.emulatePost( self.step )

        for step in self.stepSpace.taskSpace.stepSpaces():

            if step == self.stepName:
                #Don't try to parse your own report; it's not there yet
                continue

            stepLocation = os.path.join(self.stepSpace.taskSpace.location, step)
            logging.info("Beginning report processing for step %s" % step)

            reportLocation = os.path.join(stepLocation, 'Report.pkl')
            if not os.path.isfile(reportLocation):
                logging.error("Cannot find report for step %s in space %s" \
                              % (step, stepLocation))
                continue

            # First, get everything from a file and 'unpersist' it
            stepReport = Report(step)
            stepReport.unpersist(reportLocation)

            # Don't stage out files from bad steps.
            if not stepReport.stepSuccessful(step):
                continue

            files = stepReport.getAllFileRefsFromStep(step = step)
            for file in files:

                if not hasattr(file, 'lfn') or not hasattr(file, 'location') or \
                       not hasattr(file, 'guid'):
                    continue

                file.user_dn = getattr(self.step, "userDN", None)
                file.async_dest = getattr(self.step, "asyncDest", None)
                file.user_vogroup = getattr(self.step, "owner_vogroup", '')
                file.user_vorole = getattr(self.step, "owner_vorole", '')

            stepReport.persist(reportLocation)

        print("Steps.Executors.StageOut.post called")
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
        stepHelper = self.task.getStep(stepName = step)
        outputMod  = stepHelper.getOutputModule(moduleName = outputName)

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
            logging.error("Direct to Merge failed due to no mergedLFNBase in %s" \
                          % (outputName))
            return mergefile

        # Replace the actual LFN base
        oldLFN = getattr(mergefile, 'lfn')
        newLFN = oldLFN.replace(oldBase, newBase)

        # Set the file attributes
        setattr(mergefile, 'lfn', newLFN)
        setattr(mergefile, 'merged', True)

        # Return the file
        return mergefile
