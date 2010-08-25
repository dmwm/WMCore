#!/usr/bin/env python
"""
_RuntimeStageOut_

Runtime script for managing a stage out job for files produced
by another node.

This script:

- Reads its own TaskState to get the name of the task for which it is
 doing the stage out

- Reads the TaskState of the input node that it has to stage out to get
  a list of files from the job report.

- For each file, it performs the stage out and updates the job report
  with the new PFN of the staged out file


"""

import os
import sys
import time



from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.Storage.StageOutMgr import StageOutMgr
from WMCore.Storage.StageOutMgr import StageOutSuccess
import StageOut.Utilities as StageOutUtils
import StageOut.Impl



from WMCore.Storage.TaskState import TaskState, getTaskState
from WMCore.Storage.MergeReports import updateReport
from WMCore.Storage.FwkJobReport import FwkJobReport
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec


completedFiles = []


class StageOutReport:
    """
    _StageOutReport_

    Object for handling stage out of a list of files contained in a job report

    """
    def __init__(self, inputReport, override, controls):
        self.inputReport = inputReport
        self.failed = {}
        self.succeeded = []
        self.manager = StageOutMgr(**override)
        if controls.has_key("NumberOfRetries"):
            self.manager.numberOfRetries = controls['NumberOfRetries']

        if controls.has_key("RetryPauseTime"):
            self.manager.retryPauseTime = controls['RetryPauseTime']




    def __call__(self):
        """
        _operator()_

        Use call to invoke transfers

        """
        # start timer
        stageOutStartTime = int(time.time())


        for fileToStage in self.inputReport.files:
            lfn = fileToStage['LFN']
            print "Staging Out: %s" % lfn
            try:
                result = self.manager(**fileToStage)
                fileToStage.update(result)
            except StageOutFailure, ex:

                if not self.failed.has_key(lfn):
                    self.failed[lfn] = []
                self.failed[lfn].append(ex)



        #  //
        # // Check for failures and update reports if there are any.
        #//
        exitCode = 0
        if len(self.failed.keys()) > 0:
            for lfn in self.failed.keys():
                for err in self.failed[lfn]:
                    self.reportStageOutFailure(err)
            self.inputReport.status = "Failed"
            self.inputReport.exitCode = 60312
            exitCode = 60312
            print "Initiating Cleanup of any succesful files"
            self.manager.cleanSuccessfulStageOuts()

        #  //
        # // Record StageOut Timing in job report
        #//
        self.inputReport.timing['StageOutStart'] = stageOutStartTime
        self.inputReport.timing['StageOutEnd'] = int(time.time() )
        return exitCode


    def reportStageOutFailure(self, stageOutExcep):
        """
        _reportStageOutFailure_

        When a stage out failure occurs, report it to the input
        framework job report.

        - *stageOutExcep* : Instance of on of the StageOutError derived classes

        """
        errStatus = stageOutExcep.data["ErrorCode"]
        errType = stageOutExcep.data["ErrorType"]
        desc = stageOutExcep.message

        errReport = self.inputReport.addError(errStatus, errType)
        errReport['Description'] = desc
        return




def stageOut():
    """
    _stageOut_

    Main function for this module. Loads data from the task
    and manages the stage out process for a single attempt

    """
    state = TaskState(os.getcwd())
    state.loadRunResDB()

    workflow = WorkflowSpec()
    workflow.load(os.environ['PRODAGENT_WORKFLOW_SPEC'])

    print workflow
    print state.taskName()

    stageOutFor, override, controls = StageOutUtils.getStageOutConfig(
        workflow, state.taskName())

    toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                  "FrameworkJobReport.xml")


    exitCode = 0
    #  //
    # // find inputs by locating the task for which we are staging out
    #//  and loading its TaskState
    for inputTask in stageOutFor:
        print "Attempting to stage out files for node %s" % inputTask
        try:
            inputState = getTaskState(inputTask)
            msg = "Loaded Input Task: %s " % inputTask
        except Exception, ex:
            msg = "Error load for TaskState for task %s" % inputTask
            msg += "%s\n" % str(ex)
            inputState = None
        print msg

        if inputState == None:
            # exit with init error
            # generate failure report in this dir, since cant find
            # input state dir
            inputReport = FwkJobReport()
            inputReport.name = inputTask
            exitCode = 60311
            errRep = inputReport.addError(
                60311, "TaskStateError")
            errRep['Description'] = msg
            inputReport.status = "Failed"
            inputReport.exitCode = 60311
            updateReport(toplevelReport, inputReport)
            print "TaskState is None, exiting..."
            return exitCode

        try:
            inputReport = inputState.getJobReport()
            msg = "Loaded JobReport for Task : %s\n" % inputTask
            msg += "File: %s\n" % inputState.jobReport
        except Exception, ex:
            msg = "Error loading input report : %s" % str(ex)
            inputReport = None

        print msg
        if inputReport == None:
            msg += "Unable to read Job Report for input task: %s\n" % inputTask
            msg += "Looked for file: %s\n" % inputState.jobReport
            print msg
            inputReport = FwkJobReport()
            inputReport.name = inputTask
            exitCode = 60311
            errRep = inputReport.addError(
                60311, "InputReportError")
            errRep['Description'] = msg
            inputReport.status = "Failed"
            inputReport.exitCode = 60311
            updateReport(toplevelReport, inputReport)
            # exit with init error
            return 60311



        try:
            manager = StageOutReport(inputReport, override, controls)
        except StageOutInitError, ex:
            exitCode = ex.data['ErrorCode']
            errRep = inputReport.addError(
                ex.data['ErrorCode'], ex.data['ErrorType'])
            errRep['Description'] = ex.message
            inputReport.status = "Failed"
            inputReport.exitCode = ex.data['ErrorCode']
            initmgr = False
        except Exception, ex:
            msg = "Unexpected Error instantiating StageOutMgr:\n"
            msg += str(ex)
            exitCode = 60311
            errRep = inputReport.addError(
                60311, "TaskStateError")
            errRep['Description'] = msg
            inputReport.status = "Failed"
            inputReport.exitCode = 60311
            updateReport(toplevelReport, inputReport)
            print "StageOutMgr init failed, exiting..."
            return exitCode

        try:
            exitCode = manager()
        except StageOutFailure, ex:
            exitCode = ex.data['ErrorCode']
            errRep = inputReport.addError(
                ex.data['ErrorCode'], ex.data['ErrorType'])
            errRep['Description'] = ex.message
            inputReport.status = "Failed"
            inputReport.exitCode = ex.data['ErrorCode']



        inputState.saveJobReport()
        reportToUpdate = inputState.getJobReport()
        #  //
        # // Update primary job report
        #//
        updateReport(toplevelReport, reportToUpdate)
        print "Stage Out for %s complete: Return code: %s " % \
                                                    (inputTask, exitCode)
    print "All StageOuts finished: Exiting %s" % exitCode
    return exitCode








if __name__ == '__main__':
    import StageOut.Impl
    exitCode = stageOut()
    f = open("exit.status", 'w')
    f.write(str(exitCode))
    f.close()
    sys.exit(exitCode)

