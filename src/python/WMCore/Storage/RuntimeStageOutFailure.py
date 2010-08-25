#!/usr/bin/env python
"""
_RuntimeStageOutFailure_

Tool that gets invoked after a failure of the stage out script
to check that the failure is reported in the FrameworkJobReport.xml
for the job

"""

import os
import sys
import time


from WMCore.Storage.TaskState import TaskState, getTaskState
from WMCore.Storage.MergeReports import updateReport
import StageOut.Utilities as StageOutUtils
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

if __name__ == '__main__':
    msg = "******RuntimeStageOutFailure Invoked*****"

    state = TaskState(os.getcwd())
    state.loadRunResDB()
    config = state.configurationDict()

    #  //
    # // find inputs by locating the task for which we are staging out
    #//  and loading its TaskState

    workflow = WorkflowSpec()
    workflow.load(os.environ['PRODAGENT_WORKFLOW_SPEC'])
    stageOutFor, override, controls = StageOutUtils.getStageOutConfig(
        workflow, state.taskName())

    inputTasks = stageOutFor
    for inputTask in inputTasks:

        inputState = getTaskState(inputTask)
        if inputState == None:
            msg = "Input State: %s Not found, skipping..." % inputTask
            continue

        inputReport = inputState.getJobReport()

        inputReport.status = "Failed"
        if inputReport.exitCode in (0, "0"):
            #  //
            # // Non zero implies that a failure has already been reported.
            #//
            inputReport.exitCode = 60314
        #  //
        # // Add a report to log this in the job report.
        #//
        errRep = inputReport.addError(
            60314, "RuntimeStageOutFailed")
        errRep['Description'] = \
          """Unable to invoke RuntimeStageOut.py or it
             crashed with a non-zero exit status"""


        inputState.saveJobReport()
        reportToUpdate = inputState.getJobReport()

        toplevelReport = os.path.join(os.environ['PRODAGENT_JOB_DIR'],
                                      "FrameworkJobReport.xml")


        updateReport(toplevelReport, reportToUpdate)
    sys.exit(0)
