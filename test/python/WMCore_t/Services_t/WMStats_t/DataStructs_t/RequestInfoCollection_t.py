#!/usr/bin/env python
"""
Unittests for the RequestInfoCollection WMStats module
"""
from __future__ import division, print_function

import unittest

from future.utils import viewvalues

from Utils.PythonVersion import PY3

from WMCore.Services.WMStats.DataStruct.RequestInfoCollection import (JobSummary, ProgressSummary,
                                                                      TaskInfo, RequestInfo)


class DummyTask(object):
    """
    Dummy object carrying some needed attributes for TaskInfo
    """

    def __init__(self, reqName, taskName, taskType):
        self.requestName = reqName
        self.taskName = taskName
        self.taskType = taskType
        self.jobSummary = {}

    def setJobSummary(self, jobSum):
        self.jobSummary = jobSum


class MyTestCase(unittest.TestCase):

    def setUp(self):
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testJobSummary(self):
        """some very basic unit tests for the JobSummary class"""
        jobSumKeys = ["success", "canceled", "transition", "queued",
                      "submitted", "failure", "cooloff", "paused"]
        jsonStatusKeys = ["success", "failure", "cooloff", "running",
                          "queued", "pending", "paused", "created"]

        # default object
        jobSumObj = JobSummary()
        self.assertItemsEqual(list(jobSumObj.jobStatus), jobSumKeys)
        self.assertItemsEqual(list(jobSumObj.getJSONStatus()), jsonStatusKeys)
        self.assertEqual(jobSumObj.getTotalJobs(), 0)
        self.assertEqual(sum(viewvalues(jobSumObj.getJSONStatus())), 0)

        # update with no content, thus, do nothing!
        jobSumObj.addJobStatusInfo({})
        self.assertItemsEqual(list(jobSumObj.jobStatus), jobSumKeys)
        self.assertItemsEqual(list(jobSumObj.getJSONStatus()), jsonStatusKeys)
        self.assertEqual(jobSumObj.getTotalJobs(), 0)
        self.assertEqual(sum(viewvalues(jobSumObj.getJSONStatus())), 0)

        # passing an invalid key/status
        jobSumObj.addJobStatusInfo({"bad_status_key": 10})
        self.assertItemsEqual(list(jobSumObj.jobStatus), jobSumKeys)
        self.assertEqual(jobSumObj.getTotalJobs(), 0)

        # updating a simple integer status
        jobSumObj.addJobStatusInfo({"success": 10})
        self.assertItemsEqual(list(jobSumObj.jobStatus), jobSumKeys)
        self.assertEqual(jobSumObj.getTotalJobs(), 10)
        self.assertEqual(jobSumObj.getSuccess(), 10)
        # getJSONStatus considers success jobs as success and as created,
        # thus double counting
        self.assertEqual(sum(viewvalues(jobSumObj.getJSONStatus())), 20)

        # updating a dictionary status
        jobSumObj.addJobStatusInfo({"submitted": {"pending": 2, "running": 4}})
        self.assertItemsEqual(list(jobSumObj.jobStatus), jobSumKeys)
        self.assertItemsEqual(list(jobSumObj.getJSONStatus()), jsonStatusKeys)
        self.assertEqual(jobSumObj.getTotalJobs(), 16)
        self.assertEqual(jobSumObj.getSuccess(), 10)
        # Submitted only considers first/retry states
        self.assertEqual(jobSumObj.getSubmitted(), 0)
        self.assertEqual(jobSumObj.getPending(), 2)
        self.assertEqual(jobSumObj.getRunning(), 4)
        self.assertEqual(sum(viewvalues(jobSumObj.getJSONStatus())), 32)

    def testProgressSummary(self):
        """some very basic unit tests for the ProgressSummary class"""
        progSumKeys = ["totalLumis", "events", "size"]

        # default object
        progSumObj = ProgressSummary()
        self.assertItemsEqual(list(progSumObj.getReport()), progSumKeys)
        self.assertEqual(sum(viewvalues(progSumObj.getReport())), 0)

        # update with no content, thus, do nothing!
        progSumObj.addProgressReport({})
        self.assertItemsEqual(list(progSumObj.getReport()), progSumKeys)
        self.assertEqual(sum(viewvalues(progSumObj.getReport())), 0)

        # passing an invalid key/status
        progSumObj.addProgressReport({"bad_status_key": 10})
        self.assertItemsEqual(list(progSumObj.getReport()), progSumKeys)
        self.assertEqual(sum(viewvalues(progSumObj.getReport())), 0)

        # now passing some valid information
        progSumObj.addProgressReport({"totalLumis": 10, "events": 1000})
        self.assertItemsEqual(list(progSumObj.getReport()), progSumKeys)
        self.assertEqual(sum(viewvalues(progSumObj.getReport())), 1010)

    def testTaskInfo(self):
        """some very basic unit tests for the TaskInfo class"""
        reqName = "test_request_name"
        taskName = "test_task_name"

        # default object
        taskSumObj = TaskInfo(reqName, taskName, {})
        self.assertEqual(taskSumObj.getRequestName(), reqName)
        self.assertEqual(taskSumObj.getTaskName(), taskName)
        self.assertEqual(taskSumObj.getTaskType(), "N/A")
        self.assertIsInstance(taskSumObj.getJobSummary(), JobSummary)
        self.assertFalse(taskSumObj.isTaskCompleted())

        # try to add an invalid task info object
        with self.assertRaises(Exception):
            taskSumObj.addTaskInfo("blah")

        # and again, with valid attributes but invalid values
        with self.assertRaises(Exception):
            dummyTask = DummyTask("req", "task", "type")
            taskSumObj.addTaskInfo(dummyTask)

        # now with all valid, but invalid JobSummary object
        dummyTask = DummyTask(reqName, taskName, "dummy_task_type")
        with self.assertRaises(AttributeError):
            taskSumObj.addTaskInfo(dummyTask)

        # now update it with some valid content
        dummyTask.setJobSummary(JobSummary())
        taskSumObj.addTaskInfo(dummyTask)
        self.assertFalse(taskSumObj.isTaskCompleted())

        # now complete this task
        # now update it with some valid content
        dummyTask.setJobSummary(JobSummary({"success": 10}))
        taskSumObj.addTaskInfo(dummyTask)
        self.assertTrue(taskSumObj.isTaskCompleted())

    def testRequestInfo(self):
        """some very basic unit tests for the RequestInfo class"""
        reqName = "test_request_name"
        taskName = "test_task_name"
        agentName = "test_agent_name"

        # default object
        defaultDict = {"RequestName": reqName,
                       "AgentJobInfo": {}}
        reqInfoObj = RequestInfo(defaultDict)
        self.assertItemsEqual(reqInfoObj.getTasks(), {})
        self.assertEqual(reqInfoObj.getTotalTopLevelJobs(), "N/A")
        self.assertEqual(reqInfoObj.getTotalInputLumis(), "N/A")
        self.assertEqual(reqInfoObj.getTotalInputEvents(), "N/A")
        self.assertEqual(reqInfoObj.getTotalTopLevelJobsInWMBS(), 0)
        self.assertItemsEqual(reqInfoObj.getJobSummaryByAgent(), {})
        self.assertItemsEqual(reqInfoObj.getTasksByAgent(), {})
        self.assertFalse(reqInfoObj.isWorkflowFinished())

        # with some useful content now
        defaultDict = {"RequestName": reqName,
                       "total_jobs": 10,
                       "input_lumis": 100,
                       "input_events": 1000,
                       "AgentJobInfo": {agentName: {"status": {"success": 10, "inWMBS": 10},
                                                    "tasks": {taskName: {"status": {"success": 10}}}}}}
        reqInfoObj = RequestInfo(defaultDict)
        self.assertItemsEqual(list(reqInfoObj.getTasks()), [taskName])
        self.assertEqual(reqInfoObj.getTotalTopLevelJobs(), 10)
        self.assertEqual(reqInfoObj.getTotalInputLumis(), 100)
        self.assertEqual(reqInfoObj.getTotalInputEvents(), 1000)
        self.assertEqual(reqInfoObj.getTotalTopLevelJobsInWMBS(), 10)
        self.assertIsInstance(reqInfoObj.getJobSummaryByAgent(), dict)
        self.assertTrue(agentName in reqInfoObj.getJobSummaryByAgent())
        self.assertIsInstance(reqInfoObj.getJobSummaryByAgent(agentName), JobSummary)
        self.assertIsInstance(reqInfoObj.getTasksByAgent(), dict)
        self.assertTrue(agentName in reqInfoObj.getTasksByAgent())
        self.assertIsInstance(reqInfoObj.getTasksByAgent(agentName), dict)
        self.assertTrue(taskName in reqInfoObj.getTasksByAgent(agentName))
        self.assertTrue(reqInfoObj.isWorkflowFinished())


if __name__ == '__main__':
    unittest.main()
