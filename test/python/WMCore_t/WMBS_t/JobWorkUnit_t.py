#!/usr/bin/env python
"""
_JobWorkUnit_t_

Unit tests for the interaction of WMBS Job and WorkUnit
"""
from __future__ import absolute_import, division, print_function

import unittest

from WMCore.DataStructs.Run import Run
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset as Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.WorkUnit import WorkUnit
from WMCore.WMBS.Workflow import Workflow
from WMCore_t.WMBS_t.JobTestBase import JobTestBase


class JobWorkUnitTest(JobTestBase):
    """
    Create jobs and test that associated WorkUnits are correct
    """

    def notestJobCreatesWorkUnits(self):
        """
        Create a single jobs and test that both work units are created
        """

        self.createSingleJobWorkflow()

        testRunLumi = Run(1, 45)
        loadByFRL = WorkUnit(taskID=self.testWorkflow.id, fileid=self.testFileA['id'], runLumi=testRunLumi)
        loadByFRL.load()

        self.assertEqual(loadByFRL['last_unit_count'], 2)  # Two lumis in this job
        self.assertGreater(loadByFRL['id'], 0)

        testRunLumi = Run(1, 46)
        loadByFRL = WorkUnit(taskID=self.testWorkflow.id, fileid=self.testFileB['id'], runLumi=testRunLumi)
        loadByFRL.load()

        self.assertEqual(loadByFRL['last_unit_count'], 2)  # Two lumis in this job
        self.assertGreater(loadByFRL['id'], 0)

        return

    def notestCreateDeleteExists(self):
        """
        Create and then delete a job and workflow.  Use the workunit class's exists() method to
        determine if the workunit has been written to the database before the job is
        created, after the job has been created, and after the workflow has been deleted.
        """

        testWorkflow = Workflow(spec="spec.xml", owner="Simon", name="wf001", task="Test")
        testWorkflow.create()

        testWMBSFileset = Fileset(name="TestFileset")
        testWMBSFileset.create()

        testSubscription = Subscription(fileset=testWMBSFileset, workflow=testWorkflow)
        testSubscription.create()

        testJobGroup = JobGroup(subscription=testSubscription)
        testJobGroup.create()

        testFileA = File(lfn="/this/is/a/lfnA", size=1024, events=10)
        testFileA.addRun(Run(1, *[45]))
        testFileB = File(lfn="/this/is/a/lfnB", size=1024, events=10)
        testFileB.addRun(Run(1, *[46]))

        testFileA.create()
        testFileB.create()

        testJob = Job(name="TestJob", files=[testFileA, testFileB])
        testWU1 = WorkUnit(taskID=testWorkflow.id, fileid=testFileA['id'], runLumi=Run(1, *[45]))
        testWU2 = WorkUnit(taskID=testWorkflow.id, fileid=testFileB['id'], runLumi=Run(1, *[46]))

        self.assertFalse(testWU1.exists(), "WorkUnit exists before job was created")
        self.assertFalse(testWU2.exists(), "WorkUnit exists before job was created")

        testJob.create(group=testJobGroup)

        self.assertTrue(testWU1.exists(), "WorkUnit does not exist after job was created")
        self.assertTrue(testWU2.exists(), "WorkUnit does not exist after job was created")

        testJob.delete()

        self.assertTrue(testWU1.exists(), "WorkUnit does not exist after job is deleted")
        self.assertTrue(testWU2.exists(), "WorkUnit does not exist after job is deleted")

        testWorkflow.delete()

        self.assertFalse(testWU1.exists(), "WorkUnit exists after workflow is deleted")
        self.assertFalse(testWU2.exists(), "WorkUnit exists after workflow is deleted")

        return


if __name__ == "__main__":
    unittest.main()
