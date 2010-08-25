#!/usr/bin/env python
"""
_Workflow_

Unittest for the WMCore.DataStructs.Workflow class

"""


# This code written as essentially a blank for future
# Workflow development
# -mnorman


import unittest
from WMCore.DataStructs.Workflow import Workflow


class WorkflowTest(unittest.TestCase):
    """
    _WorkflowTest_

    """


    def testDefinition(self):
        """
        Tests to make sure Workflow is defined correctly

        """

        print "testDefinition"

        testSpec  = "test"
        testOwner = "mnorman"
        testName  = "testName"

        testWorkflow = Workflow(spec = testSpec, owner = testOwner, name = testName)

        self.assertEqual(testWorkflow.spec,  testSpec)
        self.assertEqual(testWorkflow.owner, testOwner)
        self.assertEqual(testWorkflow.name,  testName)

        return


    def testAddObject(self):
        """
        Tests the AddObject functionality of the DataStruct Workflow

        """

        print "testAddObject"

        testName    = "test"
        testFileset = "testFileset"


        testWorkflow = Workflow(spec = "test", owner = "mnorman")
        testWorkflow.addOutput(testName, testFileset)

        assert testWorkflow.outputMap[testName] == testFileset, "Fileset name not properly set in Workflow"

        return
    
if __name__ == '__main__':
    unittest.main()
