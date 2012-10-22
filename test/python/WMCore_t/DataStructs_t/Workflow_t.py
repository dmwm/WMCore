#!/usr/bin/env python
"""
_Workflow_

Unittest for the WMCore.DataStructs.Workflow class.
"""

import unittest

from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Fileset import Fileset

class WorkflowTest(unittest.TestCase):
    """
    _WorkflowTest_

    """
    def testDefinition(self):
        """
        Tests to make sure Workflow is defined correctly

        """

        testSpec  = "test"
        testOwner = "mnorman"
        testName  = "testName"

        testWorkflow = Workflow(spec = testSpec, owner = testOwner, name = testName)

        self.assertEqual(testWorkflow.spec,  testSpec)
        self.assertEqual(testWorkflow.owner, testOwner)
        self.assertEqual(testWorkflow.name,  testName)

        return

    def testAddOutput(self):
        """
        _testAddOutput_

        Tests the addOutput functionality of the DataStructs Workflow.
        """
        filesetA = Fileset(name = "filesetA")
        filesetB = Fileset(name = "filesetB")
        filesetC = Fileset(name = "filesetC")

        testWorkflow = Workflow(spec = "test", owner = "mnorman")
        testWorkflow.addOutput("out1", filesetA, filesetB)
        testWorkflow.addOutput("out1", filesetB, filesetA)
        testWorkflow.addOutput("out2", filesetC)

        self.assertEqual(len(testWorkflow.outputMap["out1"]), 2,
                         "Error: There should be two mappings for out1.")
        self.assertEqual(len(testWorkflow.outputMap["out2"]), 1,
                         "Error: There should be two mappings for out2.")

        self.assertTrue({"output_fileset": filesetA,
                         "merged_output_fileset": filesetB} in testWorkflow.outputMap["out1"],
                        "Error: Fileset A should be in the output map.")
        self.assertTrue({"output_fileset": filesetB,
                         "merged_output_fileset": filesetA} in testWorkflow.outputMap["out1"],
                        "Error: Fileset B should be in the output map.")

        self.assertEqual(filesetC, testWorkflow.outputMap["out2"][0]["output_fileset"],
                        "Error: Fileset C should be in the output map.")
        self.assertEqual(None, testWorkflow.outputMap["out2"][0]["merged_output_fileset"],
                         "Error: The merged output should be None.")
        return

if __name__ == '__main__':
    unittest.main()
