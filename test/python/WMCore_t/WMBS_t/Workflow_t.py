#!/usr/bin/env python2.4
"""
_Workflow_t_

Unit tests for the WMBS Workflow class.
"""

__revision__ = "$Id: Workflow_t.py,v 1.1 2008/11/20 16:19:52 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import os
import threading

from WMCore.WMBS.Workflow import Workflow
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class Workflow_t(unittest.TestCase):
    _setup = False
    _teardown = False
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        if not self._setup:
            self.testInit = TestInit(__file__, os.getenv("DIALECT"))
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                    useDefault = False)
            self._setup = True

        return
                       
    def tearDown(self):
        """
        _tearDown_

        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()

        if not self._teardown:
            factory = WMFactory("WMBS", "WMCore.WMBS")
            destroy = factory.loadObject(myThread.dialect + ".Destroy")
            myThread.transaction.begin()
            destroyworked = destroy.execute(conn = myThread.transaction.conn)
            if not destroyworked:
                raise Exception("Could not complete WMBS tear down.")
            myThread.transaction.commit()
            
            self._teardown = False

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        Test the create(), delete() and exists() methods of the workflow class
        by creating and deleting a workflow.  The exists() method will be
        called before and after creation and after deletion.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists before it was created"

        testWorkflow.create()

        assert testWorkflow.exists() > 0, \
               "ERROR: Workflow does not exist after it has been created"

        testWorkflow.delete()

        assert testWorkflow.exists() == False, \
               "ERROR: Workflow exists after it has been deleted"        
        return

    def testLoad(self):
        """
        _testLoad_

        Create a workflow and then try to load it from the database using the
        following load methods:
          Workflow.LoadFromName
          Workflow.LoadFromID
          Workflow.LoadFromSpecOwner
        """
        testWorkflowA = Workflow(spec = "spec.xml", owner = "Simon",
                                 name = "wf001")
        testWorkflowA.create()

        testWorkflowB = Workflow(name = "wf001")
        testWorkflowB.load(method = "Workflow.LoadFromName")
        assert (testWorkflowA.id == testWorkflowB.id) and \
               (testWorkflowA.name == testWorkflowB.name) and \
               (testWorkflowA.spec == testWorkflowB.spec) and \
               (testWorkflowA.owner == testWorkflowB.owner), \
               "ERROR: Workflow.LoadFromName Failed"
        
        testWorkflowC = Workflow(id = testWorkflowA.id)
        testWorkflowC.load(method = "Workflow.LoadFromID")
        assert (testWorkflowA.id == testWorkflowC.id) and \
               (testWorkflowA.name == testWorkflowC.name) and \
               (testWorkflowA.spec == testWorkflowC.spec) and \
               (testWorkflowA.owner == testWorkflowC.owner), \
               "ERROR: Workflow.LoadFromID Failed"
        
        testWorkflowD = Workflow(spec = "spec.xml", owner = "Simon")
        testWorkflowD.load(method = "Workflow.LoadFromSpecOwner")
        assert (testWorkflowA.id == testWorkflowD.id) and \
               (testWorkflowA.name == testWorkflowD.name) and \
               (testWorkflowA.spec == testWorkflowD.spec) and \
               (testWorkflowA.owner == testWorkflowD.owner), \
               "ERROR: Workflow.LoadFromSpecOwner Failed"

        testWorkflowA.delete()
        return
                                            
if __name__ == "__main__":
    unittest.main()
