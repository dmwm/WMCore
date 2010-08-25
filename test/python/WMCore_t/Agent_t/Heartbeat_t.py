"""
_WorkQueueTestCase_

Unit tests for the WMBS File class.
"""

__revision__ = "$Id: Heartbeat_t.py,v 1.1 2010/06/21 21:20:33 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import time
from WMQuality.TestInit import TestInit
from WMCore.Agent.HeartbeatAPI import HeartbeatAPI
# pylint: disable-msg = W0611

class HeartbeatTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        Heartbeat tables.  Also add some dummy locations.
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging() # logLevel = logging.SQLDEBUG
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.Agent.Database"],
                                useDefault = False)
        self.heartbeat = HeartbeatAPI("testComponent")
        
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the Heartbeat tables.
        """
        
        self.testInit.clearDatabase()
    
    def testS(self):
        print "testshema"
    
    def testHeartbeat(self):
        testComponent = HeartbeatAPI("testComponent")
        testComponent.registerComponent()
        self.assertEqual(testComponent.getHeartbeatInfo(), [])
        
        testComponent.updateWorkerHeartbeat("testWorker")
        result = testComponent.getHeartbeatInfo()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['worker_name'], "testWorker")
        time.sleep(1)
        
        testComponent.updateWorkerHeartbeat("testWorker2")
        result = testComponent.getHeartbeatInfo()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['worker_name'], "testWorker2")
        
        time.sleep(1)
        testComponent.updateWorkerHeartbeat("testWorker")
        result = testComponent.getHeartbeatInfo()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['worker_name'], "testWorker")
        
        
        testComponent = HeartbeatAPI("test2Component")
        testComponent.registerComponent()
        time.sleep(1)
        testComponent.updateWorkerHeartbeat("test2Worker")
        
        result = testComponent.getHeartbeatInfo()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['worker_name'], "testWorker")
        self.assertEqual(result[1]['worker_name'], "test2Worker")
        
        time.sleep(1)
        testComponent.updateWorkerHeartbeat("test2Worker2")
        result = testComponent.getHeartbeatInfo()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['worker_name'], "testWorker")
        self.assertEqual(result[1]['worker_name'], "test2Worker2")
        
        time.sleep(1)
        testComponent.updateWorkerHeartbeat("test2Worker")
        result = testComponent.getHeartbeatInfo()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['worker_name'], "testWorker")
        self.assertEqual(result[1]['worker_name'], "test2Worker")
        
if __name__ == "__main__":
    unittest.main()      