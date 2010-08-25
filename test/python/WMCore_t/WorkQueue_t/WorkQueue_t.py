#!/usr/bin/env python
"""

"""

__revision__ = "$Id: WorkQueue_t.py,v 1.2 2009/05/28 17:14:30 swakef Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import pickle
import os
from WMCore.WorkQueue.WorkQueue import WorkQueue, _WQElement
from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMTask import makeWMTask


def createSpec(name, path):
    """
    create a wmspec object and save to disk
    """
    wmspec = newWorkload(name)
    wmspec.addTask(makeWMTask('task1'))
    out = open(path, 'wb')
    pickle.dump(wmspec, out)
    out.close()

class WorkQueueElementTest(unittest.TestCase):
    """
    _WorkQueueElementTest_
    
    """


    def setUp(self):
        """
        If we dont have a wmspec file create one
        """
        self.specFile = os.path.join(os.getcwd(), 'testworkflow.pickle')
        self.specName = 'testWf'
        createSpec(self.specName, self.specFile)


    def tearDown(self):
        """tearDown"""
        try:
            os.unlink(self.specFile)
        except:
            pass

    def testOrdering(self):
        """
        Test priority sorting
        """
        ele1 = _WQElement(self.specFile)
        ele2 = _WQElement(self.specFile, priority=100)
        self.assertTrue(ele2 < ele1)


    def testMatch(self):
        """
        Test elements match correctly
        """
        condition = {'SiteA' : 100}
        ele = _WQElement(self.specFile, njobs=50)
        matched, _ = ele.match({'SiteA' : 49})
        self.assertFalse(matched)
        matched, condition = ele.match(condition)
        self.assertTrue(matched)
        self.assertEqual(condition, {'SiteA' : 50})
        matched, condition = ele.match(condition)
        self.assertTrue(matched)
        self.assertEqual(condition, {})
        matched, condition = ele.match(condition)
        self.assertFalse(matched)
        ele.setStatus("Acquired")
        self.assertEqual("Acquired", ele.status)


    def runTest(self):
        self.testOrdering() 
        self.testMatch()


class WorkQueueTest(unittest.TestCase):
    """
    _WorkQueueTest_
    
    """
    setup = False
    specFile = os.path.join(os.getcwd(), 'testworkflow.pickle')
    specName = 'testWf'
    queue = None
    
    
    def createSpec(self, name, path):
        """
        create a wmspec object and save to disk
        """
        wmspec = newWorkload(name)
        wmspec.addTask(makeWMTask('task1'))
        out = open(path, 'wb')
        pickle.dump(wmspec, out)
        out.close()
    
    
    def setUp(self):
        """
        If we dont have a wmspec file create one
        """
        self.specFile = os.path.join(os.getcwd(), 'testworkflow.pickle')
        self.specName = 'testWf'
        createSpec(self.specName, self.specFile)
        if not self.__class__.queue:
            self.__class__.queue = WorkQueue('http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')
        self.setup = True


    def tearDown(self):
        """tearDown"""
        try:
            os.unlink(self.specFile)
        except:
            pass

        
    def testEnQueue(self):
        """
        Create and enqueue a WMSpec, ensure splitting matches expectations
        """
        self.queue.queueWork(self.specFile)
        self.assertEqual(10, len(self.queue))
        work = self.queue.getWork({'SiteA' : 1})
        self.assertEqual([], work)
        work = self.queue.getWork({'SiteA' : 100})
        self.assertEqual(len(work), 1)
        work = self.queue.getWork({'SiteA' : 100, 'SiteB' : 100})
        self.assertEqual(len(work), 2)
        affectedBlocks = self.queue.setPriority(50, self.specFile)
        self.assertNotEqual(0, affectedBlocks)
        affectedBlocks = self.queue.setPriority(50, 'blahhhhhh')
        self.assertFalse(affectedBlocks)
        work = self.queue.getWork({'SiteA' : 10000})
        self.assert_(len(work) > 1)
        work = self.queue.getWork({'SiteA' : 10000})
        self.assert_(len(work) > 1)
        work = self.queue.gotWork(*work)
        self.assertTrue(work)
        work = self.queue.getWork({'SiteA' : 10000})
        self.assertEqual(0, len(work))

#    def testDeQueue(self):
#        """
#        Test we get expected work back from list
#        """
#        work = self.queue.getWork({'SiteA' : 100000})
#        print work
#        assert(len(self.queue) != 0)
#        a
        
        
#    def testPriorityChange(self):
#        """
#        If an element has its priority changed check it position also changes
#        """
#        raise RuntimeError
        

    def runTest(self):
        self.testEnQueue() 
        #self.testDeQueue()
        #self.testPriorityChange()


if __name__ == "__main__":
    unittest.main()