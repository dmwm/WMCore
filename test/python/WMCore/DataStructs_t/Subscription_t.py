#!/usr/bin/env python
"""
_Subscription_t_

Testcase for the Subscription class

"""


import unittest, os, logging, commands
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Subscription import Subscription
from unittest import TestCase

class Subscription(unittest.TestCase):
	"""
	_Subscription_t_

	Testcase for the Subscription class

	"""
	def setUp(self):
		
		#Logger setup
		logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=__file__.replace('.py','.log'),
                    filemode='w')
        self.logger = logging.getLogger('SubscriptionClassTest')

		#Initial testcase environment
	dummyFile = File('/tmp/dummyfile',9999,0,0,0,0)
	dummySet = Set(dummyFile)
	dummyFileSet = Fileset(dummySet)
	dummyWorkflow = Workflow()
	dummySubscription = Subscription(workflow=dummyWorkflow, fileset=Fileset(files=dummySet) )
	
	def tearDown(self):
		pass

	def testGetWorkflow(self):
		assert dummySubscription.workflow == dummyWorkflow, 'Message'
		pass

	def testGetFileset(self):
		assert dummy == FilesetSubscription.fileset.files, 'Message' 
		pass

	def testAvailableFiles(self):
		temp = dummySubscription.availableFiles()
		for x in temp:
			assert x not in (dummySubscription.acquiredFiles() | dummySubscription.failedFiles() | \
				 dummySubscription.completedFiles()), 'Message'
		pass

	def testAcquireFiles(self):
		pass

	def testCompleteFiles(self):
		pass

	def testFailFiles(self):
		pass

	def testFilesOfStatus(self):
		assert dummySubscription.FilesOfStatus('AvailableFiles') == dummySubscription.available.listFiles() - \
			dummySubscription.acquiredFiles() | dummySubscription.completedFiles() | dummySubscription.failedFiles(), \
				'Message'
		assert dummySubscription.FilesOfStatus('AcquiredFiles') == dummySubscription.acquired.listFiles(), 'Message'
		assert dummySubscription.FilesOfStatus('CompletedFiles') == dummySubscription.completed.listFiles(), 'Message'
		assert dummySubscription.FilesOfStatus('FailedFiles') == dummySubscription.failed.listFiles(), 'Message'

	def testAvailableFiles(self):
		assert dummySubscription.availableFiles() == dummySubscription.available.ListFiles(), 'Message'

	def testAcquiredFiles(self):
        	assert dummySubscription.acquiredFiles() == dummySubscription.acquired.ListFiles(), 'Message'

	def testCompletedFiles(self):
        	assert dummySubscription.completedFiles() == dummySubscription.completed.ListFiles(), 'Message'

	def testFailedFiles(self):
        	assert dummySubscription.failedFiles() == dummySubscription.failed.ListFiles(), 'Message'


if __name__ == "__main__":
            unittest.main()

