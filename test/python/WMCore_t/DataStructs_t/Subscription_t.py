#!/usr/bin/env python
"""
_Subscription_t_

Testcase for the Subscription class
"""

from builtins import str, range

import random
import unittest

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Run import Run
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow


class SubscriptionTest(unittest.TestCase):
    """
    _SubscriptionTest_

    Testcase for the Subscription class

    """

    def setUp(self):
        """
        Initial Setup for Subscription Testcase

        Set a dummy Subscription with a fileset composed of one file inside it
        and a dummy workflow using the default constructor of the Workflow class

        """
        self.dummyFile = File('/tmp/dummyfile', 9999, 0, 0, 0)
        self.dummySet = set()
        self.dummySet.add(self.dummyFile)
        self.dummyFileSet = Fileset(name='SubscriptionTestFileset',
                                    files=self.dummySet)
        self.dummyWorkFlow = Workflow()
        self.dummySubscription = Subscription(fileset=self.dummyFileSet,
                                              workflow=self.dummyWorkFlow)
        return

    def tearDown(self):
        pass

    def testGetWorkflow(self):
        """
        Testcase for the getWorkflow method of the Subscription Class

        """
        assert self.dummySubscription['workflow'] == self.dummyWorkFlow, \
            'Couldn\'t add Workflow to Subscription'

    def testGetFileset(self):
        """
        Testcase for the getFileset method of the Subscription Class

        """
        assert self.dummyFileSet.name == self.dummySubscription['fileset'].name, \
            'Couldn\'t add Fileset to Subscription - name does not match'

        for x in self.dummyFileSet.listNewFiles():
            assert x in self.dummySubscription['fileset'].newfiles, \
                'Couldn\'t add Fileset to Subscription - newFiles Set does not match'

        assert self.dummyFileSet.getFiles(type='set') == \
               self.dummySubscription['fileset'].getFiles(type='set'), \
            'Couldn\'t add Fileset to Subscription - %s Set does not match' % x

    def testAcquireFiles(self):
        """
        Testcase for the acquireFiles method of the Subscription Class

        """
        # Cleaning possible files already occupying the available set
        self.dummySubscription.acquireFiles()

        # First test - Test if initial file (on available set) is inserted in the
        # acquired set - no arguments

        dummyFile2 = File('/tmp/dummyfile2,8888', 1, 1, 1)
        # Insert dummyFile2 into the available files Set at dummySubscription
        self.dummySubscription.available.addFile(dummyFile2)

        S = self.dummySubscription.available.listNewFiles()
        # Check if Set returned by method is the same that was at the previous
        # available FileSet
        assert S == self.dummySubscription.acquireFiles(), \
            'Couldn\'t acquire file using method acquireFiles - (no arguments test)'

        # Second test - Test if target files are inserted at the acquired set

        dummyFileList = set()
        # Populating the dummy List with a random number of files
        for i in range(1, random.randint(100, 1000)):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                                   random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events,
                        checksums={"cksum": "1"})
            file.addRun(Run(run, *[lumi]))
            dummyFileList.add(file)

        # Check if return value is correct - with parameters
        acqFiles = self.dummySubscription.acquireFiles(files=dummyFileList)
        assert acqFiles == dummyFileList, \
            'Return value for acquireFiles method not the acquired files'
        # Check if all files were inserted at subscription acquired files Set
        for x in dummyFileList:
            assert x in self.dummySubscription.acquired.getFiles(type='set'), \
                'Couldn\'t acquire File %s' % x.dict['lfn']

        # Third test - Test if a replicate file is erased from the other Sets,
        # when a file is acquired

        dummyFile3 = File('/tmp/dummyfile3,5555', 1, 1, 1)
        dummyFileList = []
        dummyFileList.append(dummyFile3)

        # Inserting dummyFile3 to be used as an argument, into each of the other file sets
        self.dummySubscription.available.addFile(dummyFile3)
        self.dummySubscription.failed.addFile(dummyFile3)
        self.dummySubscription.completed.addFile(dummyFile3)

        # Run the method acquireFiles
        self.dummySubscription.acquireFiles(files=dummyFileList, size=1)

        # Check if dummyFile3 was inserted at the acquired Set
        assert dummyFile3 in self.dummySubscription.acquired.getFiles(type='set'), \
            'Replicated file could\'nt be inserted at acquired Set'

        # Check if dummyFile3 was erased from all the other Sets
        assert dummyFile3 not in self.dummySubscription.available.getFiles(type='set'), \
            'Acquired file still present at available Set'
        assert dummyFile3 not in self.dummySubscription.failed.getFiles(type='set'), \
            'Acquired file still present at failed Set'
        assert dummyFile3 not in self.dummySubscription.completed.getFiles(type='set'), \
            'Acquired file still present at completed Set'

        # Fourth test - Test if the method works properly if a wrong size number
        # is given as an argument

        # Case 1: size < number of files given as an argument

        dummyFileList = []
        for i in range(90, 100):
            dummyFileSize = File('/tmp/dummyfile' + str(i), 7656, 1, 1, 1)
            dummyFileList.append(dummyFileSize)

        # Run the method:
        self.dummySubscription.acquireFiles(files=dummyFileList, size=1)
        # Check each file of the List
        for x in dummyFileList:
            assert x in self.dummySubscription.acquired.getFiles(type='set'), \
                'File wasn\'t acquired (lower Size argument test)'

        # Case 2: size = 0

        # Run the method:
        self.dummySubscription.acquireFiles(files=dummyFileList, size=0)
        # Check each file of the List
        for x in dummyFileList:
            assert x in self.dummySubscription.acquired.getFiles(type='set'), \
                'File wasn\'t acquired (zero size argument test)'

    def testCompleteFiles(self):
        """
        Testcase for the completeFiles method of the Subscription Class

        """
        # Cleaning possible files already occupying the available set
        self.dummySubscription.completeFiles([])

        # First test - Test if initial file (on available set) is inserted in the
        # completed set - no arguments

        dummyFile2 = File('/tmp/dummyfile2,8888', 1, 1, 1)
        # Insert dummyFile2 into the available files Set at dummySubscription
        self.dummySubscription.available.addFile(dummyFile2)

        S = self.dummySubscription.availableFiles()
        # complete all files
        self.dummySubscription.completeFiles(S)

        assert len(self.dummySubscription.availableFiles()) == 0, \
            "completed subscription still has %s files, what's up with that?" % \
            len(self.dummySubscription.availableFiles())

        # Second test - Test if target files are inserted at the completed files set

        dummyFileList = []
        # Populating the dummy List with a random number of files
        for i in range(1, random.randint(100, 1000)):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                                   random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events,
                        checksums={"cksum": "1"})
            file.addRun(Run(run, *[lumi]))
            dummyFileList.append(file)
        # Add the new files
        self.dummySubscription.available.addFile(dummyFileList)
        # and complete them
        self.dummySubscription.completeFiles(files=dummyFileList)
        # Check if return value is correct - with parameters
        assert len(self.dummySubscription.availableFiles()) == 0, \
            "completed subscription still has %s files, what's up with that?" % \
            len(self.dummySubscription.availableFiles())

        # Check if all files were inserted at subscription's completed files Set
        for x in dummyFileList:
            assert x in self.dummySubscription.completed.getFiles(type='set'), \
                'Couldn\'t make file completed %s' % x.dict['lfn']

        # Third test - Test if a replicate file is erased from the other Sets,
        # when a file is made completed

        dummyFile3 = File('/tmp/dummyfile3,5555', 1, 1, 1)
        dummyFileList = []
        dummyFileList.append(dummyFile3)

        # Inserting dummyFile3 to be used as an argument, into each of the other
        # file sets
        self.dummySubscription.acquired.addFile(dummyFile3)
        self.dummySubscription.failed.addFile(dummyFile3)
        self.dummySubscription.completed.addFile(dummyFile3)

        # Run the method completeFiles
        self.dummySubscription.completeFiles(files=dummyFileList)

        # Check if dummyFile3 was inserted at the completed Set
        assert dummyFile3 in self.dummySubscription.completed.getFiles(type='set'), \
            'Replicated file could\'nt be inserted at completed Set'

        # Check if dummyFile3 was erased from all the other Sets
        assert dummyFile3 not in self.dummySubscription.acquired.getFiles(type='set'), \
            'Completed file still present at acquired Set'
        assert dummyFile3 not in self.dummySubscription.failed.getFiles(type='set'), \
            'Completed file still present at failed Set'
        assert dummyFile3 not in self.dummySubscription.available.getFiles(type='set'), \
            'Completed file still present at available Set'

    def testFailFiles(self):
        """
        Testcase for the failFiles method of the Subscription Class

        """
        # Cleaning possible files already occupying the available set
        self.dummySubscription.failFiles([])

        # First test - Test if initial file (on available set) is inserted in the
        # failed set - no arguments

        dummyFile2 = File('/tmp/dummyfile2,8888', 1, 1, 1)
        # Insert dummyFile2 into the available files Set at dummySubscription
        self.dummySubscription.available.addFile(dummyFile2)

        S = self.dummySubscription.availableFiles()
        # Fail all files
        self.dummySubscription.failFiles(S)

        assert len(self.dummySubscription.availableFiles()) == 0, \
            "failed subscription still has %s files, what's up with that?" % \
            len(self.dummySubscription.availableFiles())

        # Second test - Test if target files are inserted at the failed set

        dummyFileList = []
        # Populating the dummy List with a random number of files
        for i in range(1, random.randint(100, 1000)):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                                   random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events,
                        checksums={"cksum": "1"})
            file.addRun(Run(run, *[lumi]))
            dummyFileList.append(file)
        # Add the new files
        self.dummySubscription.available.addFile(dummyFileList)
        # and fail them
        self.dummySubscription.failFiles(files=dummyFileList)
        # Check there are no files available - everything should be failed
        assert len(self.dummySubscription.availableFiles()) == 0, \
            "failed subscription still has %s files, what's up with that?" % \
            len(self.dummySubscription.availableFiles())

        # Check if all files were inserted at subscription's failed files Set
        for x in dummyFileList:
            assert x in self.dummySubscription.failed.getFiles(type='set'), \
                'Couldn\'t make file failed %s' % x.dict['lfn']

        # Third test - Test if a replicate file is erased from the other Sets,
        # when a file is considered failed

        dummyFile3 = File('/tmp/dummyfile3,5555', 1, 1, 1)
        dummyFileList = []
        dummyFileList.append(dummyFile3)

        # Inserting dummyFile3 to be used as an argument, into each of the other
        # file sets
        self.dummySubscription.acquired.addFile(dummyFile3)
        self.dummySubscription.available.addFile(dummyFile3)
        self.dummySubscription.completed.addFile(dummyFile3)

        # Run the method failFiles
        self.dummySubscription.failFiles(files=dummyFileList)

        # Check if dummyFile3 was inserted at the failed Set
        assert dummyFile3 in self.dummySubscription.failed.getFiles(type='set'), \
            'Replicated file could\'nt be inserted at failed Set'

        # Check if dummyFile3 was erased from all the other Sets
        assert dummyFile3 not in self.dummySubscription.acquired.getFiles(type='set'), \
            'Failed file still present at acquired Set'
        assert dummyFile3 not in self.dummySubscription.completed.getFiles(type='set'), \
            'Failed file still present at completed Set'
        assert dummyFile3 not in self.dummySubscription.available.getFiles(type='set'), \
            'Failed file still present at available Set'

    def testFilesOfStatus(self):
        """
        Testcase for the filesOfStatus method of the Subscription Class

        """

        assert self.dummySubscription.filesOfStatus('Available') == \
               self.dummySubscription.available.getFiles(type='set') - \
               self.dummySubscription.acquiredFiles() | self.dummySubscription.completedFiles() | self.dummySubscription.failedFiles(), \
            'Method fileOfStatus(\'AvailableFiles\') does not return available files set'
        assert self.dummySubscription.filesOfStatus('Acquired') == self.dummySubscription.acquired.getFiles(type='set'), \
            'Method fileOfStatus(\'AcquiredFiles\') does not return acquired files set'
        assert self.dummySubscription.filesOfStatus('Completed') == self.dummySubscription.completed.getFiles(
            type='set'), \
            'Method fileOfStatus(\'CompletedFiles\') does not return completed files set'
        assert self.dummySubscription.filesOfStatus('Failed') == self.dummySubscription.failed.getFiles(type='set'), \
            'Method fileOfStatus(\'FailedFiles\') does not return failed files set'

    def testAvailableFiles(self):
        """
        Testcase for the availableFiles method of the Subscription Class
        """
        assert self.dummySubscription.availableFiles() == \
               self.dummySubscription.available.getFiles(type='set'), \
            'Method availableFiles does not return available files Set'

    def testAcquiredFiles(self):
        """
        Testcase for the acquiredFiles method of the Subscription Class
        """
        assert self.dummySubscription.acquiredFiles() == \
               self.dummySubscription.acquired.getFiles(type='set'), \
            'Method acquiredFiles does not return acquired files Set'

    def testCompletedFiles(self):
        """
        Testcase for the completedFiles method of the Subscription Class
        """
        assert self.dummySubscription.completedFiles() == \
               self.dummySubscription.completed.getFiles(type='set'), \
            'Method completedFiles does not return completed files Set'

    def testFailedFiles(self):
        """
        Testcase for the failedFiles method of the Subscription Class
        """
        assert self.dummySubscription.failedFiles() == \
               self.dummySubscription.failed.getFiles(type='set'), \
            'Method failedFiles does not return failed files Set'


if __name__ == "__main__":
    unittest.main()
