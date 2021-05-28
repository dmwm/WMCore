#!/usr/bin/env python
"""
_Fileset_t_

Testcase for Fileset

"""
from builtins import range
import logging
import random
import unittest

from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run

class FilesetTest (unittest.TestCase):
    """
    _FilesetTest_

    Testcase for Fileset
    """
    def setUp(self):
        """
        Create a dummy fileset and populate it with random files,
        in order to use it for the testcase methods

        """
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=__file__.replace('.py','.log'),
                    filemode='w')
        self.logger = logging.getLogger('FilesetClassTest')

        #Setup the initial testcase environment:
        initialfile = File('/tmp/lfn1',1000,1,1,1)
        self.initialSet = set()
        self.initialSet.add(initialfile)

        #Create a Fileset, containing a initial file on it.
        self.fileset = Fileset(name = 'self.fileset', files = self.initialSet)
        #Populate the fileset with random files
        for i in range(1,1000):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events, checksums = {"cksum": "1"})
            file.addRun(Run(run, *[lumi]))
            self.fileset.addFile(file)

    def tearDown(self):
        """
            No tearDown method for this testcase

        """
        pass

    def testAddFile(self):
        """
        _testAddFile_

        Testcase for the addFile method of the Fileset class

        """

        # First test - Add file and check if it's there
        testfile = File('/tmp/lfntest', 9999, 9, 9)
        self.fileset.addFile(testfile)
        self.assertTrue(testfile in self.fileset.listNewFiles(),
                        "Couldn't add file to fileset - fileset.addfile method not working")

        # Second test - Add file that was already at Fileset.files, and check if it gets updated
        testFileSame = File('/tmp/lfntest', 9999, 9, 9)
        testFileSame.setLocation(set('dummyse.dummy.com'))
        self.fileset.addFile(testFileSame)
        self.assertTrue(testFileSame in self.fileset.getFiles(),
                        'Same file copy ailed - fileset.addFile not updating location of already existing files')
        self.assertTrue(testfile in self.fileset.getFiles(),
                        'Same file copy failed - fileset.addFile unable to remove previous file from list')

        # Third test - Add file that was already at Fileset.newfiles, and check if it gets updated
        self.assertTrue(testFileSame in self.fileset.listNewFiles(),
                        'Same file copy failed - fileset.addFile not adding file to fileset.newFiles')

    def testListFiles(self):
        """
        _testListFiles_

        Testcase for the getFiles() method of the Fileset class
        """
        filesettemp = self.fileset.getFiles()
        for x in filesettemp:
            assert x in (self.fileset.files | self.fileset.newfiles), \
            'Missing file %s from file list returned from fileset.ListFiles' % x["lfn"]

    def testSetFiles(self):
        """
        Check that all files returned by the set are the same as those added to
        the fileset
        """
        filesettemp = self.fileset.getFiles(type = "set")
        for x in filesettemp:
            assert x in (self.fileset.files | self.fileset.newfiles), \
            'Missing file %s from file list returned from fileset.ListFiles' % x["lfn"]

    def testSetListCompare(self):
        """
        Test that all files in fileset.setFiles are in fileset.getFiles()
        """
        thelist = self.fileset.getFiles()
        theset = self.fileset.getFiles(type = "set")
        for x in thelist:
            assert x in (theset), \
            'Missing file %s from file list returned from fileset.ListFiles' % x["lfn"]

    def testSorting(self):
        """
        Fileset.getFiles() should be sorted the same as Fileset.getFiles(type = "lfn"),
        assert that this is the case here.
        """
        files = self.fileset.getFiles()
        lfns = self.fileset.getFiles(type = "lfn")
        for x in files:
            assert x["lfn"] in (lfns), \
            'Missing file %s from file list returned ' % x["lfn"]
            assert lfns[files.index(x)] == x["lfn"], \
            'Sorting not consistent: lfn = %s, file = %s' % (lfns[files.index(x)], x["lfn"])

    def testListLFNs(self):
        """
            Testcase for the listLFN method of the Fileset class

        """
        #Kinda slow way of verifying if the raw LFN from each file at the
        #fileset is returned from the meth
        allFiles = self.fileset.getFiles()

        #For each file returned by method listFiles, it checks if LFN is
        #present at the output of method listLFNs
        for x in allFiles:
            assert x['lfn'] in self.fileset.getFiles(type = "lfn"), 'Missing %s from ' \
            'list returned from fileset.ListLFNs' % x["lfn"]
        #Im a bit confused with this method, leave to discuss it at the meeting with Simon

    def testListNewFiles(self):
        """
            Testcase for the listNewFiles method of the Fileset class

        """
        newfilestemp = self.fileset.listNewFiles()
        assert newfilestemp == self.fileset.newfiles, 'Missing files from ' \
               'list returned from fileset.ListNewFiles'
    def testCommit(self):
        """
            Testcase for the commit method of the Fileset class

        """
        localTestFileSet = Fileset('LocalTestFileset', self.initialSet)
        fsSize = len(localTestFileSet.getFiles(type = "lfn"))
        #Dummy file to test
        fileTestCommit = File('/tmp/filetestcommit',0000,1,1)
        #File is added to the newfiles attribute of localTestFileSet
        localTestFileSet.addFile(fileTestCommit)
        assert fsSize == len(localTestFileSet.getFiles(type = "lfn")) - 1, 'file not added'\
                'correctly to test fileset'
        newfilestemp = localTestFileSet.newfiles
        assert fileTestCommit in newfilestemp, 'test file not in the new files'\
                'list'
        #After commit, dummy file is supposed to move from newfiles to files
        localTestFileSet.commit()
        #First, testing if the new file is present at file set object attribute of the Fileset object

        assert newfilestemp.issubset(localTestFileSet.files), 'Test file not ' \
                'present at fileset.files - fileset.commit ' \
                'not working properly'
        #Second, testing if the newfile set object attribute is empty
        assert localTestFileSet.newfiles == set(), \
                'Test file not present at fileset.newfiles ' \
                '- fileset.commit not working properly'

if __name__ == '__main__':
    unittest.main()
