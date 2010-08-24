#!/usr/bin/env python
""" 
_Fileset_t_

Testcase for Fileset

"""
import unittest, logging, random
from sets import Set
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.File import File

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
        self.initialSet = Set()
        self.initialSet.add(initialfile)
        
        #Create a Fileset, containing a initial file on it.
        self.fileset = Fileset(name = 'self.fileset', files = self.initialSet, logger = self.logger )
        #Populate the fileset with random files
        for i in range(1,1000):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events, cksum = 1)
            file.addRun(Run(run, *[lumi]))
            self.fileset.addFile(file)
        
    def tearDown(self):
        """
            No tearDown method for this testcase
            
        """
        pass
    def testAddFile(self):
        """
            Testcase for the addFile method of the Fileset class
            
        """
        #First test - Add file and check if its there
        testfile = File('/tmp/lfntest',9999,9,9)
        self.fileset.addFile(testfile)
        assert(testfile in self.fileset.listNewFiles(), 'Couldn\'t add file ' +
                'to fileset - fileset.addfile method not working')
        #Second test - Add file that was already at Fileset.files , 
        # and check if it gets updated
        testFileSame = File('/tmp/lfntest',9999,9,9)
        testFileSame.setLocation(Set('dummyse.dummy.com'))
        self.fileset.addFile(testFileSame)
        assert(testFileSame in  self.fileset.listFiles(),'Same file copy ' +
               'failed - fileset.addFile not updating location of already ' +
               'existing files' )
        assert(testfile in self.fileset.listFiles(),'Same file copy ' +
               'failed - fileset.addFile unable to remove previous file ' +
               'from list')
        #Third test - Add file that was already at Fileset.newfiles , 
        #and check if it gets updated
        assert(testFileSame in  self.fileset.listNewFiles(),'Same file copy ' +
               'failed - fileset.addFile not adding file to fileset.newFiles')
    def testListFiles(self):
        """
            Testcase for the listFiles method of the Fileset class
            
        """
        filesettemp = self.fileset.listFiles()
        for x in filesettemp:
            assert x in (self.fileset.files | self.fileset.newfiles), \
            'Missing file %s from file list returned from fileset.ListFiles' % x.dict["lfn"]
            
    def testSetFiles(self):
        """
        Check that all files returned by the set are the same as those added to 
        the fileset
        """
        filesettemp = self.fileset.setFiles()
        for x in filesettemp:
            assert x in (self.fileset.files | self.fileset.newfiles), \
            'Missing file %s from file list returned from fileset.ListFiles' % x.dict["lfn"]
            
    def testSetListCompare(self):
        """
        Test that all files in fileset.setFiles are in fileset.listFiles()
        """
        thelist = self.fileset.listFiles()
        theset = self.fileset.setFiles()
        for x in thelist:
            assert x in (theset), \
            'Missing file %s from file list returned from fileset.ListFiles' % x.dict["lfn"]
    
    def testSorting(self):
        """
        Fileset.listFiles() should be sorted the same as Fileset.listLFNs(), 
        assert that this is the case here.
        """
        files = self.fileset.listFiles()
        lfns = self.fileset.listLFNs()
        for x in files:
            assert x.dict["lfn"] in (lfns), \
            'Missing file %s from file list returned ' % x.dict["lfn"]
            assert lfns[files.index(x)] == x.dict["lfn"], \
            'Sorting not consistent: lfn = %s, file = %s' % (lfns[files.index(x)], x.dict["lfn"])
            
    def testListLFNs(self):
        """
            Testcase for the listLFN method of the Fileset class
            
        """
        #Kinda slow way of verifying if the raw LFN from each file at the
        #fileset is returned from the meth
        allFiles = self.fileset.listFiles()
        
        #For each file returned by method listFiles, it checks if LFN is
        #present at the output of method listLFNs 
        for x in allFiles:
            assert x.dict['lfn'] in self.fileset.listLFNs(), 'Missing %s from ' \
            'list returned from fileset.ListLFNs' % x.dict["lfn"] 
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
        fsSize = len(localTestFileSet.listLFNs())
        #Dummy file to test
        fileTestCommit = File('/tmp/filetestcommit',0000,1,1)
        #File is added to the newfiles attribute of localTestFileSet
        localTestFileSet.addFile(fileTestCommit)
        assert fsSize == len(localTestFileSet.listLFNs()) - 1, 'file not added'\
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
        assert localTestFileSet.newfiles == Set(), \
                'Test file not present at fileset.newfiles ' \
                '- fileset.commit not working properly'
     
if __name__ == '__main__':
    unittest.main()
