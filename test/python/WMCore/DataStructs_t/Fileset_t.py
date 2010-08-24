#!/usr/bin/env python
""" 
Testcase for Fileset

Instantiate a Fileset, with an initial file on its Set. After being populated with 1000 random files,
its access methods and additional file insert methods are tested

"""

import unittest, logging, random
import Fileset

class FilesetClassTest (TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=__file__.replace('.py','.log'),
                    filemode='w')
        self.logger = logging.getLogger('FilesetClassTest')
        
        #Setup the initial testcase environment:
        self.initialfile= File('/tmp/lfn1',1000,1,1,1)
        self.initialSet = Set(initialfile)
        
        #A Fileset, containing a initial file on it.
        fileset = Fileset(name = 'testFileSet', files = self.initialSet, logger = self.logger )
        #Populating the fileset
        for i in range(1,1000):
            lfn = '/store/data/%s/%s/file.root' % (random.randint(1000, 9999),
                                              random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)

            file = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi)
            fileset.addFile(file)
        
    def tearDown(self):
        #Is there a need for a tearDown in our fileset testcase?
        pass
    def testAddFile(self):
        #First test - Add file and check if its there
        #Second test - Add file that was already at Fileset.files , and check if its updated
        #Third test - Add file that was already at Fileset.newfiles , and check if its updated
        pass
    def testListFiles(self):
        filestemp = fileSet.listFiles()
        assert( filesettemp in (testFileSet._files | testFileSet._newfiles), 'Message' )
        pass
    def testListLFNs(self):
        #Im a bit confused with this method, leave to discuss it at the meeting with Simon
        pass
    def testListNewFiles(self):
        newfilestemp = testFileSet.ListNewFiles()
        assert(newfilestemp == testFileSet._newfiles, 'Message')
    def testCommit(self):
        localTestFileSet = Fileset('LocalTestFileset', initialSet)
        localTestFileSet.add(file3)
        newfilestemp = localTestFileSet.ListNewFiles()
        localTestFileSet.commit()
        #First, testing if the new file is present at file Set object attribute of the Fileset object
        assert( newfilestemp in localTestFileSet.listFiles(), 'Message' )
        #Second, testing if the newfile Set object attribute is empty
        assert (localTestFileSet._newfiles == None, 'Message')
