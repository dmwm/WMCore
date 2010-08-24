#!/usr/bin/env python
"""
A file set should take care of dealing with adding a new file or updating an
existing file in the fileset. This Unit test checks that with single File 
objects and multiple objects contained in a list or Set.
"""


__revision__ = "$Id: fileset_addFile_t.py,v 1.3 2008/12/26 15:31:19 afaq Exp $"
__version__ = "$Revision: 1.3 $"

import unittest, logging, os, commands
from sets import Set
import pdb
from WMCore.DataStructs.Run import Run


class BaseFilesetTestCase(unittest.TestCase):
    def setUp(self):
        pass
        
    def tearDown(self):
        pass
    
class DataStructsTestCase(BaseFilesetTestCase):
    def setUp(self):
        print "\n====DataStructs===="
        from WMCore.DataStructs.Fileset import Fileset
        self.fileset = Fileset(name="unittest")
        assert len(self.fileset.listLFNs()) == 0, "New fileset is not empty"
    
    def createFile(self, thelfn):
        from WMCore.DataStructs.File import File
        return File(lfn=thelfn, size=1000, events=230, run=1234, lumi=1)
    
    def testAddFile(self):
        file1 = self.createFile('lfn://file1')
        file2 = self.createFile('lfn://file2')
        
        file1.setLocation('testse1.cern.ch')
        file2.setLocation('testse1.cern.ch')
        
        self.fileset.addFile(file1)
        assert len(self.fileset.listLFNs()) == 1, \
            "fileset has %s files != 1 (f:%s nf:%s)\n[%s]" % (
                                                len(self.fileset.listLFNs()),
                                                len(self.fileset.files),
                                                len(self.fileset.newfiles),
                                                self.fileset.listLFNs())
            
        self.fileset.addFile(file2)
        assert len(self.fileset.listLFNs()) == 2, \
            "fileset has %s files != 1\n[%s]" % (len(self.fileset.listLFNs()),
                                                 self.fileset.listLFNs())
        self.fileset.commit()    
            
        file1.setLocation('testse2.cern.ch')
        self.fileset.addFile(file1)
        assert len(self.fileset.listLFNs()) == 2, \
            "fileset has %s files != 1\n[%s]" % (len(self.fileset.listLFNs()),
                                                 self.fileset.listLFNs())
            
        for i in self.fileset.listFiles():
            print i.dict['lfn'],i.dict['locations']
            
        """
        file3 is the same as file1 but is a new object, instead of an updated 
        reference
        """
        #pdb.set_trace()
        file3 = self.createFile('lfn://file1')
        file3.setLocation('testse3.cern.ch')
        self.fileset.addFile(file3)
        self.fileset.commit()
        print self.fileset.listLFNs()
        assert len(self.fileset.listLFNs()) == 2, \
            "fileset has %s files != 2\n[%s]" % (len(self.fileset.listLFNs()),
                                                 self.fileset.listLFNs())
            
        for i in self.fileset.listFiles():
            print i.dict['lfn'],i.dict['locations']
        
    #def testAddListOfFiles(self):
    #    pass
    #def testAddSetOfFiles(self):
    #    pass

    
class WMBSTestCase(DataStructsTestCase):
    def setUp(self):
        print "\n====WMBS===="
        from WMCore.WMBS.Fileset import Fileset
        from WMCore.Database.DBFactory import DBFactory
        from WMCore.DAOFactory import DAOFactory
        logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')
        
        self.logger = logging.getLogger('unit_test')
        
        self.dbf = DBFactory(self.logger, 'sqlite:///db_filesettest.lite')
        
        dao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
                   dbinterface=self.dbf.connect())
        
        dao(classname='CreateWMBS').execute()
        
        self.fileset = Fileset(name="unittest", logger=self.logger, 
                               dbfactory = self.dbf)
        self.fileset.commit()
        
        dao(classname='Locations.New').execute('testse1.cern.ch')
        dao(classname='Locations.New').execute('testse2.cern.ch')
        dao(classname='Locations.New').execute('testse3.cern.ch')
        
        assert len(self.fileset.listLFNs()) == 0, "New fileset is not empty"
   
    def tearDown(self):
        try:
            self.logger.debug(os.remove('db_filesettest.lite'))
            print "database torn down"
        except OSError:
            #Don't care if the file doesn't exist
            pass
    
    def createFile(self, thelfn):
        from WMCore.WMBS.File import File
        file = File(lfn=thelfn, size=1000, events=230, cksum=1, 
                    logger=self.logger, dbfactory = self.dbf)
        file.addRun(Run(10, *[12312]))

        file.save()
        return file
     
if __name__ == "__main__":
    unittest.main()
