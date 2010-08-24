#!/usr/bin/env python

import os, unittest, logging, commands, time


from unittest import TestCase
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory

class Base_t(TestCase):
    """
    __Base_t__

    Base class for DB Performance at WMBS


    """
    def setUp(self):        
        self.classType = 'FilePerfTest'
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        
        self.logger = logging.getLogger('DBPerformanceTest')

    def getClassNames(self, dirname='.'):
        files = os.listdir(dirname)
        list = []
        for x in files:
            #Hack - Only get .py files and no __init__.py,etc...
            if (x[-3:] == '.py') and (x[0] != '_'):
                parts = x.split('.')
                list.append(parts[0])
        return list
            
    def perfDAOClassName(self,classname=None, threshold=1, DBType='MySQL'):
        
        # Make up the URI, different for each database you may want tif __name__ == "__main__":
        #TODO - get a function to ignore case for DBType    
        if DBType == 'MySQL':
            sqlURI = 'mysql://jcg@localhost/wmbs'
        elif DBType == 'SQLite':
            sqlURI = 'sqlite:///fileperftest.lite'
        #elif DBType == 'Oracle':
            #sqlURI = 'Add Oracle URI here'
        #TODO - Alter Exception to make it uniform with WMException
        else:        
            raise Exception, "Invalid DBType: "+DBType
    
        daologger = logging.getLogger(DBType+'PerformanceLogger')

        dbf = DBFactory(self.logger, sqlURI)        

        daofactory = DAOFactory(package='WMCore.WMBS', logger=daologger, dbinterface=dbf.connect())        
        
        #Test each of the DAO classes of the specific WMBS class directory        
        print classname+' Performance Test ( '+DBType+' )'     
        startTime = time.time()               

        #Place execute method of the specific classname here
        #TODO - Figure out how to handle the specific arguments for each class            
        #daofactory(classname=x).execute()
            
        endTime = time.time()
        diffTime = endTime - startTime
        print classname+' DAO class performance test:'+str(diffTime)
        assert diffTime <= threshold, classname+' DAO class - Operation too slow ( elapsed time:'+str(diffTime)+', threshold:'+str(threshold)+' )'
     
    def runTest(self):
        #Construct the directory name string, based on DB type and WMBS class you may want to test        
        ClassList = ['Files', 'Fileset', 'Jobs', 'JobGroup', 'Workflow', 'Subscriptions', 'Locations']          
        DBList = ['MySQL','SQLite']        
        for wmbsclass in ClassList:
            print wmbsclass+' WMBS Class Performance Test'
            for dbtype in DBList:
                dirname = '/home/jcg/workspace/WMCoreCERN2/src/python/WMCore/WMBS/'+dbtype+'/'+wmbsclass
                daoclasslist = self.getClassNames(dirname)
                for daoclass in daoclasslist:                
                    self.perfDAOClassName(daoclass, dbtype)

if __name__ == "__main__":
    unittest.main() 
            
        
        
