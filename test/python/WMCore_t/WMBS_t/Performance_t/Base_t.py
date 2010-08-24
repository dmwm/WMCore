#!/usr/bin/env python

from unittest import TestCase

import os

class Base_t(TestCase):
"""
__Base_t__

Base class for DB Performance at WMBS

TODO - Need to figure out a way to get each WMBS class and relate it to its
DAO classnames

"""
    def setUp(self):        
        self.classType = 'FilePerfTest'
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        
        self.logger = logging.getLogger('DBPerformanceTest')

    def getClassnames(self, dirname='.'):
        files = os.commands.listdir(dirname)
        list = []
        for x in files:
            #Only get .py files and no __init__.py,etc...
            if (x[-3:] == '.py') and (x[0] != '_'):
                parts = x.split('.')
                list.append(parts[0])
        return list
            
    def perfDB(self,classname=None, threshold=1, DBType='MySQL')
        
        # Make up the URI, different for each database you may want to test
        #TODO - get a function to ignore case for DBType    
        if DBType == 'MySQL':
            sqlURI = 'mysql://jcg@localhost/wmbs'
        elif DBType == 'SQLite':
            sqlURI = 'sqlite:///fileperftest.lite'
        #elif DBType == 'Oracle':
            #sqlURI = 'Add Oracle URI here'
        #TODO - Throw Exception if wrong DBType
    
        daologger = logging.getLogger(DBType+'PerformanceLogger')

        dbf = DBFactory(logger=self.logger, sql=sqlURI)        

        daofactory = DAOFactory(package='WMCore.WMBS', logger=daologger, dbinterface=dbf.connect())        
        
        #Test each of the DAO classes of the specific WMBS class directory        
        print classname+' Performance Test ( '+DBType+' )     
        startTime = time.time()               

        #Place execute method of the specific classname here
        #TODO - Figure out how to handle the specific arguments for each class            
        #daofactory(classname=x).execute()
            
        endTime = time.time()
        diffTime = endTime - startTime
        print classname+' DAO class performance test:'+str(diffTime)
        assert difftime <= threshold, classname+' DAO class - Operation too slow ( elapsed time:'+str(diffTime)+', threshold:'+str(threshold)+' )'
     
    def runTest(self):
        #Construct the directory name string, based on DB type and WMBS class you may want to test        
        DBList = ['MySQL','SQLite']        
        for dbtype in DBList:        
            dirname = '/home/jcg/workspace/WMCoreCERN2/src/python/WMCore/'+dbtype+'/'+self.classType
            classlist = self.getClassNames(dirname)
            for daoclass in classlist:                
                self.perfClassName(daoclass, dbtype)
            
        
        
