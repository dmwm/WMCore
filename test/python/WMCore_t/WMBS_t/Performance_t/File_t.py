#!/usr/bin/env python

import unittest, time

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t

class File_t(Base_t,TestCase):
    """
    __File_t__

     DB Performance testcase for WMBS File class


    """
    def runTest(self):
        #The directory containing the DAO classes
        wmbsdir = 'Files'
        #Performance threshold, in seconds
        threshold = 1
        #Loop for each classname of File WMBS Object
        print 'File WMBS Class Performance Test'
        #loops for each DB - very hacky way to do it. DBList comes from the Base_t superclass            
        for dbtype in self.DBList:
            #Change this directory string for your own directory
            #TODO - Use Frank's solution for the TestCase suite            
            dirname = '/home/jcg/workspace/WMCoreCERN2/src/python/WMCore/WMBS/'+dbtype+'/'+wmbsdir
            #Automatically gets the classnames to test                
            daoclasslist = self.getClassNames(dirname)
            for daoclass in daoclasslist:
            #Each dao classname has a different input on execute(),
            #so put it on daoexecinput
                if daoclass == 'Example_classname':
                    daoexecinput = 'Example_Execute_Input'
            # ....and so on....for each classname...
            # ......................................
            else:
                raise Exception, 'Unknown DAO classname: '+daoclass                
            self.mysqldao(daoclass)                
            time = self.perfTest(dao=self.mysqldao, execinput=daoexecinput)
            assert time <= threshold, daoclassname+' DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(threshold)+' )'
        
        

