#!/usr/bin/env python

import unittest
from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t

class Location_t(Base_t,TestCase):
    """
    __MySQLDAOLocation_t__

     MySQL DAO Performance testcase for Location DAO Class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testNew(self):         
        print "testNew"
        
        time = self.perfTest(dao=self.mysqldao, action='Locations.New', execinput=['sename="TestLocation"'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testList(self):         
        print "testList"
        
        time = self.perfTest(dao=self.mysqldao, action='Locations.List', execinput='')
        assert time <= self.threshold, 'List DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):         
        print "testDelete"
        
        time = self.perfTest(dao=self.mysqldao, action='Locations.Delete', execinput=['sename="TestLocation"'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

#    def testFiles(self):         
#        print "testFiles"
#        
#        time = self.perfTest(dao=self.mysqldao, action='Locations.Files', execinput=['sename="TestLocation"'])
#        assert time <= self.threshold, 'Files DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


if __name__ == "__main__":
    unittest.main()
