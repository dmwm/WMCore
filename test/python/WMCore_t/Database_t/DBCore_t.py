#!/usr/bin/env python
"""
_DBCore_t_

Unit tests for the DBInterface class

"""

__revision__ = "$Id: DBCore_t.py,v 1.5 2010/02/10 03:52:27 meloam Exp $"
__version__ = "$Revision: 1.5 $"

import commands
import unittest
import logging
import threading
import os
import nose

from sqlalchemy import create_engine
from sqlalchemy.exceptions import OperationalError
from WMCore.Database.DBFactory import DBFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.Database.DBCore import DBInterface

class DBCoreTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                    useDefault = False)

        return
            
    def tearDown(self):
        """
        Delete the databases
        """
        self.testInit.clearDatabase()
        return

    def testConnection(self):
        """
        Test class for the connection functions for DBCore
        
        """

        #This function tests for two things:
        # a) Did it actually manage to get one of the tables from the dataset?
        # b) Was the table blank?
        # This assumes that you use the default WMBS table setup that I copied over
        # In the future we may want to change the order to create a table first.
        # -mnorman
    
        print "testConnection"

        myThread = threading.currentThread()

        testInterface = myThread.dbi
        connection = testInterface.connection()
        excepted   = False
        dbOutput   = []

        try:
            #This depends on the WMBS database setup format
            result = connection.execute('select * from wmbs_fileset')
        except OperationalError, oe:
            excepted = True
            print oe

        
        #This is a clumsy way to do this, but unittest lacks an inbuilt assert for exceptions
        #assertRaises uses reverse logic for what we need to do.
        self.assertEqual(excepted, False)

        self.assertEqual(result.fetchall(), dbOutput)

        return



    def testBuildBinds(self):
        """
        Test class for DBCore.buildbinds()

        """

        #This class may become obselete soon.  There is a TODO stuck onto DBCore.buildbinds()
        #This just checks to see that the sequence properly packages the first value is set
        #So that it sets the key seqname to the proper name in the files list
        #Also right now tests that it sets the keys right in each dict, but this seems redundant
        # -mnorman

        print "testBuildBinds"

        seqname       = 'file'
        dictbinds     = {'lumi':123, 'test':100}
        files         = ['testA', 'testB', 'testC']

        myThread = threading.currentThread()

        testInterface = myThread.dbi

        binds         = testInterface.buildbinds(files, seqname, dictbinds)

        #This should return a dict for every value in files with additional elements
        #from dictbinds.  We then loop over every dict (which should be equal to the
        #number of elements in files), and look to see that the filename matches and that
        #At least one of the dictbinds keys matches to its proper element

        for i in range(len(files)):
            self.assertEqual(binds[i][seqname], files[i])
            self.assertEqual(binds[i][dictbinds.keys()[0]], dictbinds[dictbinds.keys()[0]])


        return





    def testProcessDataCreateDelete(self):
        """
        Test whether we can create and delete a table with processData()

        """
      
        #Putting this in a seperate function allows tearDown to delete the test table
        #in other lines of code, removing the problem of adding the test table multiple times
        #should an error occur

        print "testProcessDataCreateDelete"


        myThread = threading.currentThread()

        testInterface = myThread.dbi

        binds         = None
        sql           = ""
        failed        = False
        blank         = None
        times         = 501

        #Table creation may be dialect dependent
        if self.mydialect.lower() == 'mysql':
            sql = "create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB "
        elif self.mydialect.lower() == 'sqlite':
            sql = "create table test (bind1 varchar(20), bind2 varchar(20))"
        else:
            sql = "create table test (bind1 varchar(20), bind2 varchar(20))"


        #Can we create a table with it?
        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())

        
        #Test if we can delete the table
        sql    = 'drop table test'
        result = myThread.dbi.processData(sqlstmt = sql, binds = blank, conn = myThread.dbi.connection())

        return


    def testProcessData(self):
        """
        Test function for DBCore.processData

        """
    
        # We have opted to put the main testing functions in here
        # This test creates a test table, inserts and selects elements
        # from that table, inserts and selects elements from a dictionary
        # as a bind, from a list of dictionaries used as binds, and then
        # from an extremely long list of dictionaries
        # -mnorman

        print "testProcessData"


        myThread = threading.currentThread()

        testInterface = myThread.dbi

        binds         = None
        sql           = ""
        failed        = False
        blank         = None
        times         = 501

        #Table creation may be dialect dependent
        if self.mydialect.lower() == 'mysql':
            sql = "create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB "
        elif self.mydialect.lower() == 'sqlite':
            sql = "create table test (bind1 varchar(20), bind2 varchar(20))"
        else:
            sql = "create table test (bind1 varchar(20), bind2 varchar(20))"


        initialInsert = {'bind1':'value1a', 'bind2': 'value2a'}
        secondInsert  = [ {'bind1':'value3a', 'bind2': 'value2a'},
                          {'bind1':'value3b', 'bind2': 'value2b'},
                          {'bind1':'value3c', 'bind2': 'value2c'}]

        #Can we create a table with it?
        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())
        self.remove_test = True


        #Is the table there?
        sql    = 'select * from test'
        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())


        #Now, can we insert into this table?
        sql           = "insert into test (bind1, bind2) values (:bind1, :bind2)"


        result = myThread.dbi.processData(sqlstmt = sql, binds = initialInsert, conn = myThread.dbi.connection())
        


        #What can we get from this table?

        #Test with a single dictionary
        sql = "select * from test where bind1 = :bind1    "
        binds = {'bind1':initialInsert['bind1']}
        

        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())

        #result should be a list of ResultSets, which should have a list of elements
        self.assertEqual(str(result[0].fetchall()[0][0]), initialInsert['bind1'])

        #Test with two dictionaries.
        sql = "select * from test where bind1 = :bind1    "
        binds = [ {'bind1':initialInsert['bind1']},
                  {'bind1':initialInsert['bind2']} ]
        

        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())

        #result should be a list of ResultSets, which should have a list of elements
        self.assertEqual(str(result[0].fetchall()[0][0]), initialInsert['bind1'])


        #Test looking for the same variable twice.
        #First, can we insert them?
        if not myThread.dialect == 'Oracle':
            #This doesn't work in Oracle, and I think it's Oracle yet: ORA-00957
            sql           = "insert into test (bind1, bind1, bind2) values (:bind1, :bind1, :bind2)"
            binds         = {'bind1':'valueX1', 'bind2': 'valueX2'}


            result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())

        
            sql = "select bind1, bind1, bind2 from test where bind1 = :bind1    "
            binds = [ {'bind1':initialInsert['bind1']},
                      {'bind1':'valueX1'} ]
        

            result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())


            #First and second element should be the same
            self.assertEqual(result[0].fetchall()[0][0], result[0].fetchall()[0][1])


        #Now insert more elements into the table
        #Insert a list of dictionaries (secondInsert)
        sql           = "insert into test (bind1, bind2) values (:bind1, :bind2)"

        result = myThread.dbi.processData(sqlstmt = sql, binds = secondInsert, conn = myThread.dbi.connection())

        #Test multiple selections
        value2 = 'value3a'
        sql = "select * from test where bind1 = :bind1    "
        binds = [ {'bind1':initialInsert['bind1']},
                  {'bind1':value2} ]
        
        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())

        #result should be a list of ResultSets, which should have a list of elements
        self.assertEqual(str(result[0].fetchall()[0][0]), initialInsert['bind1'])
        self.assertEqual(str(result[0].fetchall()[1][0]), value2)

        return




    def testProcessDataMultiple(self):
        """
        Test for inserting and selecting with large numbers of binds using processData()

        """

        print "testProcessDataMultiple"
                

        myThread = threading.currentThread()

        testInterface = myThread.dbi

        binds         = None
        sql           = ""
        failed        = False
        blank         = None
        times         = 501
        initialInsert = {'bind1':'value1a', 'bind2': 'value2a'}

        #Table creation may be dialect dependent
        if self.mydialect.lower() == 'mysql':
            sql = "create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB "
        elif self.mydialect.lower() == 'sqlite':
            sql = "create table test (bind1 varchar(20), bind2 varchar(20))"
        else:
            sql = "create table test (bind1 varchar(20), bind2 varchar(20))"

        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())
        self.remove_test = True




        #Now insert and select a huge number of dictionaries
        #This depends on the times variable
        binds = [ ]
        largeInsert  = [ {'bind1':'value3a', 'bind2': 'value2a'}]
                         
                         

        for i in range(times):
            binds.append({'bind1':'value1' + str(i)})
            largeInsert.append({'bind1':'value1'+str(i), 'bind2': 'value2'+str(i)})

        sql    = "insert into test (bind1, bind2) values (:bind1, :bind2)"
        result = myThread.dbi.processData(sqlstmt = sql, binds = largeInsert, conn = myThread.dbi.connection())

        sql = "select * from test where bind1 = :bind1"      
        result = myThread.dbi.processData(sqlstmt = sql, binds = binds, conn = myThread.dbi.connection())

            
        #Test that we have the right number of results, and that the first one matches
        self.assertEqual(sum([ len(res.fetchall()) for res in result]), times)
        self.assertEqual(result[0].fetchall()[0][0], 'value10')
        

        

        
        return





    

if __name__ == "__main__":
    unittest.main()     
                   
    
