#!/usr/bin/env python
"""
_DBPerformance_t_

Unit performance tests for database Performance

TODO: Make the performance test more general, for any of the DBs supported,
using the DAO.

TODO: Validate the algorithm used for testing, checking if it has any errors,
e.g. method overhead, python interpreter slowness,....

"""

import commands
import unittest
import logging
import os
import time
import random

from unittest import TestCase
from sqlalchemy import create_engine
from sqlalchemy.exceptions import OperationalError
from WMCore.Database.DBFactory import DBFactory

class DBPerformanceTest(TestCase):

    def setUp(self):
        """
        Initial settings for the DB Performance tests
        
        """
        "make a logger instance and create tables"
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        
        self.logger = logging.getLogger('DBPerformanceTest')
        
        self.sqlitedb = 'transaction_test.lite'
        self.mysqldb = 'transaction_test'
        
        self.tearDown()
        self.db = {}
        self.db['mysql'] = DBFactory(self.logger,
                         'mysql://jcg@localhost/%s' % self.mysqldb).connect()
        self.db['sqlite'] = DBFactory(self.logger,
                         'sqlite:///%s' % self.sqlitedb).connect()
        #add in Oracle
            
    def tearDown(self):
        """
        Delete the databases
        """
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u jcg drop %s' % self.mysqldb))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u jcg create %s' % self.mysqldb))
        self.logger.debug("mysql database deleted")
        try:
            self.logger.debug(os.remove(self.sqlitedb))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        
    def runTest(self, sql, binds={}):
        """
        Simple sql query runner on binds
           
        """
        result = {}
        for name, dbi in self.db.items():
            result[name] = dbi.processData(sql, binds)
        return result

    def createTable(self):
        """
        Creates initial table for DB testing

        """
        try:
            #Create a table with two rows
            #TODO - Generalize this solution for n rows and all kinds of values            
            sqlquery = "create table test (bind1 varchar(20), bind2 varchar(20))"
            self.runTest(sqlquery)
        except OperationalError, oe:
            print oe

    def perfSQL(self, sql, binds={}):
        """
        Performance test for generic SQL queries
        Returns the total time of the query

        """        
        #Creating the SQL query strings                           
        startTime = time.time()               
        self.runTest(sql, binds)
        endTime = time.time()
        diffTime = endTime - startTime
        return diffTime

    def genValues(self, iterator=2):
        """
        Generator of values for usage with Insert tests
        Returns a list of dictionary structures

        """        
        values = []        
        for i in range(1,iterator):                    
                values.append({'bind1':'value'+str(random.randint(1000, 9999)), 
                    'bind2': 'value'+str(random.randint(1000, 9999))})   
        return values

    def testDBPerformance(self, value=3, multiplier=10, increment=20, times=1000, threshold=0.8):
        """
        Performance test for Basic DB Operations
        
        An initial test, just to illustrate the possible DB performance testing
        issues. Coded it by hand in Python, without using PyUnitPerf or HotShot
        Perhaps we can take this idea and see how it applies to both of these
        other solutions.
        
        The number of inserts increase like an exponential rate, the
        count variable being the factor increment. This is just an example,
        another methods of simulating DB behavior can be used.
        
        value: initial constant value
        multiplier: base constant multiplier
        increment: increment made to the multiplier at each loop
        times: How many times will this function loop. Each loop is composed of
        a series of Inserts and a Select in the end
        threshold: the maximum time an operation should take, in seconds

        TODO: Encapsulate this test in generic classes to be used as a
              generic DB performance testing tool
        """
        #Preparing Performance test
        b = []
        sqlInsert = "insert into test (bind1, bind2) values (:bind1, :bind2)"        
        sqlSelect = "select * from test"
        #Restarting the DB State        
        self.tearDown()                 
        self.createTable() 
        #Iterate through each of the benchmarking points        
        total = 0        
        #Iterate the increment factor of the inserts        
        for n in range(1,times):                      
            #Generate the dict list of the values to be inserted             
            b = self.genValues(value * multiplier)
            total = total + value * multiplier
            # Insert Performance results
            diffTime = self.perfSQL(sqlInsert, b)
            print'Multiple Insert test time ('+str(total)+' rows): %f sec' % diffTime        
            assert diffTime <= threshold, 'Multiple Insert Test failed' \
                '('+str(total)+' rows) - Operation too slow.'        
            #Insert Performance results
            diffTime = self.perfSQL(sqlSelect)
            print'Select test time ('+str(total)+' rows): %f sec' % diffTime        
            assert diffTime <= threshold, 'Select Test failed' \
                '('+str(total)+' rows), %f sec - Operation too slow.' % diffTime
            #Updating the counters
            multiplier = multiplier + increment

if __name__ == "__main__":
    unittest.main()    

