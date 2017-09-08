#!/usr/bin/env python
"""
Test that the following dburl's are correctly made:

            postgres://username:password@host:port/database

            mysql://host/database
            mysql://username@host/database
            mysql://username:password@host:port/database

            oracle://username:password@tnsName
            oracle://username:password@host:port/sidname
"""
from __future__ import print_function





import unittest
import logging

from WMCore.Database.DBFactory import DBFactory

class DBFactoryTest(unittest.TestCase):
    def setUp(self):
        "make a logger instance and create tables"

        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')

        self.logger = logging.getLogger('DBFactoryTest')

    def urlTest(self, url, testurl):
        self.asserEqual(url ,testurl, '%s is incorrect should be %s' % (url, testurl))
        return True

    def genericTest(self, testname=None, testoptions={}, testurl=None):
        try:
            dbf = DBFactory(self.logger, options=testoptions)
            if self.urlTest(dbf.dburl, testurl):
                print(testname, " : ", testurl, "  .... OK!")
            else:
                print(testname, " : ", testurl, "  .... FAIL!")
        except Exception as e:
            print(testname, " : ", testurl, "  .... EXCEPTION!", e)

    def testAllURLs(self):
        """
        Checks all the url's we use in the tests are valid
        """

        urls = [
                'postgres://username:password@host:1234/database',
                'mysql://host/database',
                'mysql://username@host/database',
                'mysql://username:password@host:1234/database',
                'oracle://username:password@tnsName',
                'oracle://username:password@host:1234/sidname'
                ]
        for testurl in urls:
            try:
                dbf = DBFactory(self.logger, dburl=testurl)
                if self.urlTest(dbf.dburl, testurl):
                    print("testAllURLs : ", testurl, "  .... OK!")
                else:
                    print("testAllURLs : ", testurl, "  .... FAIL!")
            except Exception as e:
                print("testAllURLs : ", testurl, "  .... EXCEPTION!", e)

    def testPostGres(self):
        """
        Test that factory correctly makes:
            postgres://username:password@host:1234/postgresdb
        """

        self.genericTest(testname='testPostGres',
                         testoptions={'dialect':'postgres',
                                      'database':'database',
                                      'username':'username',
                                      'password':'password',
                                      'host':'host',
                                      'port':1234},
                         testurl='postgres://username:password@host:1234/database')

    def testMySQL(self):
        """
        Test that factory correctly makes:
            mysql://host/database
            mysql://username@host/database
            mysql://username:password@host:1234/database
        """

        self.genericTest(testname='testMySQL',
                         testoptions={'dialect':'mysql',
                                      'host':'host',
                                      'database':'database'},
                         testurl='mysql://host/database')

        self.genericTest(testname='testMySQL',
                         testoptions={'dialect':'mysql',
                                      'host':'host',
                                      'username':'username',
                                      'database':'database'},
                         testurl='mysql://username@host/database')

        self.genericTest(testname='testMySQL',
                         testoptions={'dialect':'mysql',
                                      'host':'host',
                                      'username':'username',
                                      'password':'password',
                                      'database':'database',
                                      'port':1234},
                         testurl='mysql://username:password@host:1234/database')

    def testOracle(self):
        """
        Test that factory correctly makes:
            oracle://username:password@tnsName
            oracle://username:password@host:1234/sidname
        """

        self.genericTest(testname='testOracle',
                         testoptions={'dialect':'oracle',
                                      'sidname':'sidname',
                                      'username':'username',
                                      'password':'password',
                                      'tnsName':'tnsName'},
                         testurl='oracle://username:password@tnsName')

        self.genericTest(testname='testOracle',
                         testoptions={'dialect':'oracle',
                                      'sid':'sidname',
                                      'username':'username',
                                      'password':'password',
                                      'host':'host',
                                      'port':1234},
                         testurl='oracle://username:password@host:1234/sidname')

        self.genericTest(testname='testOracle',
                         testoptions={'dialect':'oracle',
                                      'database':'sidname',
                                      'username':'username',
                                      'password':'password',
                                      'host':'host',
                                      'port':1234},
                         testurl='oracle://username:password@host:1234/sidname')




if __name__ == "__main__":
    unittest.main()
