#!/usr/bin/env python

import commands, os
from ConfigParser import ConfigParser
class SQLiteDAOTest():
    """
    __SQLiteDAOTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        cfg = ConfigParser()
        cfg.read('sqlite.ini')
        #Set specific user for mysqladmin here        
        self.logname = 'SQLite'
        self.dbuser = cfg.get('database', 'user')
        self.dbhost = cfg.get('database', 'host')
        self.dbinst = cfg.get('database', 'instance')
        self.verbose = cfg.get('output','verbose')
        self.sqlURI = 'sqlite:///%s' % (self.dbinst+'.lite') 

    def tearDown(self):
        #Call superclass tearDown method
        #DB Specific tearDown code        
        try:
            self.logger.debug(os.remove(self.dbinst+'.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.logger.debug("WMBS SQLite database deleted")

