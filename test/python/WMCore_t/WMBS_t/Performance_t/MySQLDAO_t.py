#!/usr/bin/env python

import commands

class MySQLDAO_t():
    """
    __MySQLDAO_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        #Set specific user for mysqladmin here        
        self.dbuser = 'jcg'
        self.logname = 'MySQL'
        self.sqlURI = 'mysql://'+self.dbuser+'@localhost/wmbs'

    def tearDown(self):
        #Call superclass tearDown method
        #DB Specific tearDown code        
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u '+self.dbuser+' drop wmbs'))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u '+self.dbuser+' create wmbs'))
        self.logger.debug("WMBS MySQL database deleted")

