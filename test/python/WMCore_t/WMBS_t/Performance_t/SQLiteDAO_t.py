#!/usr/bin/env python

import commands,os

class SQLiteDAO_t():
    """
    __SQLiteDAO_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        #Set specific user for mysqladmin here        
        self.logname = 'MySQL'
        self.sqlURI = 'sqlite:///dbperftest.lite'

    def tearDown(self):
        #Call superclass tearDown method
        #DB Specific tearDown code        
        try:
            self.logger.debug(os.remove('dbperftest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.logger.debug("WMBS SQLite database deleted")

