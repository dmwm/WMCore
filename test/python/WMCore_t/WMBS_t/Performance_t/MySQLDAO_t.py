#!/usr/bin/env python

"""
__MySQLDAOTest__

DB Performance testcase for WMBS File class


"""

import commands
from ConfigParser import ConfigParser  
class MySQLDAOTest():
    """
    __MySQLDAOTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        """
        MySQL specific setUp method

        """    
        cfg = ConfigParser()
        cfg.read('test.ini')
        #Set specific user for mysqladmin here        
        self.logname = 'MySQL'
        self.dbuser = cfg.get('mysql', 'user')
        self.dbpass = cfg.get('mysql', 'pass')
        self.dbhost = cfg.get('mysql', 'host')
        self.dbinst = cfg.get('mysql', 'instance')
        self.verbose = cfg.get('output','verbose')
        self.sqlURI = 'mysql://%s:%s@%s/%s' % (self.dbuser, self.dbpass, 
                                                self.dbhost, self.dbinst)

    def tearDown(self):
        """
        MySQL specific tearDown Method

        """
        #DB Specific tearDown code        
        if self.dbpass != '':        
            self.logger.debug(
              commands.getstatusoutput('echo yes | mysqladmin --user=%s \
                                        --password=%s drop %s' %\
                                       (self.dbuser, self.dbpass, self.dbinst)))
            self.logger.debug(
              commands.getstatusoutput('mysqladmin --user=%s --password=%s \
                                        create %s' %\
                                       (self.dbuser, self.dbpass, self.dbinst)))
        else:
            self.logger.debug(
              commands.getstatusoutput('echo yes | mysqladmin --u %s drop %s' %\
                                       (self.dbuser, self.dbinst)))
            self.logger.debug(
              commands.getstatusoutput('mysqladmin -u %s create %s' %\
                                       (self.dbuser, self.dbinst)))

        self.logger.debug("WMBS MySQL database deleted")

