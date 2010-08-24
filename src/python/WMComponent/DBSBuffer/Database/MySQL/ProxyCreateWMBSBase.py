"""
_ProxyCreateWMBS_

Base class for creating the WMBS database proxy
(proxy in the sense that when REAL wmbs will be 
in play this file will be removed and actual 
wmbs tables will be used)

"""

__revision__ = "$Id: ProxyCreateWMBSBase.py,v 1.2 2008/12/12 19:23:57 afaq Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "anzar@fnal.gov"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class ProxyCreateWMBSBase(DBCreator):    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi
            
        DBCreator.__init__(self, logger, dbi)
                
        self.requiredTables = [
                               "02wmbs_file_details",
                               "04wmbs_file_parent",
                               "05wmbs_file_runlumi_map",
                               "06wmbs_location",
                               "07wmbs_file_location",
				]
        
        
        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
             id           INTEGER      PRIMARY KEY AUTO_INCREMENT,
             lfn          VARCHAR(255) NOT NULL,
             size         INTEGER,
             events       INTEGER,
             cksum        INTEGER,
             first_event  INTEGER,
             last_event   INTEGER)ENGINE=InnoDB"""
        
        self.create["04wmbs_file_parent"] = \
          """CREATE TABLE wmbs_file_parent (
             child  INTEGER NOT NULL,
             parent INTEGER NOT NULL,
             FOREIGN KEY (child)  references wmbs_file_details(id)
               ON DELETE CASCADE,
             FOREIGN KEY (parent) references wmbs_file_details(id),
             UNIQUE(child, parent))ENGINE=InnoDB"""  
        
        self.create["05wmbs_file_runlumi_map"] = \
          """CREATE TABLE wmbs_file_runlumi_map (
             file    INTEGER NOT NULL,
             run     INTEGER NOT NULL,
             lumi    INTEGER NOT NULL,
             FOREIGN KEY (file) references wmbs_file_details(id)
               ON DELETE CASCADE)ENGINE=InnoDB"""
        
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
             id      INTEGER      PRIMARY KEY AUTO_INCREMENT,
             se_name VARCHAR(255) NOT NULL,
             UNIQUE(se_name))ENGINE=InnoDB"""
             
        self.create["07wmbs_file_location"] = \
          """CREATE TABLE wmbs_file_location (
             file     INTEGER NOT NULL,
             location INTEGER NOT NULL,
             UNIQUE(file, location),
             FOREIGN KEY(file)     REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location) REFERENCES wmbs_location(id)
               ON DELETE CASCADE)ENGINE=InnoDB"""
        
    def execute(self, conn=None, transaction=None):
        """
        _execute_
        
        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in self.requiredTables:
            if requiredTable not in self.create.keys():
                raise WMException("The table '%s' is not defined." % \
                                  requiredTable, "WMCORE-2")
                   
        try:
            DBCreator.execute(self, conn, transaction)
            return True
        except Exception, e:
            print "ERROR: %s" % e
            return False
    
