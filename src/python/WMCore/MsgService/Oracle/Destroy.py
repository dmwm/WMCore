#!/usr/bin/python
#pylint: disable-msg=E1103

"""
_Destroy_

Class for destroying Oracle specific schema for the trigger

"""

__revision__ = "$Id: Destroy.py,v 1.2 2009/06/16 14:46:17 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMCore.Database.DBCreator import DBCreator
from WMCore.MsgService.MySQL.Destroy import Destroy as MySQLDestroy
from WMCore.MsgService.Oracle.Create import Create

class Destroy(MySQLDestroy):
    """
    _Create_
    
    Class for destroying Oracle specific schema for the trigger.
    """
    
    
    
    def __init__(self, logger = None, dbi = None):
        myThread = threading.currentThread()
        MySQLDestroy.__init__(self, logger, dbi)




        #self.create['z_ms_process'] = "DROP TABLE ms_process"
        #self.create['y_ms_process_seq'] = "DROP SEQUENCE MS_PROCESS_SEQ1"
        #self.create['x_ms_type'] = "DROP TABLE ms_type"
        #self.create['w_ms_type_seq'] = "DROP SEQUENCE MS_TYPE_SEQ1"

        j = 50
        for i in Create.sequence_tables:
            seqname = i
            self.create["%s%s" % (j, seqname)] = \
                           "DROP SEQUENCE %s"  % seqname
