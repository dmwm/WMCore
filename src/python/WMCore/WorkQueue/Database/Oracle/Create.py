"""
_CreateWorkQueue_

Implementation of CreateWorkQueue for Oracle.

Inherit from CreateWorkQueue, and add Oracle specific substitutions (e.g. 
use trigger and sequence to mimic auto increment in MySQL.
"""

__revision__ = "$Id: Create.py,v 1.4 2009/07/10 15:43:20 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WorkQueue.Database.CreateWorkQueueBase import CreateWorkQueueBase

class Create(CreateWorkQueueBase):
    """
    Class to set up the WMBS schema in a MySQL database
    """
    sequenceTables = ["wq_wmspec",
                      "wq_block",
                      "wq_element"]
    seqStartNum = 40
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """        
        CreateWorkQueueBase.__init__(self, logger, dbi)
        
        for tableName in self.sequenceTables:
            seqname = '%s_SEQ' % tableName
            self.create["%s%s" % (self.seqStartNum, seqname)] = """
            CREATE SEQUENCE %s start with 1 
            increment by 1 nomaxvalue cache 100""" % seqname
            
            triggerName = '%s_TRG' % tableName
            self.create["%s%s" % (self.seqStartNum, triggerName)] = """
                    CREATE TRIGGER %s
                        BEFORE INSERT ON %s
                        REFERENCING NEW AS newRow
                        FOR EACH ROW
                        BEGIN
                            SELECT %s.nextval INTO :newRow.id FROM dual;
                        END; """ % (triggerName, tableName, seqname)
