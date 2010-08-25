#/usr/bin/env python
"""
_Destroy_

Implementation of DBSBuffer.Destroy for Oracle
"""

__revision__ = "$Id: Destroy.py,v 1.2 2009/08/12 17:24:51 meloam Exp $"
__version__ = "$Revision: 1.2 $s"

import threading

from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    def __init__(self):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        
        self.delete["08dbsbuffer_dataset"] = "DROP TABLE dbsbuffer_dataset" 
        self.delete["07dbsbuffer_algo"] = "DROP TABLE dbsbuffer_algo"
        self.delete["06dbsbuffer_algo_dataset_assoc"] = "DROP TABLE dbsbuffer_algo_dataset_assoc"
        self.delete["05dbsbuffer_file"] = "DROP TABLE dbsbuffer_file"
        self.delete["04dbsbuffer_file_parent"] = "DROP TABLE dbsbuffer_file_parent"
        self.delete["03dbsbuffer_file_runlumi_map"] = "DROP TABLE dbsbuffer_file_runlumi_map"
        self.delete["02dbsbuffer_location"] = "DROP TABLE dbsbuffer_location"
        self.delete["01dbsbuffer_file_location"] = "DROP TABLE dbsbuffer_file_location"

        self.delete["01dbsbuffer_dataset_seq"] = "DROP SEQUENCE dbsbuffer_dataset_seq"
        self.delete["02dbsbuffer_algo_seq"] = "DROP SEQUENCE dbsbuffer_algo_seq"
        self.delete["03dbsbuffer_file_seq"] = "DROP SEQUENCE dbsbuffer_file_seq"
        self.delete["04dbsbuffer_location_seq"] = "DROP SEQUENCE dbsbuffer_location_seq"
        self.delete["05dbsbuffer_algo_dataset_assoc_seq"] = "DROP SEQUENCE dbsbuffer_algdset_assoc_seq"

        self.delete["01dbsbuffer_dataset_trg"] = "DROP TRIGGER dbsbuffer_dataset_trg"
        self.delete["02dbsbuffer_algo_trg"] = "DROP TRIGGER dbsbuffer_algo_trg"
        self.delete["03dbsbuffer_file_trg"] = "DROP TRIGGER dbsbuffer_file_trg"
        #self.create["04dbsbuffer_location_trg"] = "DROP TRIGGER dbsbuffer_location_trg"
        self.delete["05dbsbuffer_algo_dataset_assoc_trg"] = "DROP TRIGGER dbsbuffer_algdset_assoc_trg"
