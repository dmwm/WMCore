#/usr/bin/env python
"""
_Destroy_

Implementation of DBSBuffer.Destroy for SQLite
"""

__revision__ = "$Id: Destroy.py,v 1.4 2009/12/02 20:14:59 mnorman Exp $"
__version__ = "$Revision: 1.4 $s"

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


        self.delete["11dbsbuffer_block"]              = "DROP TABLE dbsbuffer_block"
        self.delete["10dbsbuffer_dataset"]            = "DROP TABLE dbsbuffer_dataset" 
        self.delete["09dbsbuffer_algo"]               = "DROP TABLE dbsbuffer_algo"
        self.delete["08dbsbuffer_algo_dataset_assoc"] = "DROP TABLE dbsbuffer_algo_dataset_assoc"
        self.delete["07dbsbuffer_file"]               = "DROP TABLE dbsbuffer_file"
        self.delete["06dbsbuffer_file_parent"]        = "DROP TABLE dbsbuffer_file_parent"
        self.delete["05dbsbuffer_file_runlumi_map"]   = "DROP TABLE dbsbuffer_file_runlumi_map"
        self.delete["04dbsbuffer_location"]           = "DROP TABLE dbsbuffer_location"
        self.delete["03dbsbuffer_file_location"]      = "DROP TABLE dbsbuffer_file_location"
        self.delete["02dbsbuffer_checksum_type"]      = "DROP TABLE dbsbuffer_checksum_type"
        self.delete["01dbsbuffer_file_checksums"]     = "DROP TABLE dbsbuffer_file_checksums"

