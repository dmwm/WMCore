#/usr/bin/env python
"""
_Destroy_

Implementation of DBSBuffer.Destroy for SQLite
"""

__revision__ = "$Id: Destroy.py,v 1.1 2009/07/13 19:44:27 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $s"

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
        
        self.create["08dbsbuffer_dataset"] = "DROP TABLE dbsbuffer_dataset" 
        self.create["07dbsbuffer_algo"] = "DROP TABLE dbsbuffer_algo"
        self.create["06dbsbuffer_algo_dataset_assoc"] = "DROP TABLE dbsbuffer_algo_dataset_assoc"
        self.create["05dbsbuffer_file"] = "DROP TABLE dbsbuffer_file"
        self.create["04dbsbuffer_file_parent"] = "DROP TABLE dbsbuffer_file_parent"
        self.create["03dbsbuffer_file_runlumi_map"] = "DROP TABLE dbsbuffer_file_runlumi_map"
        self.create["02dbsbuffer_location"] = "DROP TABLE dbsbuffer_location"
        self.create["01dbsbuffer_file_location"] = "DROP TABLE dbsbuffer_file_location"
