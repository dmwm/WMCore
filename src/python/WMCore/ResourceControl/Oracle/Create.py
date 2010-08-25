#!/usr/bin/python
"""
_Create_

Class for creating Oracle specific schema for resource control.
"""




import threading

from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    _Create_
    
    Class for creating Oracle specific schema for resource control.
    """
    def __init__(self, **params):
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.create = {}
        self.constraints = {}

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if params.has_key("tablespace_table"):
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if params.has_key("tablespace_index"):
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        self.create["rc_threshold"] = """
        CREATE TABLE rc_threshold(
            site_id INTEGER NOT NULL,
            sub_type_id INTEGER NOT NULL,
            max_slots INTEGER NOT NULL) %s""" % tablespaceTable

        self.constraints["rc_threshold_fk1"] = \
          """ALTER TABLE rc_threshold ADD
               (CONSTRAINT rc_threshold_fk1 FOREIGN KEY (site_id) REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

        self.constraints["rc_threshold_fk2"] = \
          """ALTER TABLE rc_threshold ADD
               (CONSTRAINT rc_threshold_fk2 FOREIGN KEY (sub_type_id) REFERENCES wmbs_sub_types(id) ON DELETE CASCADE)"""               
