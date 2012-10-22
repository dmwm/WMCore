#!/usr/bin/env python
"""
_ConfigureState_

Populate the states tables with all known states, and set the max retries for
each state. Default to one retry.
Create the CouchDB and associated views if needed.
"""




from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject

class ConfigureState(WMObject):
    def configure(self):
        server = CouchServer(self.config.JobStateMachine.couchurl)
        dbname = 'JSM/JobHistory'
        if dbname not in server.listDatabases():
            server.createDatabase(dbname)
