"""
_Create_

Implementation of CreateAgent for SQLite.

"""

__revision__ = "$Id: Create.py,v 1.1 2010/06/21 21:18:53 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.CreateAgentBase import CreateAgentBase

class Create(CreateAgentBase):
    """
    Class to set up the Agent schema in a MySQL database
    """
    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateAgentBase.__init__(self, logger, dbi, params)
             
        self.create["02wm_workers"] = \
          """CREATE TABLE wm_workers (
             component_id  INTEGER NOT NULL REFERENCES wm_components(id),
             name          VARCHAR(255) NOT NULL,
             last_updated  INTEGER      NOT NULL,
             state         VARCHAR(255),
             pid           INTEGER,
             UNIQUE (component_id, name))"""
         
        # constraints added in table definition
        self.constraints.clear()    