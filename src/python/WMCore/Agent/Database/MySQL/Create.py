"""
_CreateAgent_

Implementation of CreateAgent for MySQL.

Inherit from CreateAgentBase, and add MySQL specific substitutions (e.g. add
INNODB).
"""

from WMCore.Agent.Database.CreateAgentBase import CreateAgentBase


class Create(CreateAgentBase):
    """
    Class to set up the Agent schema in a MySQL database
    """

    def __init__(self, logger=None, dbi=None, params=None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateAgentBase.__init__(self, logger, dbi, params)

    def execute(self, conn=None, transaction=None):
        for i in self.create:
            self.create[i] += " ENGINE=InnoDB ROW_FORMAT=DYNAMIC"
            self.create[i] = self.create[i].replace('INTEGER', 'INT(11)')

        return CreateAgentBase.execute(self, conn, transaction)
