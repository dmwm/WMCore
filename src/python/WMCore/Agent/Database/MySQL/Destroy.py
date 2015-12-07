#/usr/bin/env python2.4
"""
_Destroy_

"""




from WMCore.Agent.Database.DestroyAgentBase import DestroyAgentBase

class Destroy(DestroyAgentBase):
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for
        deletion,
        """
        DestroyAgentBase.__init__(self, logger, dbi)
