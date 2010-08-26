#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2010/06/21 21:18:47 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.Database.DestroyAgentBase import DestroyAgentBase

class Destroy(DestroyAgentBase):    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for 
        deletion,
        """      
        DestroyAgentBase.__init__(self, logger, dbi)
