#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.2 2008/11/24 19:47:06 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.DestroyWMBSBase import DestroyWMBSBase

class Destroy(DestroyWMBSBase):    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for 
        deletion,
        """        
        DestroyWMBSBase.__init__(self, logger, dbi)
        self.create["30wmbs_subs_type"] = "DROP TABLE wmbs_subs_type"   
