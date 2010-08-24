#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2008/11/20 21:54:27 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.DestroyWMBSBase import DestroyWMBSBase

class Destroy(DestroyWMBSBase):    
    def __init__(self):
        """
        _init_

        Call the base class's constructor and add all necessary tables for 
        deletion,
        """        
        DestroyWMBSBase.__init__(self)
        self.create["30wmbs_subs_type"] = "DROP TABLE wmbs_subs_type"   