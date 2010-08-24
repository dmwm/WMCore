#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.2 2008/11/20 21:52:34 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.DestroyWMBSBase import DestroyWMBSBase

class Destroy(DestroyWMBSBase):    
    def __init__(self):
        """
        _init_

        Call the base class's constructor and add all necessary tables for 
        deletion,
        """        
        DestroyWMBSBase.__init__(self)
        