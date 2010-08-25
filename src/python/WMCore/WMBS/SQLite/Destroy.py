#/usr/bin/env python2.4
"""
_Destroy_

"""




from WMCore.WMBS.DestroyWMBSBase import DestroyWMBSBase

class Destroy(DestroyWMBSBase):    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for 
        deletion,
        """        
        DestroyWMBSBase.__init__(self, logger, dbi)
