#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.2 2009/08/18 23:18:13 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WorkQueue.Database.DestroyWorkQueueBase import DestroyWorkQueueBase
#from WMCore.WorkQueue.Database.Oracle.Create import Create

class Destroy(DestroyWorkQueueBase):    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for 
        deletion,
        """      
        DestroyWorkQueueBase.__init__(self, logger, dbi)
        
