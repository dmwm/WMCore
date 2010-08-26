#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.3 2009/10/12 21:11:14 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.DestroyWMBSBase import DestroyWMBSBase
from WMCore.WMBS.Oracle.Create import Create

class Destroy(DestroyWMBSBase):    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for 
        deletion,
        """        
        DestroyWMBSBase.__init__(self, logger, dbi)
        
        j = 50
        for i in Create.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
                           "DROP SEQUENCE %s"  % seqname 
