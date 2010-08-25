#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.2 2009/07/21 14:32:57 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

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
        self.delete["30wmbs_subs_type"] = "DROP TABLE wmbs_subs_type"
        
        j=50
        for i in Create.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
                           "DROP SEQUENCE %s"  % seqname 
