#/usr/bin/env python2.4
"""
_Destroy_

"""




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
