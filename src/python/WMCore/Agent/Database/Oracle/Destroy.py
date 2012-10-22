#/usr/bin/env python2.4
"""
_Destroy_

"""




from WMCore.Agent.Database.DestroyAgentBase import DestroyAgentBase
from WMCore.Agent.Database.Oracle.Create import Create

class Destroy(DestroyAgentBase):
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for
        deletion,
        """
        DestroyAgentBase.__init__(self, logger, dbi)

        for tableName in Create.sequenceTables:
            seqname = '%s_SEQ' % tableName
            self.create["%s%s" % (Create.seqStartNum, seqname)] = \
                           "DROP SEQUENCE %s"  % seqname
            # triggers have to be deleted first
            triggerName = '%s_TRG' % tableName
            self.create["%s%s" % ('00', triggerName)] = \
                           "DROP TRIGGER %s"  % triggerName
