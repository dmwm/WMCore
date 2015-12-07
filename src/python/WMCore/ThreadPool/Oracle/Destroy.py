#/usr/bin/env python2.4
"""
_Destroy_

"""




from WMCore.ThreadPool.MySQL.Destroy import Destroy as MySQLDestroy
from WMCore.ThreadPool.Oracle.Create import Create

class Destroy(MySQLDestroy):
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and add all necessary tables for
        deletion,
        """
        MySQLDestroy.__init__(self, logger, dbi)

        j=50
        for i in Create.sequence_tables:
            seqname = i
            self.delete["%s%s" % (j, seqname)] = \
                           "DROP SEQUENCE %s"  % seqname


        #for i in Create.trigger_tables:
        #    seqname = i
        #    self.create["%s%s" % (j, seqname)] = \
        #                   "DROP TRIGGER %s"  % seqname
