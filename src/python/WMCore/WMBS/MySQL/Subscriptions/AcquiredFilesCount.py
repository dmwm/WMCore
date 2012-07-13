"""
_AcquiredFilesCount_

MySQL implementation of Subscriptions.AcquiredFilesCount
"""

from WMCore.Database.DBFormatter import DBFormatter

class AcquiredFilesCount(DBFormatter):
    """
    _AcquiredFilesCount_

    Not much to say, title says it all

    """
    sql = """SELECT COUNT(*)
             FROM wmbs_sub_files_acquired
             WHERE subscription = :SUBSCRIPTION"""

    def execute(self, subscription, conn = None, transaction = False):

        binds = { 'SUBSCRIPTION' : subscription }

        fileCount = self.dbi.processData(self.sql, binds, conn = conn,
                                         transaction = transaction)[0].fetchall()[0][0]
        return fileCount
