from WMCore.Database.DBFormatter import DBFormatter

class DefaultFormatter(DBFormatter):
    """
    change default format as dict format for monitoring
    """

    def format(self, result):
        return self.formatDict(result)
