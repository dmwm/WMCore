import datetime
import time
class SQLiteBase(object):
    def timestamp(self):
        """
        generate a timestamp
        """
        t = datetime.datetime.now()
        return time.mktime(t.timetuple())
    
    def format(self, result):
        """
        Some standard formatting making allowances for the difference with MySQL
        """
        return result