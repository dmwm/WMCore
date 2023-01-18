from __future__ import division, print_function

from builtins import str
from functools import wraps
import logging
import threading

### Don't crash the components on the following database exceptions
# ORA-03113: end-of-file on communication channel
# ORA-03114: Not Connected to Oracle
# ORA-03135: connection lost contact
# ORA-12545: Connect failed because target host or object does not exist
# ORA-00060: deadlock detected while waiting for resource
# ORA-01033: ORACLE initialization or shutdown in progress
# (cx_Oracle.InterfaceError) not connected  # same as ORA-03114, in the new SQLAlchemy
# (cx_Oracle.DatabaseError) ORA-25408: can not safely replay call
# (cx_Oracle.DatabaseError) ORA-25401: can not continue fetches
# and those two MySQL exceptions
DB_CONNECTION_ERROR_STR = ["ORA-03113", "ORA-03114", "ORA-03135", "ORA-12545", "ORA-00060", "ORA-01033",
                           "MySQL server has gone away", "Lock wait timeout exceeded",
                           "(cx_Oracle.InterfaceError) not connected", "ORA-25408", "ORA-25401"]


def db_exception_handler(f):
    """
    :param f: function
    :return: wrapper fuction

    Only need to handle DB connection problem other db problems which need to rollback the transaction,
    shouldn't be included in this hanldler

    Warning: This only used, when original function return values are not used, or wrapped around Utils.Timer.timeFunc
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        myThread = threading.currentThread()
        if hasattr(myThread, "logdbClient") and myThread.logdbClient is not None:
            myThread.logdbClient.delete("DBConnection_error_handler", "warning", this_thread=True)
        try:
            return f(*args, **kwargs)
        except Exception as ex:
            msg = str(ex)
            for errStr in DB_CONNECTION_ERROR_STR:
                if errStr in msg:
                    logging.error("%s: Temp error will try later", msg)

                    if hasattr(myThread, "logdbClient") and myThread.logdbClient is not None:
                        myThread.logdbClient.post("DBConnection_error_handler", "warning")

                    # returns tuples of 3 since timeFunc returns (time to spend, result, func.__name__)
                    return 0, None, f.__name__

            # for other case raise the same exception
            raise

    return wrapper
