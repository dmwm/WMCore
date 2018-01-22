from __future__ import division, print_function

from functools import wraps
import logging

DB_CONNECTION_ERROR_STR = ["ORA-03113", "ORA-03114", "ORA-03135",
                           "MySQL server has gone away"]

def db_exception_handler(f):
    """
    :param f: function
    :return: wrapper fuction

    Only need to handle DB connection problem other db problems which need to rollback the transaction,
    shouldn't be included in this hanldler

    TODO: instead of logging we can add logDB report
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as ex:
            raiseFlag = True
            msg = str(ex)
            for errStr in DB_CONNECTION_ERROR_STR:
                if errStr in msg:
                    logging.exception("%s: Temp error will try later", msg)
                    raiseFlag = False
                    break

            if raiseFlag:
                raise

    return wrapper
