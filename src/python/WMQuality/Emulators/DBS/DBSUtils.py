#!/usr/bin/env python

"""
Version of DBSUtils module to be used with mock or unittest.mock
"""


def MockDBSErrors(dbsUrl):
    """
    Fetch and return all DBS server errors
    :param dbsUrl: dbs url, we do not use it here in mock function
    :return: dictionary of DBS server errors and their meaning
    """
    dbsErrors = {
            100: 'generic DBS error',
            101: 'database error',
            300: 'insert error for dataset'
            }
    return dbsErrors
