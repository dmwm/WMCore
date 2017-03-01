"""
"""
from __future__ import print_function

import logging
import os
import sys
import threading

from WMCore.Database.CMSCouch import Database
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMInit import connectToDB

def fixDBSmissingFileAssoc():
    os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)
    problemFilesSql = """
        select dbsbuffer_file.id as fileid, dbsbuffer_location.id as seid from wmbs_file_location fl
            inner join wmbs_file_details fd on fd.id = fl.fileid
            inner join wmbs_location_pnns wls on wls.location = fl.location
            inner join wmbs_location wl on wl.id = fl.location
            inner join dbsbuffer_location on dbsbuffer_location.pnn = wls.pnn
            inner join dbsbuffer_file on dbsbuffer_file.lfn = fd.lfn
            where fd.lfn in (select df.lfn from dbsbuffer_file df
                               left outer join dbsbuffer_file_location dfl on df.id = dfl.filename
                               where dfl.location is null)
                      """
    unfinishedTasks = formatter.formatDict(formatter.dbi.processData(problemFilesSql))
    print("%s lenth" % len(unfinishedTasks))
    result = {}
    for row in unfinishedTasks:
        result.setdefault(row["fileid"], row)
        print(row)
    print("trimed %s lenth" % len(result))
    insertSQL = """INSERT INTO dbsbuffer_file_location (filename, location)
               VALUES (:fileid, :seid)"""
    done = formatter.dbi.processData(insertSQL, result.values())
    print("inserted %s" % done)

if __name__ == '__main__':
    fixDBSmissingFileAssoc()

