#!/usr/bin/env python
"""
_NewAlgo_

SQLite implementation of DBSBuffer.NewAlgo
"""

__revision__ = "$Id: NewAlgo.py,v 1.2 2009/07/13 19:39:01 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.NewAlgo import NewAlgo as MySQLNewAlgo

class NewAlgo(MySQLNewAlgo):
    """
    _NewAlgo_

    Add a new algorithm to the DBSBuffer.  This will do nothing if an algorithm
    with the given parameters already exists.
    """
    sql = """INSERT INTO dbsbuffer_algo (app_name, app_ver, app_fam, pset_hash,
                                         config_content, in_dbs)
               SELECT :app_name, :app_ver, :app_fam, :pset_hash,
                 :config_content, 0 WHERE NOT EXISTS   
                   (SELECT * FROM dbsbuffer_algo WHERE app_name = :app_name AND
                      app_ver = :app_ver AND app_fam = :app_fam AND
                      pset_hash = :pset_hash)"""    
