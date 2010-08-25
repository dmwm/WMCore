#!/usr/bin/env python
"""
_AlgoDatasetAssoc_

SQLite implementation of DBSBuffer.AlgoDatasetAssoc
"""

__revision__ = "$Id: AlgoDatasetAssoc.py,v 1.1 2009/07/13 19:44:27 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.AlgoDatasetAssoc import AlgoDatasetAssoc as MySQLAlgoDatasetAssoc

class AlgoDatasetAssoc(MySQLAlgoDatasetAssoc):
    """
    _AlgoDatasetAssoc_

    Associate an algorithm to a dataset and return the ID of the association.
    """
    sql = """INSERT INTO dbsbuffer_algo_dataset_assoc (algo_id, dataset_id)
               SELECT (SELECT id FROM dbsbuffer_algo WHERE app_name = :app_name AND
                         app_ver = :app_ver AND app_fam = :app_fam AND
                         pset_hash = :pset_hash) AS algo_id,
                      (SELECT id FROM dbsbuffer_dataset WHERE path = :path) AS dataset_id
               WHERE NOT EXISTS
                 (SELECT * FROM dbsbuffer_algo_dataset_assoc WHERE algo_id =
                   (SELECT id FROM dbsbuffer_algo WHERE app_name = :app_name AND
                      app_ver = :app_ver AND app_fam = :app_fam AND
                      pset_hash = :pset_hash) AND dataset_id =
                   (SELECT id FROM dbsbuffer_dataset WHERE path = :path))"""

    idsql = """SELECT id FROM dbsbuffer_algo_dataset_assoc WHERE algo_id =
                 (SELECT id FROM dbsbuffer_algo WHERE app_name = :app_name AND
                    app_ver = :app_ver AND app_fam = :app_fam AND
                    pset_hash = :pset_hash) AND dataset_id =
                 (SELECT id FROM dbsbuffer_dataset WHERE path = :path)"""
