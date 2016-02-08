#!/usr/bin/env python
"""
_AlgoDatasetAssoc_

Associate an algorithm with a dataset in DBSBuffer.
"""

from WMComponent.DBS3Buffer.MySQL.AlgoDatasetAssoc import AlgoDatasetAssoc as MySQLAlgoDatasetAssoc

class AlgoDatasetAssoc(MySQLAlgoDatasetAssoc):

    sql = """INSERT INTO dbsbuffer_algo_dataset_assoc
             (id, algo_id, dataset_id)
             SELECT dbsbuffer_algdset_assoc_seq.nextval,
                    (SELECT id FROM dbsbuffer_algo
                     WHERE app_name = :app_name AND app_ver = :app_ver
                     AND app_fam = :app_fam AND pset_hash = :pset_hash),
                    (SELECT id FROM dbsbuffer_dataset WHERE path = :path)
             FROM DUAL
             WHERE NOT EXISTS
               ( SELECT *
                 FROM dbsbuffer_algo_dataset_assoc
                 WHERE algo_id =
                   (SELECT id FROM dbsbuffer_algo
                    WHERE app_name = :app_name AND app_ver = :app_ver
                    AND app_fam = :app_fam AND pset_hash = :pset_hash)
                 AND dataset_id =
                   (SELECT id FROM dbsbuffer_dataset WHERE path = :path) )
             """
