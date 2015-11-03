#!/usr/bin/env python
"""
_NewDataset_

Oracle implementation of DBS3Buffer.NewDataset
"""




from WMComponent.DBS3Buffer.MySQL.NewDataset import NewDataset as MySQLNewDataset

class NewDataset(MySQLNewDataset):

    sql = """INSERT INTO dbsbuffer_dataset
             (id, path, processing_ver, acquisition_era, valid_status, global_tag, parent, prep_id)
             SELECT dbsbuffer_dataset_seq.nextval, :path, :processing_ver, :acquisition_era,
                    :valid_status, :global_tag, :parent, :prep_id
             FROM DUAL
             WHERE NOT EXISTS
               ( SELECT *
                 FROM dbsbuffer_dataset
                 WHERE path = :path )
             """
