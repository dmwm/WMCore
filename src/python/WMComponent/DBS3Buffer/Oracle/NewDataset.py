#!/usr/bin/env python
"""
_NewDataset_

Oracle implementation of DBS3Buffer.NewDataset
"""




from WMComponent.DBS3Buffer.MySQL.NewDataset import NewDataset as MySQLNewDataset

class NewDataset(MySQLNewDataset):

    sql = """INSERT INTO dbsbuffer_dataset (path, processing_ver, acquisition_era, valid_status, global_tag, parent, custodial_site)
               SELECT :path, :processing_ver, :acquisition_era, :valid_status, :global_tag, :parent, :custodial_site FROM DUAL
               WHERE NOT EXISTS (SELECT path FROM dbsbuffer_dataset WHERE path = :path)"""
