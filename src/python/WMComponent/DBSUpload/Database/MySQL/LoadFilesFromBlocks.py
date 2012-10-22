#!/usr/bin/env python

"""
This should load the files from active blocks with
the block info
"""

from WMComponent.DBSUpload.Database.MySQL.LoadDBSFilesByDAS import LoadDBSFilesByDAS

class LoadFilesFromBlocks(LoadDBSFilesByDAS):
    """
    _LoadFilesFromBlocks_

    Load the files from blocks that are Open or Pending
    that are ready for upload (status == READY)
    """



    fileInfoSQL = """SELECT files.id AS id, files.lfn AS lfn, files.filesize AS filesize,
                       files.events AS events,
                       files.status AS status,
                       files.block_id AS block_id,
                       dbsbuffer_algo.app_name AS app_name, dbsbuffer_algo.app_ver AS app_ver,
                       dbsbuffer_algo.app_fam AS app_fam, dbsbuffer_algo.pset_hash AS pset_hash,
                       dbsbuffer_algo.config_content, dbsbuffer_dataset.path AS dataset_path
                     FROM dbsbuffer_file files
                     INNER JOIN dbsbuffer_algo_dataset_assoc ON
                       files.dataset_algo = dbsbuffer_algo_dataset_assoc.id
                     INNER JOIN dbsbuffer_algo ON
                       dbsbuffer_algo_dataset_assoc.algo_id = dbsbuffer_algo.id
                     INNER JOIN dbsbuffer_dataset ON
                       dbsbuffer_algo_dataset_assoc.dataset_id = dbsbuffer_dataset.id
                     INNER JOIN dbsbuffer_block dbb ON dbb.id = files.block_id
                     WHERE (dbb.status = 'Open' OR dbb.status = 'Pending')
                     AND files.status = 'READY'
                     AND dbb.id = :block_id
                     ORDER BY files.id"""



    def execute(self, blockID, conn = None, transaction = False):
        """
        Execute multiple SQL queries to extract all binding information
        Use the first query to get the fileIDs

        """
        result   = self.dbi.processData(self.fileInfoSQL, {'block_id': blockID},
                                        conn = conn,
                                        transaction = transaction)
        fileInfo = self.formatFileInfo(result)

        fileIDs  = [x['id'] for x in fileInfo]
        binds    = self.getBinds(fileIDs)

        if len(fileInfo) == 0:
            # Then we have no files for this DAS
            return []


        # Do locations
        result   = self.dbi.processData(self.getLocationSQL, binds,
                                        conn = conn,
                                        transaction = transaction)
        locInfo  = self.locInfo(result)
        fullResults = self.merge(fileInfo, locInfo)


        # Do checksums
        result   = self.dbi.processData(self.getChecksumSQL, binds,
                                        conn = conn,
                                        transaction = transaction)

        ckInfo      = self.ckInfo(result)
        fullResults = self.merge(fullResults, ckInfo)


        # Do runLumi
        result      = self.dbi.processData(self.getRunLumiSQL, binds,
                                           conn = conn,
                                           transaction = transaction)
        runInfo  = self.runInfo(result)
        fullResults = self.merge(fullResults, runInfo)



        # Do parents
        result   = self.dbi.processData(self.getParentLFNSQL, binds,
                                        conn = conn,
                                        transaction = transaction)
        parInfo  = self.parentInfo(result)
        fullResults = self.merge(fullResults, parInfo)



        return fullResults
