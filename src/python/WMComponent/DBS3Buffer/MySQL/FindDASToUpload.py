#!/usr/bin/env python

"""
This code should load the necessary information regarding
dataset-algo combinations from the DBSBuffer.

"""




from WMCore.Database.DBFormatter import DBFormatter


class FindDASToUpload(DBFormatter):
    """
    Find Uploadable DAS

    """
    sql = """SELECT dbsbuffer_dataset.path AS datasetpath,
                    dbsbuffer_dataset.acquisition_era AS acquera,
                    dbsbuffer_dataset.processing_ver AS procver
             FROM dbsbuffer_file
             INNER JOIN dbsbuffer_algo_dataset_assoc ON
               dbsbuffer_algo_dataset_assoc.id = dbsbuffer_file.dataset_algo
             INNER JOIN dbsbuffer_dataset ON
               dbsbuffer_dataset.id = dbsbuffer_algo_dataset_assoc.dataset_id
             LEFT OUTER JOIN dbsbuffer_file_parent ON
               dbsbuffer_file_parent.child = dbsbuffer_file.id
             LEFT OUTER JOIN dbsbuffer_file parent_file ON
               parent_file.id = dbsbuffer_file_parent.parent AND
               parent_file.status = 'NOTUPLOADED'
             WHERE dbsbuffer_file.status = 'NOTUPLOADED'
             GROUP BY dbsbuffer_dataset.path,
                      dbsbuffer_dataset.acquisition_era,
                      dbsbuffer_dataset.processing_ver
             HAVING COUNT(parent_file.id) = 0
             """

    def makeDAS(self, results):
        """
        build the data structure we want to return

        """
        datasetalgos = []
        for result in results:
            datasetalgos.append( { 'DatasetPath' : result['datasetpath'],
                                   'AcquisitionEra' : result['acquera'],
                                   'ProcessingVer' : result['procver'] } )

        return datasetalgos

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.makeDAS(self.formatDict(result))
