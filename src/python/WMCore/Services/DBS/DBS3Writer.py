#!/usr/bin/env python
"""
_DBSReader_

Read/Write DBS Interface

"""
from __future__ import print_function, division
from builtins import str
import logging

from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException

from WMCore.Services.DBS.DBSErrors import DBSWriterError, formatEx3
from WMCore.Services.DBS.DBS3Reader import DBS3Reader


class DBS3Writer(DBS3Reader):
    """
    _DBSReader_

    General API for writing data to DBS
    """

    def __init__(self, url, logger=None, **contact):

        # instantiate dbs api object
        try:
            self.dbsWriteUrl = url
            self.dbs = DbsApi(url, **contact)
            self.logger = logger or logging.getLogger(self.__class__.__name__)
        except dbsClientException as ex:
            msg = "Error in DBSWriter with DbsApi\n"
            msg += "%s\n" % formatEx3(ex)
            raise DBSWriterError(msg)

    def setDBSStatus(self, dataset, status):
        """
        The function to set the DBS status of an output dataset
        :param dataset: Dataset name
        :return: True if operation is successful, False otherwise
        """

        dbsApi = DbsApi(url=self.dbsWriteUrl)

        try:
            dbsApi.updateDatasetType(dataset=dataset,
                                     dataset_access_type=status)
        except Exception as ex:
            msg = "Exception while setting the status of following dataset on DBS: {} ".format(dataset)
            msg += "Error: {}".format(str(ex))
            self.logger.exception(msg)

        dbsStatus = self.getDBSStatus(dataset)

        if dbsStatus == status:
            return True
        else:
            return False
