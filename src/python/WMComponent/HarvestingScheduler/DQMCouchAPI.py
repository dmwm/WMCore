#!/usr/bin/env python
"""
_DatabaseAPI_

Set of API's to update dataset's info in CouchDB
"""

import time
import logging

from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Services.UUID import makeUUID
from WMCore.WMConnectionBase import WMConnectionBase

_LIMIT = 1000

# CouchDB view functions

DatasetsByDatasetID = {"map": \
"""
function(doc) {
    if (doc['type'] == 'dataset') {
        emit(doc['datasetid'], {'_id': doc['_id']});
        }
    }
"""}

BlocksByDatasetID = {"map": \
"""
function(doc) {
    if (doc['type'] == 'block') {
        emit(doc['datasetid'], {'_id': doc['_id']})
        }
    }
"""}

OpenDatasetsByDatasetID = {"map": \
"""
function(doc) {
    if (doc['type'] == 'dataset' & doc['status'] == 'open' ) {
        emit(doc['datasetid'], {'_id': doc['_id']});
        }
    }
"""}

class DQMCouchAPI(WMObject, WMConnectionBase):
    """
    Update the harvesting status of a dataset in CouchDB
    """
    def __init__(self, config, couchDbName = None, couchurl = None):
        WMObject.__init__(self, config)
        WMConnectionBase.__init__(self, "WMCore.WMBS")
        self.designDoc = "HarvestingDatasets"

        if couchDbName == None:
            self.dbname = getattr(self.config.HarvestingScheduler, "couchDBName",
                                  "dqm_default")
        else:
            self.dbname = couchDbName

        if couchurl is not None:
            self.couchurl = couchurl
        elif getattr(self.config.HarvestingScheduler, "couchurl",
                                                            None) is not None:
            self.couchurl = self.config.HarvestingScheduler.couchurl
        else:
            self.couchurl = self.config.JobStateMachine.couchurl

        try:
            self.couchdb = CouchServer(self.couchurl)
            if self.dbname not in self.couchdb.listDatabases():
                self.createDatabase()

            self.database = self.couchdb.connectDatabase(self.dbname,
                                                         size=_LIMIT)
        except Exception, ex:
            logging.error("Error connecting to couch: %s" % str(ex))
            self.database = None

        return 

    def insertDatasets(self, datasets):
        """
        _insertDatasets_

        Inserts Dataset information in couchDB, the input argument should
        be a dictionary containing the following keys:
            datasetName
            scenario
            referenceFile (optional)
            status (optional) ['open', 'closed']
            globalTag (optional)
            release (optional)

        """
        if not datasets:
            return []

        if type(datasets) == type({}):
            datasets = [datasets]

        insertedDatasets = set()

        errors = []

        for dataset in datasets:
            if dataset["datasetName"] in insertedDatasets:
                # This dataset was already queued?
                continue

            # Too many datasets queued? Commit 
            if len(self.database._queue) >= _LIMIT - 1:
                results = self.database.commit()
                # Keep track of possible duplications
                for result in results:
                    if 'error' in result.keys():
                        errors.append(result)

            datasetDocument = {}
            datasetDocument["_id"] = dataset["datasetName"]
            datasetDocument["scenario"] = dataset["scenario"]
            datasetDocument["ref_file"] = dataset.get("referenceFile", None)
            datasetDocument["datasetid"] = datasetDocument["_id"]
            datasetDocument["creation_date"] = int(time.time())
            datasetDocument["type"] = 'dataset'
            datasetDocument["last_check"] = datasetDocument["creation_date"]
            datasetDocument["last_update"] = datasetDocument["creation_date"]
            datasetDocument["global_tag"] = dataset.get("globalTag", None)
            datasetDocument["release"] = dataset.get("release", None)
            status = dataset.get('status', 'open')
            if status in ['open', 'closed']:
                datasetDocument['status'] = status
            else:
                raise Exception, "Wrong dataset status: %s" % status

            self.database.queue(datasetDocument)
            insertedDatasets.add(dataset["datasetName"])

        # Final commit
        results = self.database.commit()
        # Keep track of possible duplications
        for result in results:
            if 'error' in result.keys():
                errors.append(result)

        return errors

    def deleteDatasets(self, datasets, limit=400):
        """
        _deleteDatasets_

        Remove a dataset entry from the couch db. The input argument should be
        the dataset name.

        'limit' sets the number of datasets to be fetched at once from the
        database

        Returns a list of datasets not known by the database
        """

        tooManyDatasets = '1000'

        if not datasets:
            return []

        if type(datasets) == str:
            datasets = [datasets]
        elif type(datasets) == type(set()):
            datasets = list(datasets)

        docsInDB = {}

        # Too many datasets? Will fetch the whole list of datasets in reduced
        # number of queries to the server
        if len(datasets) > tooManyDatasets:
            # Fetching the whole list of datasets by chunks
            # need the _rev attributes in order to delete these entries.
            # I'm reading in chunks 'cause I don't want to have broken pipes
            # downstream.
            options = {"include_docs": True, "limit": int(limit)}
            datasetResults = self.database.loadView(self.designDoc,
                                        "datasetsByDatasetID", options)

            # Nothing? Get out.
            if len(datasetResults["rows"]) == 0:
                return []

            for row in datasetResults["rows"]:
                docsInDB[row["doc"]["_id"]] = row["doc"]

            total_rows = datasetResults["total_rows"]
            # Still have rows to fetch.
            total_left = total_rows - limit
            i = 1
            while total_left > 0:
                options = {"include_docs": True, "limit": int(limit),
                               "skip": int(limit) * 1}
                datasetResults = self.database.loadView(self.designDoc,
                                            "datasetsByDatasetID", options)
                for row in datasetResults["rows"]:
                    docsInDB[row["doc"]["_id"]] = row["doc"]
                total_left -= limit
                i += 1
        # Not too many datasets? Will use 'keys' to fetch only the list of
        # of provided datasets
        else:
            options = {"include_docs": True}
            keys = datasets
            datasetResults = self.database.loadView(self.designDoc,
                                                    "datasetsByDatasetID",
                                                    options,
                                                    keys)

            for row in datasetResults["rows"]:
                docsInDB[row["doc"]["_id"]] = row["doc"]

        # creating list of blocks to delete
        # I have to do this whole procedure 'cause I need _rev to be in the
        # doc in order to delete it
        errors = []
        deletedDatasets = set()
        for dataset in datasets:

            # Too many docs queued? Commit 
            if len(self.database._queue) >= _LIMIT - 1:
                results = self.database.commit()

            try:
                docsInDB[dataset]["_deleted"] = True
            except KeyError:
                errors.append(dataset)
                continue
            self.database.queue(docsInDB[dataset])
            deletedDatasets.add(dataset)

        results = self.database.commit()

        # Removing associated blocks
        self.deleteAllBlocksFromDataset(deletedDatasets)

        return errors

    def updateDatasets(self, datasets, mode='ready'):
        """
        _updateDatasets_

        mode in ('ready', 'lookup')

        ready mode:
        'datasets' argument is a list of documents. Each document must contain
        the '_rev' field, otherwise the document won't be updated, like this:
            [{'field1': value1, 'field2': value2, '_rev': value3},
             {'field1': value4, 'field2': value5, '_rev': value6}]

        lookup mode:
        'datasets' is a dictionary of datasets like this:
            {datasetName1: {'field1': value1, 'field2': value2}}
        I this mode, the method will query couch db in order to collect the
        '_rev' value for each document.

        See: http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API#Modify_Multiple_Documents_With_a_Single_Request
        """
        if mode not in ('ready', 'lookup'):
            raise Exception, "Invalid mode: %s" % mode

        # Looking for _rev in couch db
        if mode == 'lookup':
            if type(datasets) != type({}):
                msg = "Wrong argument type: %s." \
                      " It shoulb be a dictionary" % type(datasets)
                raise Exception, msg

            # Fetching datasets info.
            if len(datasets) > 1:
                options = {"include_docs": True}
                keys = datasets.keys()
            else:
                options = {"include_docs": True,
                                   "key": datasets.keys()[0]}
                keys = []
            datasetResults = self.database.loadView(self.designDoc,
                                                    "datasetsByDatasetID",
                                                    options, keys)

            # Nothing? return.
            if len(datasetResults["rows"]) == 0:
                return []

            # We found these records.
            docsInDB = {}
            for row in datasetResults["rows"]:
                docsInDB[row["doc"]["_id"]] = row["doc"]

            # Updating and queueing selected datasets for commit
            for dataset in datasets:
                docsInDB[dataset].update(datasets[dataset])

                # Too many docs queued? Commit 
                if len(self.database._queue) >= _LIMIT - 1:
                    results = self.database.commit()

                self.database.queue(docsInDB[dataset])

        # ready mode: datasets already have _rev values, just queue them for
        # commit
        else:
            # Queueing selected datasets for commit
            for dataset in datasets:

                # Too many docs queued? Commit 
                if len(self.database._queue) >= _LIMIT - 1:
                    results = self.database.commit()

                self.database.queue(dataset)

        # Commiting!
        results = self.database.commit()

    def insertBlocks(self, dataset, blocks):
        """
        _insertBlocks_

        Inserts blocks into databaset, each block should have an associated
        dataset.

        """
        # Check that dataset exists
        options = {"include_docs": True, "key": dataset}
        datasetResults = self.database.loadView(self.designDoc,
                                                "datasetsByDatasetID",
                                                options)
        if len(datasetResults["rows"]) > 1:
            raise Exception, "Two datasets with the same name?"
        if len(datasetResults["rows"]) == 0:
            return Exception, "Dataset %s is not known by db" % dataset

        result = []
        errors = []
        insertedBlocks = set()
        for block in blocks:
            if block in insertedBlocks:
                # This block was already queued?
                continue

            # Too many docs queued? Commit 
            if len(self.database._queue) >= _LIMIT - 1:
                results = self.database.commit()
                # Keep track of possible duplications
                for result in results:
                    if 'error' in result.keys():
                        errors.append(result)

            blockDocument = {}
            blockDocument["_id"] = block
            blockDocument["blockid"] = block
            blockDocument["datasetid"] = dataset
            blockDocument["type"] = "block"
            blockDocument["create_date"] = int(time.time())

            self.database.queue(blockDocument)
            insertedBlocks.add(block)

        # Final commit
        results = self.database.commit()
        # Keep track of possible duplications
        for result in results:
            if 'error' in result.keys():
                errors.append(result)

        return errors

    def deleteAllBlocksFromDataset(self, datasets):
        """
        _deleteAllBlocks_

        Given a list of datasets, it deletes all the associated blocks
        """
        if type(datasets) == str:
            datasets = [datasets]
        elif type(datasets) == type(set()):
            datasets = list(datasets)

        if not datasets:
            return

        # Querying blocks from database
        if len(datasets) > 1:
            options = {"include_docs": True}
            keys = datasets
        else:
            options = {"include_docs": True, "key": datasets[0]}
            keys = []

        blockResults = self.database.loadView(self.designDoc,
                                            "blocksByDatasetID", options, keys)

        if len(blockResults["rows"]) == 0:
            return

        docsInDB = {}
        for row in blockResults["rows"]:

            # Too many docs queued? Commit 
            if len(self.database._queue) >= _LIMIT - 1:
                results = self.database.commit()

            block = row["doc"]["_id"]
            docsInDB[block] = row["doc"]
            docsInDB[block]["_deleted"] = True

            self.database.queue(docsInDB[block])

        results = self.database.commit()

    def bulkUpdate(self, docs):
        """
        _bulkUpdate_

        This method does bulk update to the input documents. I will return
        errors from the server.
        """
        for doc in docs:

            # Too many docs queued? Commit 
            if len(self.database._queue) >= _LIMIT - 1:
                results = self.database.commit()

            self.database.queue(doc)

        # Commiting!
        results = self.database.commit()
        return results

    def listBlocks(self, dataset):
        """
        _queryProcessedBlocks_

        Given a dataset path, this method will return all the blocks stored
        in the database (processed blocks).
        """
        options = {"include_docs": True, "key": dataset}
        blockResults = self.database.loadView(self.designDoc,
                                              "blocksByDatasetID", options)

        if len(blockResults["rows"]) == 0:
            return []

        blocks = []
        for row in blockResults["rows"]:
            blocks.append(row["doc"])

        return blocks

    def listOpenDatasets(self):
        """
        _listOpenDatasets_

        Lists all open datasets in database
        """
        options = {"include_docs": True}
        datasetResults = self.database.loadView(self.designDoc,
                                                "openDatasetsByDatasetID",
                                                options)

        if len(datasetResults["rows"]) == 0:
            return []

        datasets = []
        for row in datasetResults["rows"]:
            datasets.append(row["doc"])

        return datasets

    def createDatabase(self):
        """
        _createDatabase_

        Create the couch database and install the views.
        """
        database = self.couchdb.createDatabase(self.dbname)

        hashViewDoc = database.createDesignDoc(self.designDoc)
        #TODO: This has to be moved somwhere else --> couchapps
        viewDict = {"datasetsByDatasetID": DatasetsByDatasetID,
                    "blocksByDatasetID": BlocksByDatasetID,
                    "openDatasetsByDatasetID": OpenDatasetsByDatasetID}
        hashViewDoc["views"] = viewDict

        database.queue(hashViewDoc)
        database.commit()
        return database

