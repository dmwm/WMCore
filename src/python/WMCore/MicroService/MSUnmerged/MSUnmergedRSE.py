"""
File       : MSUnmergedRSE.py
Description: Provides a document Template for the MSUnmerged MicroServices
"""

from pymongo.errors  import NotPrimaryError
# from pymongo.results import results as MongoResults


class MSUnmergedRSE(dict):
    """
    A minimal RSE information representation to serve the needs
    of the MSUnmerged Micro Service.
    """
    def __init__(self, rseName, **kwargs):
        super(MSUnmergedRSE, self).__init__(**kwargs)

        self.rseName = rseName
        self.update(self.defaultDoc())
        self.mongoFilter = {'name': self['name']}

    def defaultDoc(self):
        """
        Returns the data schema for a record in MongoDB.
        :return: A simple dictionary populated with default values
        """
        # NOTE: totalNumFiles reflects the total number of files at the RSE as
        #       fetched from the Rucio Consistency Monitor. Once the relevant
        #       protected paths have been filtered out and the path been cut to the
        #       proper depth (as provided by the WMStats Protected LFNs interface),
        #       then the final number (but on a directory level rather than on
        #       files granularity level) will be put in the counter 'dirsToDelete'

        # NOTE: The type of msUnmergedRSE['files']['toDelete'] is a dictionary of
        #       of generators holding the filters for the files to be deleted e.g.:
        #       msUnmergedRSE['files']['toDelete'] = {
        #          '/store/unmerged/Run2018B/TOTEM42/MINIAOD/22Feb2019-v1': <filter at 0x7f3699d93208>,
        #          '/store/unmerged/Run2018B/TOTEM21/AOD/22Feb2019-v1': <filter at 0x7f3699d93128>,
        #          '/store/unmerged/Run2018D/MuonEG/RAW-RECO/TopMuEG-12Nov2019_UL2018-v1': <filter at 0x7f3699d93668>}
        defaultDoc = {
            "name": self.rseName,
            "pfnPrefix": None,
            "isClean": False,
            "rucioConMonStatus": None,
            "timestamps": {'rseConsStatTime': 0.0,
                           'prevStartTime': 0.0,
                           'startTime': 0.0,
                           'prevEndTime': 0.0,
                           'endTime': 0.0},
            "counters": {"totalNumFiles": 0,
                         "totalNumDirs": 0,
                         "dirsToDelete": 0,
                         "filesToDelete": 0,
                         "filesDeletedSuccess": 0,
                         "filesDeletedFail": 0,
                         "dirsDeletedSuccess": 0,
                         "dirsDeletedFail": 0,
                         "gfalErrors": {}},
            "files": {"allUnmerged": [],
                      "toDelete": {},
                      "protected": {},
                      "deletedSuccess": set(),
                      "deletedFail": set()},
            "dirs": {"allUnmerged": set(),
                     "toDelete": set(),
                     "protected": set(),
                     "deletedSuccess": set(),
                     "deletedFail": set()}
        }
        return defaultDoc

    def buildMongoProjection(self, fullRSEToDB=False):
        """
        Returns the correct mongoProjection based on the `fullRSEToDB` flag passed
        :param fullRSEToDB: Flag to decide whether the whole RSE object is to be
                        included in the projection ergo saved into the database.
        :return:        A dictionary type MongoDB Projection
        """
        mongoProjection = {
            "_id": False,
            "name": True,
            "pfnPrefix": True,
            "rucioConMonStatus": True,
            "isClean" : True,
            "timestamps": True,
            "counters": True,
            "dirs": True}
        if fullRSEToDB:
            mongoProjection.update({"files": True})
        return mongoProjection

    def readRSEFromMongoDB(self, collection, useProjection=False):
        """
        A method to read the RSE object from Database and update it's fields.
        :param collection: The MongoDB collection to read from
        :param useProjection: Use the projection returned by self.buildProjection()
                              while reading from MongoDB
        :return:           True if read and update were both successful, False otherwise.
        """
        if useProjection:
            mongoProjection = self.buildMongoProjection()
            mongoRecord = collection.find_one(self.mongoFilter, projection=mongoProjection)
        else:
            mongoRecord = collection.find_one(self.mongoFilter)

        # update the list fields read from MongoDB back to strictly pythonic `set`s
        if mongoRecord:
            for field in mongoRecord:
                if field == 'dirs' or field == 'files':
                    for fieldKey, fieldList in mongoRecord[field].items():
                        if isinstance(self[field][fieldKey], set):
                            mongoRecord[field][fieldKey] = set(fieldList)
            self.update(mongoRecord)
            return True
        else:
            return False

    def writeRSEToMongoDB(self, collection, fullRSEToDB=False, overwrite=False, retryCount=0):
        """
        A method to write/update the RSE at the Database from the current object.
        :param collection:     The MonogoDB collection to write on
        :param fullRSEToDB:    Bool flag, used to trigger dump of the whole RSE object to
                               the database with the `files` section (excluding the generator objects)
                               NOTE: if fullRSEToDB=False and a previous record for the RSE already exists
                                     the fields missing from the projection won't be updated
                                     during this write operation but will preserver their values.
                                     To completely refresh and RSE record in the database use
                                     self.purgeRSEAtMongoDB first.
        :param overwrite:      A flag to note if the currently existing document into
                               the database is about to be completely replaced or just
                               fields update is to happen.
        :param retryCount:     The number of retries for the write operation if failed due to
                               `NotPrimaryError. Possible values:
                               0   - the write operation will be tried exactly once and no retries will happen
                               > 0 - the write operation will be retried this number of times
                               < 0 - the write operation will never be tried
        :return:               True if update was successful, False otherwise.
        """
        # NOTE: The fields to be manipulated are only those which are compatible
        #       with MongoDB (i.e. here we avoid any field holding a strictly
        #       pythonic objects, like generators etc.)

        updateFields = {}
        mongoProjection = self.buildMongoProjection(fullRSEToDB)
        for field in mongoProjection:
            if mongoProjection[field]:
                # convert back the strictly pythonic `set`s && `generators` to Json compatible lists
                if field == 'dirs':
                    updateFields[field] = {}
                    for dirKey, dirSet in self[field].items():
                        if isinstance(dirSet, set):
                            updateFields[field][dirKey] = list(dirSet)
                elif field == 'files':
                    updateFields[field] = {}
                    for fileKey, fileSet in self[field].items():
                        if isinstance(fileSet, set):
                            updateFields[field][fileKey] = list(fileSet)
                        elif isinstance(fileSet, dict):
                            # Iterating through the filterNames here, and recording only empty lists for filter values
                            # NOTE: We can either execute the filter and write every single file in the database
                            #       or if we need it we may simply use the filterName to recreate it.
                            updateFields[field][fileKey] = dict([(filterName, []) for filterName in list(self[field][fileKey])])
                        else:
                            updateFields[field][fileKey] = self[field][fileKey]
                else:
                    updateFields[field] = self[field]

        updateOps = {'$set': updateFields}
        # NOTE: NotPrimaryError is a recoverable error, caused by a session to a
        #       non primary backend part of a replicaset.
        while retryCount >= 0:
            try:
                if overwrite:
                    result = collection.replace_one(self.mongoFilter, updateFields, upsert=True)
                else:
                    result = collection.update_one(self.mongoFilter, updateOps, upsert=True)
                break
            except NotPrimaryError:
                if retryCount:
                    # msg = "Failed write operation to MongoDB. %s retries left."
                    # self.logger.warning(msg, retryCount)
                    retryCount -= 1
                else:
                    # msg = "Failed write operation to MongoDB. All retries exhausted."
                    # self.logger.warning(msg, retryCount)
                    raise

        # NOTE: If and `upsert` took place the modified_count for both operations is 0
        #       because no modification took place  but rather an insertion.
        if result.modified_count or result.upserted_id:
            return True
        else:
            return False

    def resetRSE(self, collection, keepTimestamps=False, keepCounters=False, retryCount=0):
        """
        Resets all records of the RSE object to default values  and write the
        document to MongoDB
        :param keepTimestamps: Bool flag to keep the timestamps
        :param keepCounters:   Bool flag to keep the counters
        :param retryCount:     The number of retries for the write operation if failed due to `NotPrimaryError.
        :return:               True if operation succeeded.
        """
        resetDoc = self.defaultDoc()

        if keepTimestamps:
            resetDoc['timestamps'] = self['timestamps']
        if keepCounters:
            resetDoc['counters'] = self['counters']

        self.update(resetDoc)
        writeResult = self.writeRSEToMongoDB(collection, fullRSEToDB=True, overwrite=True, retryCount=retryCount)
        return writeResult
