"""
File       : MSUnmergedRSE.py
Description: Provides a document Template for the MSUnmerged MicroServices
"""

from pymongo import ReturnDocument
# from pymongo.results import results as MongoResults


class MSUnmergedRSE(dict):
    """
    A minimal RSE information representation to serve the needs
    of the MSUnmerged Micro Service.
    """
    def __init__(self, rseName, **kwargs):
        super(MSUnmergedRSE, self).__init__(**kwargs)

        # NOTE: totalNumFiles reflects the total number of files at the RSE as
        #       fetched from the Rucio Consistency Monitor. Once the relevant
        #       protected paths have been filtered out and the path been cut to the
        #       proper depth (as provided by the WMStats Protected LFNs interface),
        #       then the final number (but on a directory level rather than on
        #       files granularity level) will be put in the counter 'toDelete'

        # NOTE: The type of msUnmergedRSE['files']['toDelete'] is a dictionary of
        #       of generators holding the filters for the files to be deleted e.g.:
        #       msUnmergedRSE['files']['toDelete'] = {
        #          '/store/unmerged/Run2018B/TOTEM42/MINIAOD/22Feb2019-v1': <filter at 0x7f3699d93208>,
        #          '/store/unmerged/Run2018B/TOTEM21/AOD/22Feb2019-v1': <filter at 0x7f3699d93128>,
        #          '/store/unmerged/Run2018D/MuonEG/RAW-RECO/TopMuEG-12Nov2019_UL2018-v1': <filter at 0x7f3699d93668>}
        myDoc = {
            "name": rseName,
            "pfnPrefix": None,
            "isClean": False,
            "timestamps": {'prevStartTime': 0.0,
                           'startTime': 0.0,
                           'prevEndtime': 0.0,
                           'endTime': 0.0},
            "counters": {"totalNumFiles": 0,
                         "dirsToDeleteAll": 0,
                         "dirsToDelete": 0,
                         "filesToDelete": 0,
                         "deletedSuccess": 0,
                         "deletedFail": 0},
            "files": {"allUnmerged": [],
                      "toDelete": {},
                      "protected": {},
                      "deletedSuccess": [],
                      "deletedFail": []},
            "dirs": {"allUnmerged": set(),
                     "toDelete": set(),
                     "protected": set()}
        }
        self.update(myDoc)
        self.mongoFilter = {'name': self['name']}

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
            "isClean" : True,
            "timestamps": True,
            "counters": True,
            "files": False,
            "dirs": True}
        if fullRSEToDB:
            mongoProjection.update({"files": True})
        return mongoProjection

    def readRSEFromMongoDB(self, collection):
        """
        A method to read the RSE object from Database and update it's fields.
        :param collection: The MongoDB collection to read from
        :return:           True if read and update were both successful, False otherwise.
        """
        mongoRecord = collection.find_one(self.mongoFilter)

        # update the list fields read from MongoDB back to strictly pythonic `set`s
        if mongoRecord:
            for dirKey, dirList in mongoRecord['dirs'].items():
                mongoRecord['dirs'][dirKey] = set(dirList)
            self.update(mongoRecord)
            return True
        else:
            return False

    def writeRSEToMongoDB(self, collection, fullRSEToDB=False, overwrite=False):
        """
        A method to write/update the RSE at the Database from the current object.
        :param collection: The MonogoDB collection to write on
        :param fullRSEToDB:    Bool flag, used to trigger dump of the whole RSE object to
                           the database with the `files` section (excluding the generator objects)
                           NOTE: if fullRSEToDB=False and a previous record for the RSE already exists
                                 the fields missing from the projection won't be updated
                                 during this write operation but will preserver their values.
                                 To completely refresh and RSE record in the database use
                                 self.purgeRSEAtMongoDB first.
        :param overwrite:  A flag to note if the currently existing document into
                           the database is about to be completely replaced or just
                           fields update is to happen.
        :return:           True if update was successful, False otherwise.
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
                        updateFields[field][dirKey] = list(dirSet)
                elif field == 'files':
                    updateFields[field] = {}
                    for fileKey, fileSet in self[field].items():
                        if isinstance(self[field][fileKey], dict):
                            updateFields[field][fileKey] = list(self[field][fileKey])
                        else:
                            updateFields[field][fileKey] = self[field][fileKey]
                else:
                    updateFields[field] = self[field]

        updateOps = {'$set': updateFields}
        if overwrite:
            result = collection.replace_one(self.mongoFilter,
                                            updateFields,
                                            upsert=True)
        else:
            result = collection.update_one(self.mongoFilter,
                                           updateOps,
                                           upsert=True)
        # NOTE: If and `upsert` took place the modified_count for both operations is 0
        #       because no modification took place  but rather an insertion.
        if result.modified_count or result.upserted_id:
            return True
        else:
            return False
