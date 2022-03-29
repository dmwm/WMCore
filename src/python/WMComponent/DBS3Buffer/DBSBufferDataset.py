#!/usr/bin/env python
"""
_DBSBufferDataset_

Module that describes a dataset in DBS buffer

Created on May 2, 2013

@author: dballest
"""

from WMCore.WMConnectionBase import WMConnectionBase

class DBSBufferDataset(WMConnectionBase, dict):
    """
    DBSBuffer dataset object, it also contains subscription
    information
    """

    def __init__(self, path, id = -1,
                 processingVer = 0, acquisitionEra = None, validStatus = None,
                 globalTag = None, parent = None, prep_id = None):
        """
        Initialize the stored attributes and
        database connection.
        """
        WMConnectionBase.__init__(self, daoPackage = "WMComponent.DBS3Buffer")

        # Fill out the attributes
        self['path'] = path
        self['id'] = id
        self['processingVer'] = processingVer
        self['acquisitionEra'] = acquisitionEra
        self['validStatus'] = validStatus
        self['globalTag'] = globalTag
        self['parent'] = parent
        self['prep_id'] = prep_id
        self['subscriptions'] = []

    def exists(self):
        """
        _exists_

        Determine whether or not a dataset with this path exists inside the
        database.  Return the datasets's ID if it exists, False otherwise.
        """
        action = self.daofactory(classname = "ListDataset")
        result = action.execute(datasetPath = self["path"],
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())
        if not result:
            return False
        self['id'] = result[0]['id']
        return self['id']

    def create(self):
        """
        _create_

        Create a new dataset with the information stored in the object.
        Return it's ID.
        """
        if self.exists():
            self.load()
        else:
            action = self.daofactory(classname = "NewDataset")
            action.execute(datasetPath = self['path'],
                           acquisitionEra = self['acquisitionEra'],
                           processingVer = self['processingVer'],
                           validStatus = self['validStatus'],
                           globalTag = self['globalTag'],
                           parent = self['parent'],
                           prep_id = self['prep_id'],
                           conn = self.getDBConn(), transaction = self.existingTransaction())
        return self.exists()

    def load(self, subscriptions = True):
        """
        _load_

        Load a the dataset information out the database
        with subscription information included
        """
        action = self.daofactory(classname = "ListDataset")
        result = action.execute(datasetPath = self["path"],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        if result:
            self.update(result[0])
        else:
            raise RuntimeError("No dataset present with path %s" % self['path'])
        if subscriptions:
            self.loadSubscriptions()
        return

    def updateDataset(self):
        """
        _updateDataset_

        Update the database record for this dataset with
        the information contained in this object, it must exist
        """
        if self['id'] == -1:
            raise RuntimeError("Dataset doesn't exist in the database")
        action = self.daofactory(classname = "UpdateDataset")
        action.execute(self["id"],
                       acquisitionEra = self['acquisitionEra'],
                       processingVer = self['processingVer'],
                       validStatus = self['validStatus'],
                       globalTag = self['globalTag'],
                       parent = self['parent'],
                       prep_id = self['prep_id'],
                       conn = self.getDBConn(), transaction = self.existingTransaction())
        return

    def loadSubscriptions(self):
        """
        _loadSubscriptions_

        Load subscription info out of the database
        """
        if not self.exists():
            self.load(subscriptions = False)
        action = self.daofactory(classname = "ListSubscriptions")
        result = action.execute(datasetID = self['id'],
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())
        self['subscriptions'] = result
        return

    def addSubscription(self, subscriptionInformation):
        """
        _addSubscription_

        Associate a new subscription to the dataset
        """
        if not self.exists():
            self.load(subscriptions = False)
        action = self.daofactory(classname = "NewSubscription")
        action.execute(self['id'], subscriptionInformation,
                       conn = self.getDBConn(), transaction = self.existingTransaction())

        # Refresh subscription info
        self.loadSubscriptions()
        return
