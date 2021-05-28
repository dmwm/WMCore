#!/usr/bin/env python
"""
_WMConfigCache_

Being in itself a wrapped class around a config cache
"""

from future import standard_library
standard_library.install_aliases()
import urllib.request, urllib.parse, urllib.error
from builtins import str
from future.utils import with_metaclass
from future.utils import viewkeys, viewvalues

import hashlib
import logging
import traceback
from contextlib import closing

import WMCore.GroupUser.Decorators as Decorators
from Utils.Patterns import Singleton
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Database.CMSCouch import CouchNotFoundError
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.GroupUser.Group import Group
from WMCore.GroupUser.User import makeUser
from WMCore.WMException import WMException


class ConfigCacheException(WMException):
    """
    Placeholder for a smarter exception class


    """


class DocumentCache(with_metaclass(Singleton, object)):
    """DocumentCache holds config ids. Use this class as singleton"""

    def __init__(self, database, detail=True):
        super(DocumentCache, self).__init__()
        self.cache = {}
        self.database = database
        # flag to decide whether update detail or not when it is prefetched
        self.detail = detail

    def __getitem__(self, configID):
        """
        Internal get method to fetch document based on provided ID
        """
        if configID not in self.cache:
            self.prefetch([configID])
        return self.cache[configID]

    def cleanup(self, ids):
        """
        _cleanup_

        Clean-up given ids from local cache
        """
        for rid in ids:
            if rid in self.cache:
                del self.cache[rid]

    def prefetch(self, keys):
        """
        _fetch_

        Pre-fetch given list of documents from CouchDB
        """
        if self.detail:
            options = {'include_docs': True}
        else:
            options = {}
        result = self.database.allDocs(options=options, keys=keys)
        for row in result['rows']:
            if self.detail:
                self.cache[row['id']] = row['doc']
            else:
                self.cache[row['id']] = True


class ConfigCache(WMObject):
    """
    _ConfigCache_

    The class that handles the upload and download of configCache
    artifacts from Couch
    """

    def __init__(self, dbURL, couchDBName=None, id=None, rev=None, usePYCurl=True,
                 ckey=None, cert=None, capath=None, detail=True):
        super(ConfigCache, self).__init__()
        self.dbname = couchDBName
        self.dburl = dbURL
        self.detail = detail
        try:
            self.couchdb = CouchServer(self.dburl, usePYCurl=usePYCurl, ckey=ckey, cert=cert, capath=capath)
            if self.dbname not in self.couchdb.listDatabases():
                self.createDatabase()

            self.database = self.couchdb.connectDatabase(self.dbname)
        except Exception as ex:
            msg = "Error connecting to couch: %s\n" % str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message=msg)

        # local cache
        self.docs_cache = DocumentCache(self.database, self.detail)

        # UserGroup variables
        self.group = None
        self.owner = None

        # Internal data structure
        self.document = Document()
        self.attachments = {}
        self.document['type'] = "config"
        self.document['description'] = {}
        self.document['description']['config_label'] = None
        self.document['description']['config_desc'] = None

        if id != None:
            self.document['_id'] = id
        self.document['pset_tweak_details'] = None
        self.document['info'] = None
        self.document['config'] = None
        return

    def createDatabase(self):
        """
        _createDatabase_

        """
        database = self.couchdb.createDatabase(self.dbname)
        database.commit()
        return database

    def connectUserGroup(self, groupname, username):
        """
        _connectUserGroup_

        """
        self.group = Group(name=groupname)
        self.group.setCouch(self.dburl, self.dbname)
        self.group.connect()
        self.owner = makeUser(groupname, username,
                              couchUrl=self.dburl,
                              couchDatabase=self.dbname)
        return

    def createUserGroup(self, groupname, username):
        """
        _createUserGroup_

        Create all the userGroup information
        """
        self.createGroup(name=groupname)
        self.createUser(username=username)
        return

    def createGroup(self, name):
        """
        _createGroup_

        Create Group for GroupUser
        """
        self.group = Group(name=name)
        self.group.setCouch(self.dburl, self.dbname)
        self.group.connect()
        self.group.create()
        return

    def setLabel(self, label):
        """
        _setLabel_

        Util to add a descriptive label to the configuration doc
        """
        self.document['description']['config_label'] = label

    def setDescription(self, desc):
        """
        _setDescription_

        Util to add a verbose description string to a configuration doc
        """
        self.document['description']['config_desc'] = desc

    @Decorators.requireGroup
    def createUser(self, username):
        self.owner = makeUser(self.group['name'], username,
                              couchUrl=self.dburl,
                              couchDatabase=self.dbname)
        self.owner.create()
        self.owner.ownThis(self.document)
        return

    @Decorators.requireGroup
    @Decorators.requireUser
    def save(self):
        """
        _save_

        Save yourself!  Save your internal document.
        """
        rawResults = self.database.commit(doc=self.document)

        # We should only be committing one document at a time
        # if not, get the last one.

        try:
            commitResults = rawResults[-1]
            self.document["_rev"] = commitResults.get('rev')
            self.document["_id"] = commitResults.get('id')
        except KeyError as ex:
            msg = "Document returned from couch without ID or Revision\n"
            msg += "Document probably bad\n"
            msg += str(ex)
            logging.error(msg)
            raise ConfigCacheException(message=msg)

        # Now do the attachments
        for attachName in self.attachments:
            self.saveAttachment(name=attachName,
                                attachment=self.attachments[attachName])

        return

    def saveAttachment(self, name, attachment):
        """
        _saveAttachment_

        Save an attachment to the document
        """

        retval = self.database.addAttachment(self.document["_id"],
                                             self.document["_rev"],
                                             attachment,
                                             name)

        if retval.get('ok', False) != True:
            # Then we have a problem
            msg = "Adding an attachment to document failed\n"
            msg += str(retval)
            msg += "ID: %s, Rev: %s" % (self.document["_id"], self.document["_rev"])
            logging.error(msg)
            raise ConfigCacheException(msg)

        self.document["_rev"] = retval['rev']
        self.document["_id"] = retval['id']
        self.attachments[name] = attachment

        return

    def loadDocument(self, configID):
        """
        _loadDocument_

        Load a document from the document cache given its couchID
        """
        self.document = self.docs_cache[configID]

    def loadByID(self, configID):
        """
        _loadByID_

        Load a document from the server given its couchID
        """
        try:
            self.document = self.database.document(id=configID)
            if 'owner' in self.document:
                self.connectUserGroup(groupname=self.document['owner'].get('group', None),
                                      username=self.document['owner'].get('user', None))
            if '_attachments' in self.document:
                # Then we need to load the attachments
                for key in viewkeys(self.document['_attachments']):
                    self.loadAttachment(name=key)
        except CouchNotFoundError as ex:
            msg = "Document with id %s not found in couch\n" % (configID)
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message=msg)
        except Exception as ex:
            msg = "Error loading document from couch\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message=msg)

        return

    def loadAttachment(self, name, overwrite=True):
        """
        _loadAttachment_

        Load an attachment from the database and put it somewhere useful
        """

        attach = self.database.getAttachment(self.document["_id"], name)

        if not overwrite:
            if name in self.attachments:
                logging.info("Attachment already exists, so we're skipping")
                return

        self.attachments[name] = attach

        return

    def loadByView(self, view, value):
        """
        _loadByView_

        Underlying code to load views
        """

        viewRes = self.database.loadView('ConfigCache', view, {}, [value])

        if not viewRes['rows']:
            # Then we have a problem
            msg = "Unable to load using view %s and value %s" % (view, str(value))
            logging.error(msg)

        self.unwrapView(viewRes)
        self.loadByID(self.document["_id"])
        return

    def saveConfigToDisk(self, targetFile):
        """
        _saveConfigToDisk_

        Make sure we can save our config file to disk
        """
        config = self.getConfig()
        if not config:
            return

        # Write to a file
        with open(targetFile, 'w') as f:
            f.write(config)
        return

    def load(self):
        """
        _load_

        Figure out how to load
        """

        if self.document.get("_id", None) != None:
            # Then we should load by ID
            self.loadByID(self.document["_id"])
            return

        # Otherwise we have to load by view

        if not self.document.get('md5_hash', None) is None:
            # Then we have an md5_hash
            self.loadByView(view='config_by_md5hash', value=self.document['md5_hash'])
            # TODO: Add more views as they become available.


            # elif not self.owner == None:
            # Then we have an owner
            # self.loadByView(view = 'config_by_owner', value = self.owner['name'])

    def unwrapView(self, view):
        """
        _unwrapView_

        Move view information into the main document
        """

        self.document["_id"] = view['rows'][0].get('id')
        self.document["_rev"] = view['rows'][0].get('value').get('_rev')

    def setPSetTweaks(self, PSetTweak):
        """
        _setPSetTweaks_

        Set the PSet tweak details for the config.
        """
        self.document['pset_tweak_details'] = PSetTweak
        return

    def getPSetTweaks(self):
        """
        _getPSetTweaks_

        Retrieve the PSet tweak details.
        """
        return self.document['pset_tweak_details']

    def getOutputModuleInfo(self):
        """
        _getOutputModuleInfo_

        Retrieve the dataset information for the config in the ConfigCache.
        """
        psetTweaks = self.getPSetTweaks()
        if 'process' not in psetTweaks:
            raise ConfigCacheException("Could not find process field in PSet while getting output modules!")
        try:
            outputModuleNames = psetTweaks["process"]["outputModules_"]
        except KeyError as ex:
            msg = "Could not find outputModules_ in psetTweaks['process'] while getting output modules.\n"
            msg += str(ex)
            logging.error(msg)
            raise ConfigCacheException(msg)

        results = {}
        for outputModuleName in outputModuleNames:
            try:
                outModule = psetTweaks["process"][outputModuleName]
            except KeyError:
                msg = "Could not find outputModule %s in psetTweaks['process']" % outputModuleName
                logging.error(msg)
                raise ConfigCacheException(msg)
            dataset = outModule.get("dataset", None)
            if dataset:
                results[outputModuleName] = {"dataTier": outModule["dataset"]["dataTier"],
                                             "filterName": outModule["dataset"]["filterName"]}
            else:
                results[outputModuleName] = {"dataTier": None, "filterName": None}

        return results

    def addConfig(self, newConfig, psetHash=None):
        """
        _addConfig_


        """
        # The newConfig parameter is a URL suitable for passing to urlopen.
        with closing(urllib.request.urlopen(newConfig)) as f:
            configString = f.read(-1)
        configMD5 = hashlib.md5(configString).hexdigest()

        self.document['md5_hash'] = configMD5
        self.document['pset_hash'] = psetHash
        self.attachments['configFile'] = configString
        return

    def getConfig(self):
        """
        _getConfig_

        Get the currently active config
        """
        return self.attachments.get('configFile', None)

    def getCouchID(self):
        """
        _getCouchID_

        Return the document's couchID
        """

        return self.document["_id"]

    def getCouchRev(self):
        """
        _getCouchRev_

        Return the document's couchRevision
        """

        return self.document["_rev"]

    @Decorators.requireGroup
    @Decorators.requireUser
    def delete(self):
        """
        _delete_

        Deletes the document with the current docid
        """
        if not self.document["_id"]:
            logging.error("Attempted to delete with no couch ID")

        # TODO: Delete without loading first
        try:
            self.database.queueDelete(self.document)
            self.database.commit()
        except Exception as ex:
            msg = "Error in deleting document from couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message=msg)

        return

    def getIDFromLabel(self, label):
        """
        _getIDFromLabel_

        Retrieve the ID of a config given it's label.
        """
        results = self.database.loadView("ConfigCache", "config_by_label",
                                         {"startkey": label,
                                          "limit": 1})

        if results["rows"][0]["key"] == label:
            return results["rows"][0]["value"]

        return None

    def listAllConfigsByLabel(self):
        """
        _listAllConfigsByLabel_

        Retrieve a list of all the configs in the config cache.  This is
        returned in the form of a dictionary that is keyed by label.
        """
        configs = {}
        results = self.database.loadView("ConfigCache", "config_by_label")

        for result in results["rows"]:
            configs[result["key"]] = result["value"]

        return configs

    def __str__(self):
        """
        Make something printable

        """

        return self.document.__str__()

    def validate(self, configID):

        try:
            # TODO: need to change to DataCache
            # self.loadDocument(configID = configID)
            self.loadByID(configID=configID)
        except Exception as ex:
            raise ConfigCacheException("Failure to load ConfigCache while validating workload: %s" % str(ex))

        if self.detail:
            duplicateCheck = {}
            try:
                outputModuleInfo = self.getOutputModuleInfo()
            except Exception as ex:
                # Something's gone wrong with trying to open the configCache
                msg = "Error in getting output modules from ConfigCache during workload validation.  Check ConfigCache formatting!"
                raise ConfigCacheException("%s: %s" % (msg, str(ex)))
            for outputModule in viewvalues(outputModuleInfo):
                dataTier = outputModule.get('dataTier', None)
                filterName = outputModule.get('filterName', None)
                if not dataTier:
                    raise ConfigCacheException("No DataTier in output module.")

                # Add dataTier to duplicate dictionary
                if dataTier not in duplicateCheck:
                    duplicateCheck[dataTier] = []
                if filterName in duplicateCheck[dataTier]:
                    # Then we've seen this combination before
                    raise ConfigCacheException("Duplicate dataTier/filterName combination.")
                else:
                    duplicateCheck[dataTier].append(filterName)
            return outputModuleInfo
        return True
