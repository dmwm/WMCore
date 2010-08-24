#!/usr/bin/env python
"""
_WMConfigCache_

Being in itself a wrapped class around a config cache
"""


import urllib
import logging
import traceback


try:
    import hashlib
except:
    import md5 as hashlib

from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.DataStructs.WMObject import WMObject

from WMCore.WMException import WMException
from WMCore.Database.CMSCouch import CouchNotFoundError


from WMCore.GroupUser.Group import Group
from WMCore.GroupUser.User  import makeUser

import WMCore.GroupUser.Decorators as Decorators


class ConfigCacheException(WMException):
    """
    Placeholder for a smarter exception class


    """




class ConfigCache(WMObject):
    """
    _ConfigCache_

    The class that handles the upload and download of configCache
    artifacts from Couch
    """



    def __init__(self, config, couchDBName = None, id = None, rev = None):
        WMObject.__init__(self, config)

        self.dbname = couchDBName
        self.dburl  = self.config.CoreDatabase.couchurl

        try:
            self.couchdb = CouchServer(self.dburl)
            if self.dbname not in self.couchdb.listDatabases():
                self.createDatabase()

            self.database = self.couchdb.connectDatabase(self.dbname)
        except Exception, ex:
            msg = "Error connecting to couch: %s\n" % str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message = msg)
            


        # Couch variables
        self.id   = id  # Couch ID
        self.rev  = rev # Couch Revision

        # UserGroup variables
        self.group = None
        self.owner = None


        # Internal data structure
        self.document    = Document()
        self.attachments = {}

        self.document['pset_tweak_details'] = None
        self.document['info']               = None
        self.document['config']             = None


        return


    def createDatabase(self):
        """
        _createDatabase_

        """
        database = self.couchdb.createDatabase(self.dbname)

        database.commit()
        return database


    def createUserGroup(self, groupname, username):
        """
        _createUserGroup_

        Create all the userGroup information
        """

        self.createGroup(name = groupname)
        self.createUser(username = username)

        return


    def createGroup(self, name):
        """
        _createGroup_

        Create Group for GroupUser
        """

        self.group = Group(name = name)
        self.group.setCouch(self.dburl, self.dbname)
        self.group.connect()

        return


    @Decorators.requireGroup
    def createUser(self, username):

        self.owner = makeUser(self.group['name'], username,
                              couchUrl = self.dburl,
                              couchDatabase = self.dbname)

        self.owner.ownThis(self.document)

        return


    


    @Decorators.requireGroup
    @Decorators.requireUser
    def save(self):
        """
        _save_

        Save yourself!  Save your internal document.
        """


        rawResults = self.database.commit(doc = self.document)

        # We should only be committing one document at a time
        # if not, get the last one.

        try:
            commitResults = rawResults[-1]

            self.rev = commitResults.get('rev')
            self.id  = commitResults.get('id')

            
        except KeyError, ex:
            msg  = "Document returned from couch without ID or Revision\n"
            msg += "Document probably bad\n"
            msg += str(ex)
            logging.error(msg)
            raise ConfigCacheException(message = msg)


        # Now do the attachments
        for attachName in self.attachments:
            self.saveAttachment(name = attachName,
                                attachment = self.attachments[attachName])
            
        
        return


    def saveAttachment(self, name, attachment):
        """
        _saveAttachment_

        Save an attachment to the document
        """


        retval = self.database.addAttachment(self.id,
                                             self.rev,
                                             attachment,
                                             name)

        if retval.get('ok', False) != True:
            # Then we have a problem
            msg = "Adding an attachment to document failed\n"
            msg += str(retval)
            msg += "ID: %s, Rev: %s" % (self.id, self.rev)
            logging.error(msg)
            raise ConfigCacheException(msg)

        self.rev = retval['rev']
        self.id  = retval['id']

        self.attachments[name] = attachment


        return


    def loadByID(self):
        """
        _loadByID_

        Load a document from the server given its couchID
        """

        try:
            doc = self.database.document(id = self.id)
            if 'owner' in doc.keys():
                self.createUserGroup(groupname = doc['owner'].get('group', None),
                                     username  = doc['owner'].get('user', None))
            if '_attachments' in doc.keys():
                # Then we need to load the attachments
                for key in doc['_attachments'].keys():
                    self.loadAttachment(name = key)
            self.document.update(doc)
        except CouchNotFoundError, ex:
            msg =  "Document with id %s not found in couch\n" % (self.id)
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message = msg)
        except Exception, ex:
            msg =  "Error loading document from couch\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message = msg)

        return

    def loadAttachment(self, name, overwrite = True):
        """
        _loadAttachment_

        Load an attachment from the database and put it somewhere useful
        """


        attach = self.database.getAttachment(self.id, name)

        if not overwrite:
            if name in self.attachments.keys():
                logging.info("Attachment already exists, so we're skipping")
                return

        self.attachments[name] = attach

        return

    def loadByView(self, view, value):
        """
        _loadByView_

        Underlying code to load views
        """

        viewRes = self.database.loadView( 'ConfigCache', view, {}, [value] )

        if len(viewRes['rows']) == 0:
            # Then we have a problem
            logging.error("Unable to load using view %s and value %s" % (view, str(value)))

        if len(viewRes['rows']) > 0:
            print viewRes

        self.unwrapView(viewRes)

        self.loadByID()

        return


    def load(self):
        """
        _load_

        Figure out how to load
        """

        if self.id != None:
            # Then we should load by ID
            self.loadByID()
            return

        # Otherwise we have to load by view

        if not self.document.get('md5_hash', None) == None:
            # Then we have an md5_hash
            self.loadByView(view = 'config_by_md5hash', value = self.document['md5_hash'])
        # TODO: Add more views as they become available.

            
        #elif not self.owner == None:
            # Then we have an owner
            #self.loadByView(view = 'config_by_owner', value = self.owner['name'])
       




    def unwrapView(self, view):
        """
        _unwrapView_
        
        Move view information into the main document
        """

        self.id  = view['rows'][0].get('id')
        self.rev = view['rows'][0].get('value').get('_rev')

        


    def setPSetTweaks(self, PSetTweak):
        """
        _setPSetTweaks_

        Does exactly what it says on the tin
        """

        self.document['pset_tweak_details'] = PSetTweak
        

        return

    def getPSetTweaks(self):
        """
        _getPSetTweaks_

        Returns whatever you have in the current document
        """


        return self.document['pset_tweak_details']


    def addConfig(self, newConfig):
        """
        _addConfig_


        """

        if not self.id:
            # Then we have a non-existant document
            # Hard to add stuff to that
            msg = "Attempting to add config to non-existant document"
            logging.error(msg)


        # The newConfig parameter is a URL suitable for passing to urlopen.
        configString = urllib.urlopen(newConfig).read(-1)
        configMD5 = hashlib.md5(configString).hexdigest()



        if False:
            # Then we have a duplicate file
            # Load from the source
            return


        self.document['md5_hash']      = configMD5
        self.attachments['configFile'] = configString


        # Save the new md5 hash
        self.save()


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

        return self.id


    def getCouchRev(self):
        """
        _getCouchRev_

        Return the document's couchRevision
        """


        return self.rev


    @Decorators.requireGroup
    @Decorators.requireUser
    def delete(self):
        """
        _delete_

        Deletes the document with the current docid
        """
        if not self.id:
            logging.error("Attempted to delete with no couch ID")


        # TODO: Delete without loading first
        try:
            document = self.database.document(self.id)
            self.database.queueDelete(document)
            self.database.commit()
        except Exception, ex:
            msg =  "Error in deleting document from couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            raise ConfigCacheException(message = msg)


        return

    
