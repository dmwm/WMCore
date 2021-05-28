#!/usr/bin/env python
# encoding: utf-8
"""
Interface.py

Created by Dave Evans on 2010-07-21.
Copyright (c) 2010 Fermilab. All rights reserved.
"""
from __future__ import print_function



from builtins import str, object

import WMCore.Database.CMSCouch as CMSCouch

class CouchConnectionError(Exception):
    """docstring for CouchConnectionError"""
    def __init__(self, arg):
        super(CouchConnectionError, self).__init__()
        self.arg = arg


class Interface(object):
    def __init__(self, couchUrl, couchDatabase):
        self.cdb_url = couchUrl
        self.cdb_database = couchDatabase
        try:
            self.cdb_server = CMSCouch.CouchServer(self.cdb_url)
            self.couch = self.cdb_server.connectDatabase(self.cdb_database)
        except Exception as ex:
            msg = "Exception instantiating couch services for :\n"
            msg += " url = %s\n database = %s\n" % (self.cdb_url, self.cdb_database)
            msg += " Exception: %s" % str(ex)
            print(msg)
            raise CouchConnectionError(msg)


    def documentsOwned(self, group, user):
        """
        _documentsOwned_

        Get a list of doc IDs that are owned by the group/user pair

        """
        result = self.couch.loadView("GroupUser", 'owner_group_user',
                 {'startkey' :[group, user],
                   'endkey' : [group, user]}, []
                )
        output = [str(x[u'id']) for x in result[u'rows']]
        return output



    def callUpdate(self, update, document, **args ):
        """
        _callUpdate_

        Wrapper primarily for calling/testing update methods on the groupuser couchapp

         * update - name of the update method in the couchapp
         * document - ID of the document to be updated
         * args  - key:value dict of args to be encoded in the PUT request

        """
        updateUri = "/" + self.couch.name + "/_design/GroupUser/_update/"+ update + "/" + document
        argsstr = "?"
        for k, v in list(args.items()):
            argsstr += "%s=%s&" % (k, v)
        updateUri += argsstr
        updateUri= updateUri[:-1]
        self.couch.makeRequest(uri = updateUri, type = "PUT", decode = False)
        return
