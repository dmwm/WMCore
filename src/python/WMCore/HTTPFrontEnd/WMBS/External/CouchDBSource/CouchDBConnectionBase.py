import os
import urllib
import re

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Configuration import loadConfigurationFile

class CouchDBConnectionBase(object):

    def __init__(self, couchConfig):
        self.couchURL = couchConfig.couchURL
        self.acdcDB = couchConfig.acdcDBName
        self.jobDumpDB = couchConfig.jobDumpDBName

    def getCouchDBURL(self):
        return self.couchURL

    def getCouchDBName(self):
        return self.jobDumpDB

    def getCouchACDCURL(self):
        return self.couchURL

    def getCouchACDCName(self):
        return self.acdcDB

    def getCouchDB(self):

        couchServer = CouchServer(dburl = self.couchURL)
        couchDB = couchServer.connectDatabase(dbname = self.jobDumpDB)
        couchDB['timeout'] = 300 # set request timeout 5 min
        return couchDB

    def getCouchJobsDB(self):
        couchServer = CouchServer(dburl = self.couchURL)
        couchDB = couchServer.connectDatabase(dbname = self.jobDumpDB + "/jobs")
        couchDB['timeout'] = 300 # set request timeout 5 min
        return couchDB

    def getCouchACDC(self):

        couchServer = CouchServer(dburl = self.couchURL)
        couchDB = couchServer.connectDatabase(dbname = self.acdcDB)
        return couchDB

    def getCouchACDCHtmlBase(self):
        """
        TODO: currently it is hard code to the front page of ACDC
        When there is more information is available, it can be added
        through
        """

        baseURL = '%s/%s/_design/ACDC/collections.html' % (self.couchURL,
                                                           self.acdcDB)
        baseURL = re.sub('://.+:.+@', '://', baseURL, 1)

        return baseURL

    def getCouchDBHtmlBase(self, database, design, view, path = None, options = {},
                           type = "show"):
        """
        type should be either 'show' or 'list'
        Couch server will raise an error if another type is passed
        """

        baseURL = '%s/%s/_design/%s/_%s/%s' % \
                        (self.couchURL, database, design, type, view)

        baseURL = re.sub('://.+:.+@', '://', baseURL, 1)

        if (options):
            data = urllib.urlencode(options)
            if path:
                baseURL = "%s/%s?%s" % (baseURL, path, data)
            else:
                baseURL = "%s?%s" % (baseURL, data)
        return baseURL
