import os
import urllib
import re

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Configuration import loadConfigurationFile

class CouchDBConnectionBase(object):

    wmAgentConfig = None

    @staticmethod
    def setCouchDBConfig():
        if not CouchDBConnectionBase.wmAgentConfig:
            if not os.environ.has_key("WMAGENT_CONFIG"):
                msg = "Please set WMAGENT_CONFIG to \
                       point at your WMAgent configuration."
                #TODO raise proper exception:
                raise Exception(msg)

            if not os.path.exists(os.environ["WMAGENT_CONFIG"]):
                msg = "Can't find config: %s" % os.environ["WMAGENT_CONFIG"]
                #TODO raise proper exception:
                raise Exception(msg)

            CouchDBConnectionBase.wmAgentConfig = \
                 loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    @staticmethod
    def getCouchDBURL():
        CouchDBConnectionBase.setCouchDBConfig()
        return CouchDBConnectionBase.wmAgentConfig.JobStateMachine.couchurl

    @staticmethod
    def getCouchDBName():
        CouchDBConnectionBase.setCouchDBConfig()
        return CouchDBConnectionBase.wmAgentConfig.JobStateMachine.couchDBName
    
    @staticmethod
    def getCouchACDCURL():
        CouchDBConnectionBase.setCouchDBConfig()
        return CouchDBConnectionBase.wmAgentConfig.ACDC.couchurl
    
    @staticmethod
    def getCouchACDCName():
        CouchDBConnectionBase.setCouchDBConfig()
        return CouchDBConnectionBase.wmAgentConfig.ACDC.database
    
    @staticmethod
    def getCouchDB():

        couchServer = CouchServer(dburl =
                                  CouchDBConnectionBase.getCouchDBURL())
        couchDB = couchServer.connectDatabase(dbname =
                                  CouchDBConnectionBase.getCouchDBName())

        return couchDB
    
    @staticmethod
    def getCouchACDC():

        couchServer = CouchServer(dburl =
                                  CouchDBConnectionBase.getCouchACDCURL())
        couchDB = couchServer.connectDatabase(dbname =
                                  CouchDBConnectionBase.getCouchACDCName())

        return couchDB

    @staticmethod
    def getCouchACDCHtmlBase():
        """
        TODO: currently it is hard code to the front page of ACDC
        When there is more information is available, it can be added
        through 
        """
        CouchDBConnectionBase.setCouchDBConfig()
        serverURL = CouchDBConnectionBase.getCouchACDCURL()
        couchACDCName = CouchDBConnectionBase.getCouchACDCName()

        baseURL = '%s/%s/_design/ACDC/collections.html' % (serverURL, 
                                                           couchACDCName) 
        baseURL = re.sub('://.+:.+@', '://', baseURL, 1)

        return baseURL

    @staticmethod
    def getCouchDBHtmlBase(design, view, path = None, options = {}, type = "show"):
        """
        type should be either 'show' or 'list'
        Couch server will raise an error if another type is passed
        """
        CouchDBConnectionBase.setCouchDBConfig()
        serverURL = CouchDBConnectionBase.getCouchDBURL()
        couchDBName = CouchDBConnectionBase.getCouchDBName()

        baseURL = '%s/%s/_design/%s/_%s/%s' % \
                        (serverURL, couchDBName, design, type, view)

        baseURL = re.sub('://.+:.+@', '://', baseURL, 1)

        if (options):
            data = urllib.urlencode(options)
            if path:
                baseURL = "%s/%s?%s" % (baseURL, path, data)
            else:
                baseURL = "%s?%s" % (baseURL, data)
        return baseURL

    @staticmethod
    def getWMAgentConfig():
        CouchDBConnectionBase.setCouchDBConfig()
        return CouchDBConnectionBase.wmAgentConfig
