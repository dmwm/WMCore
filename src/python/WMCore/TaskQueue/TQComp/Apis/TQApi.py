#!/usr/bin/env python
"""
Base class of the TQ API.
"""

__all__ = []
__revision__ = "$Id: TQApi.py,v 1.4 2009/12/16 18:09:05 delgadop Exp $"
__version__ = "$Revision: 1.4 $"

import logging
import threading
import time
from TQComp.TQComp import TQComp
from WMCore.Configuration import Configuration
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory


class TQApi(object):
    """
    Base class of the TQ API. It provides the basics to construct
    an object with access to the TQ databases.

    Other classes can extend this one to offer further methods
    that manage the TQ.
    """

    def __init__(self, logger, tqRef, dbIface = None):
        """
        Constructor.

        Param logger is a python logger (required).

        Param tqRef is either a reference to the TQComp object we want
        to interface with (preferred), or the WMCore.Configuration object 
        that was used to configure it.

        Param dbIface is optional. If used, it must be a valid 
        WMCore.Database.Transaction object pointing to the DB interface
        that the TQComp object is using. Otherwise, such interface
        will be retrieved/reconstructed from the tqRef.

        Example of how to create an API from a WMCore component:
          from TQComp.Apis.TQSubmitApi import TQSubmitApi
          from TQComp.Apis.TQApiData import Task

          myThread = threading.currentThread()
          tqApi = TQApi(myThread.logger, self.config, \
                        myThread.transaction)

        How to do create an API from the python interpreter:
          >>> from TQComp.Apis.TQStateApi import TQStateApi
          >>> import logging
          >>> mylogger = logging.getLogger("tqclient")
          >>> confFile = "/pool/TaskQueue/cms_code/WMCore-conf.py"
          >>> from WMCore import Configuration
          >>> myconfig = Configuration.loadConfigurationFile(confFile)
          >>> api = TQApi(mylogger, myconfig, None)

        For many practical purposes, one can instead use the 'tqclient'
        command line interface.
        """
        self.logger = logger
        self.logger.debug("Creating TQApi with params: %s, %s, %s" % \
                          (logger,  type(tqRef), dbIface))
        self.transaction = None 
        if dbIface:
           self.transaction = dbIface

        if isinstance(tqRef, TQComp):
            self.tq = tqRef
            self.conf = None
            self.dialect = self.tq.dialect
            if not self.transaction:
                self.transaction = self.tq.transaction
            
        elif isinstance(tqRef, Configuration):
            self.tq = None
            self.conf = tqRef
            self.dialect = self.conf.CoreDatabase.dialect
            if not self.transaction:
                options = {}
                coreSect = self.conf.CoreDatabase
                if hasattr(coreSect, "socket"):
                    options['unix_socket'] = coreSect.socket
                if hasattr(coreSect, "connectUrl"):
                    dbStr = coreSect.connectUrl
                else:
                    dbStr = self.dialect + '://' + coreSect.user + \
                    ':' + coreSect.passwd+"@"+coreSect.hostname+'/'+\
                    coreSect.name
                self.dbFactory = DBFactory(self.logger, dbStr, options)
                self.dbi = self.dbFactory.connect()
                self.transaction = Transaction(self.dbi)

        else:
            msg = "tqRef should be instance of TQComp or WMCore.Configuration"
            raise ValueError(msg)

        # Make things available for Queries (or others relying in myThread)
        myThread = threading.currentThread()
        if not hasattr(myThread, 'transaction'):
            myThread.transaction = self.transaction
        if not hasattr(myThread, 'logger'):
            myThread.logger = self.logger
        if not hasattr(myThread, 'dbi'):
            myThread.dbi = self.dbi

        if self.dialect == 'mysql':
            self.dialect = 'MySQL'
        self.factory = WMFactory("default", \
           "TQComp.Database." + self.dialect)

        self.queries = self.factory.loadObject("Queries")


