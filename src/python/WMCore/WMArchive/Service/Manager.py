#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Manager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchiveManager class provides full functionality of
             the WMArchive service.

TODO: This class will be expanded/modified according to requirements
of WMArchive project, see
https://twiki.cern.ch/twiki/bin/viewauth/CMS/WMArchive
"""

# system modules
import os
import sys
import time

class WMArchiveManager(object):
    def __init__(self, config=None):
        """
        Initialize WMArchive proxy server configuration. The given configuration
        file will provide details of proxy server, agent information, etc.
        """
        self.config = config
        self._version = "1.0.0"

    def version(self):
        return self._version

    def encode(self, docs):
        """
        Encode given set of documents into appropriate format for long term storage.
        This method will consume documents in DMWM JSON format.
        Yield encoded documents to the client.
        """
        for doc in docs:
            doc['ts'] = time.time()
            # do more transformation
            yield doc

    def decode(self, docs):
        """
        Decode given set of documents into DMWM JSON format.
        Yield decoded documents to the client.
        """
        for doc in docs:
            yield doc

    def get_ids(self, docs):
        """
        Extract ids from given set of documents
        """
        for doc in docs:
            yield doc['uid']

    def write(self, data, chunk_size=None):
        """
        Write given data chunk (list of WM documents) into proxy server.
        The chunk_size default value will be determined by back-end throughput.
        Return true or false of write operation.
        """
        if  isinstance(data, dict):
            data = [data]
        if  not isinstance(data, list):
            raise Exception("WMArchiveManager::write, Invalid data format: %s" % type(data))
#        ids = self.get_ids(data)
#        docs = self.encode(data)
        # write docs into back-end
        # get doc-ids of written docs
        return True

    def read(self, query):
        """
        Send request to proxy server to read data for given query.
        Yield list of found documents or None.
        """
        # request data from back-end

    def ack(self, docIds):
        """
        Send acknowledgement request to proxy server for given set of ids.
        Return true or false if data is present on back-end server.
        """
        # request confirmation about the data from ba
