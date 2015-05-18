#!/usr/bin/env python
"""
_UserFileCache_

API for UserFileCache service
"""

import os
import hashlib
import json
import logging
import tarfile

from WMCore.Services.Service import Service


class UserFileCache(Service):
    """
    API for UserFileCache service
    """
    # Should be filled out with other methods: download, exists

    def __init__(self, dict={}):
        dict['endpoint'] =  dict.get('endpoint', 'https://cmsweb.cern.ch/crabcache/')
        Service.__init__(self, dict)
        self['requests']['accept_type'] = 'application/json'

        if 'proxyfilename' in dict:
            #in case there is some code I have not updated in ticket #3780. Should not be required... but...
            self['logger'].warning('The UserFileCache proxyfilename parameter has been replace with the more'
                                   ' general (ckey/cert) pair.')

    def downloadLog(self, fileName, output):
        """
        """
        url = self['endpoint'] + 'logfile?name=%s' % os.path.split(fileName)[1]

        self['logger'].info('Fetching URL %s' % url)
        fileName, header = self['requests'].downloadFile(output, str(url)) #unicode broke pycurl.setopt
        self['logger'].debug('Wrote %s' % output)
        return fileName

    def uploadLog(self, fileName, uploadName=None):
        """
        """
        if not uploadName:
            uploadName = os.path.split(fileName)[1]
        params = [('name', uploadName)]

        resString = self["requests"].uploadFile(fileName=fileName, fieldName='inputfile',
                                                url=self['endpoint'] + 'logfile',
                                                params=params, verb='PUT')

        return json.loads(resString)['result'][0]

    def removeFile(self, haskey):

        result=self['requests'].makeRequest(uri = 'info', data = {'subresource':'fileremove', 'hashkey': haskey})
        return result[0]['result'][0]

    def download(self, hashkey, output):
        """
        Download tarfile with the provided hashkey.
        """
        url = self['endpoint'] + 'file?hashkey=%s' % hashkey

        self['logger'].info('Fetching URL %s' % url)
        fileName, header = self['requests'].downloadFile(output, str(url)) #unicode broke pycurl.setopt
        self['logger'].debug('Wrote %s' % fileName)
        return fileName

    def upload(self, fileName):
        """
        Upload the tarfile fileName to the user file cache. Returns the hash of the content of the file
        which can be used to retrieve the file later on.
        """
        params = [('hashkey', self.checksum(fileName))]

        resString = self["requests"].uploadFile(fileName=fileName, fieldName='inputfile',
                                                url=self['endpoint'] + 'file',
                                                params=params, verb='PUT')

        return json.loads(resString)['result'][0]

    def checksum(self, fileName):
        """
        Calculate the checksum of the file. We don't just hash the contents because
        that includes the timestamp of when the tar was made, not just the timestamps
        of the constituent files
        """

        tar = tarfile.open(fileName, mode='r')
        lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tar.getmembers()]
        hasher = hashlib.sha256(str(lsl))
        checksum = hasher.hexdigest()

        return checksum
