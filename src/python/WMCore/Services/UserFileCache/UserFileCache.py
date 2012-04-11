#!/usr/bin/env python
"""
_UserFileCache_

API for UserFileCache service
"""

import hashlib
import json
import logging
import tarfile

from WMCore.Services.Service import Service
from WMCore.Services.Requests import uploadFile, downloadFile


class UserFileCache(Service):
    """
    API for UserFileCache service
    """
    # Should be filled out with other methods: download, exists

    def __init__(self, dict=None):

        if not dict:
            dict = {}
        if not dict.has_key('endpoint'):
            dict['endpoint'] = "http://cms-xen38.fnal.gov:7725/userfilecache/"
        if not dict.has_key('proxyfilename'):
            dict['proxyfilename'] = None
        if not dict.has_key('capath'):
            dict['capath'] = None
        #TODO: Temporary flag used to indicate that the UserFileCache is talking with the new REST
        #Remove when the branch 3.0.x of the CRABClient is deprecated
        if not dict.has_key('newrest'):
            dict['newrest'] = False
        Service.__init__(self, dict)

    def download(self, hashkey=None, subDir=None, name=None, output=None):
        """
        Download file
        """
        # FIXME: option for temp file if output=None
        if hashkey:
            url = self['endpoint'] + 'download?hashkey=%s' % hashkey
        else:
            url = self['endpoint'] + 'download?subDir=%s;name=%s' % (subDir, name)

        self['logger'].info('Fetching URL %s' % url)
        fileName, header = downloadFile(output, url)
        self['logger'].debug('Wrote %s' % fileName)
        return fileName

    def upload(self, fileName, subDir=None, name=None):
        """
        Upload the file
        """
        #TODO: the following three lines will not be needed anymore if we only support the new REST
        endpointSuffix = '/userfilecache/upload/' if not self['newrest'] else ''
        cksumParam = 'checksum' if not self['newrest'] else 'hashkey'
        fieldName = 'userfile' if not self['newrest'] else 'inputfile'
        verb = 'POST' if not self['newrest'] else 'PUT'

        uploadURL = self['endpoint'] + endpointSuffix
        params = [(cksumParam, self.checksum(fileName))]
        if subDir or name:
            params.append(('subDir', subDir))
            params.append(('name', name))

        resString = uploadFile(fileName=fileName, fieldName=fieldName, url=uploadURL, params=params, \
                                                 verb=verb, ckey=self['proxyfilename'], cert=self['proxyfilename'], capath=self['capath'] )

        return json.loads(resString)
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
