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
        uploadURL = self['endpoint'] + '/userfilecache/upload/'
        params = [('checksum', self.checksum(fileName))]
        if subDir or name:
            params.append(('subDir', subDir))
            params.append(('name', name))

        resString = uploadFile(fileName=fileName, fieldName='userfile', url=uploadURL, params=params)
        try:
            result = json.loads(resString)
        except ValueError:
            result = json.loads(resString.replace("'", '"'))
        return result

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
