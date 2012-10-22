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


class UserFileCache(Service):
    """
    API for UserFileCache service
    """
    # Should be filled out with other methods: download, exists

    def __init__(self, dict={}):
        dict['endpoint'] =  dict.get('endpoint', 'https://cmsweb.cern.ch/crabcache/')
        Service.__init__(self, dict)

        if dict.has_key('proxyfilename'):
            #in case there is some code I have not updated in ticket #3780. Should not be required... but...
            self['logger'].warning('The UserFileCache proxyfilename parameter has been replace with the more'
                                   ' general (ckey/cert) pair.')

    def download(self, hashkey=None, name=None, output=None):
        """
        Download file. If hashkey is provided use it. Otherwise use filename. At least one
        of them should be provided.
        """
        # FIXME: option for temp file if output=None
        if hashkey:
            url = self['endpoint'] + 'file?hashkey=%s' % hashkey
        else:
            url = self['endpoint'] + 'file?inputfilename=%s' % name

        self['logger'].info('Fetching URL %s' % url)
        fileName, header = self['requests'].downloadFile(output, str(url)) #unicode broke pycurl.setopt
        self['logger'].debug('Wrote %s' % fileName)
        return fileName

    def upload(self, fileName, name=None):
        """
        Upload the file
        """
        params = [('hashkey', self.checksum(fileName))]
        if name:
            params.append(('inputfilename', name))

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
