#!/usr/bin/env python
"""
_UserFileCache_

API for UserFileCache service
"""

import os
import json
import shutil
import hashlib
import tarfile
import tempfile

from WMCore.Services.Service import Service


def calculateChecksum(tarfile_, exclude=None):
    """
    Calculate the checksum of the tar file in input.

    The tarfile_ input parameter could be a string or a file object (anything compatible
    with the fileobj parameter of tarfile.open).

    The exclude parameter could be a list of strings, or a callable that takes as input
    the output of  the list of tarfile.getmembers() and return a list of strings.
    The exclude param is interpreted as a list of files that will not be taken into consideration
    when calculating the checksum.

    The output is the checksum of the tar input file.

    The checksum is calculated taking into consideration the names of the objects
    in the tarfile (files, directories etc) and the content of each file.

    Each file is exctracted, read, and then deleted right after the input is passed
    to the hasher object. The file is read in chuncks of 4096 bytes to avoid memory
    issues.
    """
    if exclude==None: #[] is a dangerous value for a param
        exclude = []

    hasher = hashlib.sha256()

    ## "massage" out the input parameters
    if isinstance(tarfile_, basestring):
        tar = tarfile.open(tarfile_, mode='r')
    else:
        tar = tarfile.open(fileobj=tarfile_, mode='r')

    if exclude and hasattr(exclude, '__call__'):
        excludeList = exclude(tar.getmembers())
    else:
        excludeList = exclude


    tmpDir = tempfile.mkdtemp()
    try:
        for tarmember in tar:
            if tarmember.name in excludeList:
                continue
            hasher.update(tarmember.name)
            if tarmember.isfile() and tarmember.name.split('.')[-1]!='pkl':
                tar.extractall(path=tmpDir, members=[tarmember])
                fn = os.path.join(tmpDir, tarmember.name)
                with open(fn, 'rb') as fd:
                    while True:
                        buf = fd.read(4096)
                        if not buf:
                            break
                        hasher.update(buf)
                os.remove(fn)
    finally:
        #never leave tmddir around
        shutil.rmtree(tmpDir)
    checksum = hasher.hexdigest()

    return checksum


class UserFileCache(Service):
    """
    API for UserFileCache service
    """
    # Should be filled out with other methods: download, exists

    def __init__(self, mydict=None):
        if mydict==None: #dangerous {} default value
            mydict = {}
        mydict['endpoint'] =  mydict.get('endpoint', 'https://cmsweb.cern.ch/crabcache/')
        Service.__init__(self, mydict)
        self['requests']['accept_type'] = 'application/json'

        if 'proxyfilename' in mydict:
            #in case there is some code I have not updated in ticket #3780. Should not be required... but...
            self['logger'].warning('The UserFileCache proxyfilename parameter has been replace with the more'
                                   ' general (ckey/cert) pair.')

    def downloadLog(self, fileName, output):
        """
        """
        url = self['endpoint'] + 'logfile?name=%s' % os.path.split(fileName)[1]

        self['logger'].info('Fetching URL %s' % url)
        fileName, dummyHeader = self['requests'].downloadFile(output, str(url)) #unicode broke pycurl.setopt
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
        return json.loads(result[0])['result'][0]

    def download(self, hashkey, output, username=None):
        """
        Download tarfile with the provided hashkey.
        """
        url = self['endpoint'] + 'file?hashkey=%s' % hashkey
        if username:
            url = url + '&username=%s' % username

        self['logger'].info('Fetching URL %s' % url)
        fileName, dummyHeader = self['requests'].downloadFile(output, str(url)) # unicode broke pycurl.setopt
        self['logger'].debug('Wrote %s' % fileName)
        return fileName

    def upload(self, fileName, excludeList = None):
        """
        Upload the tarfile fileName to the user file cache. Returns the hash of the content of the file
        which can be used to retrieve the file later on.
        """
        if excludeList==None: #pylint says [] is a dangerous default value
            excludeList = []

        #The parameter newchecksum tells the crabcache to use the new algorithm. It's there
        #to guarantee backward compatibility.
        params = [('hashkey', calculateChecksum(fileName, excludeList)), ('newchecksum', '2')]

        resString = self["requests"].uploadFile(fileName=fileName, fieldName='inputfile',
                                                url=self['endpoint'] + 'file',
                                                params=params, verb='PUT')

        return json.loads(resString)['result'][0]

