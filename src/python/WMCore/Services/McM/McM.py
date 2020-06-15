#! /bin/env python

"""
A service class for retrieving data from McM using
an SSO cookie since it sits behind CERN SSO
"""

import json
import os
import pycurl
import subprocess

from io import BytesIO

from WMCore.WMException import WMException

class McMNoDataError(WMException):
    """
    _McMNoDataError_
        McM responded but has no data for the request
    """

    def __init__(self):
        WMException.__init__(self, 'McM responded correctly but has no data')

class McM(object):
    """
    A service class for retrieving data from McM using
    an SSO cookie since it sits behind CERN SSO
    'key' must be unencrypted
    """

    def __init__(self, cert, key, url='https://cms-pdmv.cern.ch/mcm', tmpDir='/tmp'):
        self.url = url
        self.tmpDir = tmpDir
        self.cert = cert
        self.key = key
        self.cookieFile = None

    def __enter__(self):
        self.cookieFile = os.path.join(self.tmpDir, 'ssoCookie.txt')
        process = subprocess.Popen(["cern-get-sso-cookie", "--cert", self.cert, "--key", self.key,
                                    "-u", self.url, "-o", self.cookieFile],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        strout = process.communicate()[0]
        if process.returncode != 0:
            raise RuntimeError(" FATAL -- could not generate SSO cookie\nError msg: %s" % (str(strout)))
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.cookieFile:
            try:
                os.remove(self.cookieFile)
            except OSError:
                return
        return

    def _getURL(self, extendURL):
        """
        Fetch an MCM URL with CURL using the SSO cookie.
        Only intended to be used internally
        """

        try:
            b = BytesIO()
            c = pycurl.Curl()

            fullUrl = '%s/%s' % (self.url, extendURL)

            c.setopt(c.URL, fullUrl)
            c.setopt(c.SSL_VERIFYPEER, False)
            c.setopt(c.SSL_VERIFYHOST, False)
            c.setopt(c.FOLLOWLOCATION, True)
            c.setopt(c.COOKIEJAR, self.cookieFile)
            c.setopt(c.COOKIEFILE, self.cookieFile)
            c.setopt(c.WRITEFUNCTION, b.write)
            c.perform()
            if c.getinfo(pycurl.HTTP_CODE) != 200:
                raise IOError
        except:
            c.close()
            raise IOError('Was not able to fetch or decode URL from McM')

        try:
            body = b.getvalue()
            res = json.loads(body)
        except ValueError:
            c.close()
            raise IOError('Was not able to decode JSON from McM')

        c.close()
        return res

    def getHistory(self, prepID):
        """
        Get the history record which has who did what to an McM request
        """

        try:
            url = 'search?db_name=batches&contains=%s&get_raw' % prepID
            res = self._getURL(url)
            history = res['rows'][0]['doc']['history']
            return history
        except IndexError:
            raise McMNoDataError

    def getRequest(self, prepID):
        """
        Get the request record which has, among other things,
        the number of requested events
        """

        url = 'public/restapi/requests/get/%s' % prepID
        res = self._getURL(url)
        return res['results']

if __name__ == '__main__':
    with McM(cert='.globus/usercert.pem', key='.globus/nopasskey.pem') as mcm:
        history = mcm.getHistory(prepID = 'BTV-Upg2023SHCAL14DR-00002')
        request = mcm.getRequest(prepID = 'BTV-Upg2023SHCAL14DR-00002')
