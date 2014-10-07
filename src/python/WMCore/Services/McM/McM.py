#! /bin/env python

import json
import os
import pycurl
import subprocess

from StringIO import StringIO

class McM():
    def __init__(self, cert, key, url='https://cms-pdmv.cern.ch/mcm', tmpDir='/tmp'):
        self.url = url
        self.tmpDir = tmpDir
        self.cert = cert
        self.key = key
        self.cookieFile = None

    def __enter__(self):
        self.cookieFile = os.path.join(self.tmpDir, 'ssoCookie.txt')
        process = subprocess.Popen(["cern-get-sso-cookie", "--cert", self.cert, "--key", self.key, "-u", self.url, "-o", self.cookieFile], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        strout = process.communicate()[0]
        if process.returncode != 0:
            raise Exception(" FATAL -- could not generate SSO cookie\nError msg: %s" % (str(strout)))
        return self

    def __exit__(self, type, value, traceback):
        if self.cookieFile:
            try:
                os.remove(self.cookieFile)
            except OSError:
                return
        return

    def _getURL(self, extendURL):
        try:
            b = StringIO()
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

            body = b.getvalue()
            res = json.loads(body)
            c.close()
            return res
        except:
            raise(IOError, 'Was not able to fetch or decode URL from McM')

    def getHistory(self, prepID):
        url = 'search?db_name=batches&contains=%s&get_raw' % prepID
        try:
            res = self._getURL(url)
            history = res['rows'][0]['doc']['history']
            return history
        except (IndexError, IOError):
            return []

    def getRequest(self, prepID):
        try:
            url = 'public/restapi/requests/get/%s' % prepID
            res = self._getURL(url)
            return res['results']
        except IOError:
            return {}

if __name__ == '__main__':
    with McM(cert = '.globus/usercert.pem', key = '.globus/nopasskey.pem') as mcm:
        import pdb
        history = mcm.getHistory(prepID = 'BTV-Upg2023SHCAL14DR-00002')
        request = mcm.getRequest(prepID = 'BTV-Upg2023SHCAL14DR-00002')
        pdb.set_trace()
