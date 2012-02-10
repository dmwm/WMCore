#!/usr/bin/env python
"""
_UnpackUserTarball_

Unpack the user tarball and put it's contents in the right place
"""

import commands
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib
import urlparse


def setHttpProxy(url):
    """
    Use frontier to figure out the http_proxies.
    Pick one deterministically based on the url and loadbalance settings
    """
    if os.environ.has_key('http_proxy'):
        return os.environ['http_proxy']

    status, output = commands.getstatusoutput('cmsGetFnConnect frontier://smallfiles')
    if status:
        return None

    proxyList = re.findall('\(proxyurl=([\w\d\.\-\:\/]+)\)', output)
    if 'loadbalance=proxies' in output:
        proxy = proxyList[hash(url) % len(proxyList)]
    else:
        proxy = proxyList[0]
    os.environ['http_proxy'] = proxy
    return proxy

def UnpackUserTarball():
    tarballs = []
    userFiles = []
    if len(sys.argv) > 1:
        tarballs = sys.argv[1].split(',')
    if len(sys.argv) > 2:
        userFiles = sys.argv[2].split(',')

    jobDir = os.environ['WMAGENTJOBDIR']

    for tarball in tarballs:
        splitResult = urlparse.urlsplit(tarball)
        tarFile = os.path.join(jobDir, os.path.basename(tarball))

        # Is it a URL or a file that exists in the jobDir?

        if splitResult[0] in ['http','https'] and splitResult[1]:
            with tempfile.NamedTemporaryFile() as tempFile:
                if setHttpProxy(tarball):
                    try:
                        print 'Fetching URL tarball %s through proxy server' % tarball
                        fileName, headers = urllib.urlretrieve(tarball, tempFile.name)
                    except RuntimeError:
                        del os.environ['http_proxy']
                        print 'Fetching URL tarball %s after proxy server failure' % tarball
                        fileName, headers = urllib.urlretrieve(tarball, tempFile.name)
                else:
                    print 'Fetching URL tarball %s without proxy server' % tarball
                    fileName, headers = urllib.urlretrieve(tarball, tempFile.name)

                try:
                    subprocess.check_call(['tar', 'xzf', fileName])
                except subprocess.CalledProcessError:
                    raise RuntimeError('Error extracting %s' % tarball)
        elif os.path.isfile(tarFile):
            print "Untarring ", tarFile
            subprocess.check_call(['tar', 'xzf', tarFile])
        else:
            raise IOError('%s does not exist' % tarFile)

    for userFile in userFiles:
        if userFile:
            print "Moving", userFile, "to execution directory."
            shutil.move(userFile, '..')

    return 0

if __name__ == '__main__':
    sys.exit(UnpackUserTarball())
