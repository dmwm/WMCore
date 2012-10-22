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
from urllib import URLopener
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

def getRetriever(scheme):
    """
    Get the right retriever function depending on the scheme.
    If scheme is 'http' return urllib.urlretrieve, else if the scheme is https create a URLOpener
    with certificates taken from the X509_USER_PROXY variable. If certificates are not available return
    urllib.urlretrieve as for the http case.
    """
    if os.environ.has_key('X509_USER_PROXY') and os.path.isfile(os.environ['X509_USER_PROXY']):
        certfile = os.environ['X509_USER_PROXY']
    else:
        if scheme == 'https':
            print "User proxy not found. Trying to retrieve the file without using certificates"
        certfile = None

    if scheme == 'http' or not certfile:
        retriever = urllib.urlretrieve
    else:
        print "Using %s as X509 certificate" % certfile
        op = URLopener(None, key_file=certfile, cert_file=certfile)
        op.addheader( 'Accept', 'application/octet-stream' )
        retriever = op.retrieve

    return retriever

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
        if splitResult[0] in ['xrootd', 'root']:
            print "Fetching tarball %s through xrootd" % tarball
            try:
                subprocess.check_call(['xrdcp', '-d', '1', '-f', tarball, 'TEMP_TARBALL.tgz'])
                subprocess.check_call(['tar', 'xzf', 'TEMP_TARBALL.tgz'])
            except subprocess.CalledProcessError:
                print "Couldn't retrieve/extract file from xrootd"
                raise
            finally:
                if os.path.exists('TEMP_TARBALL.tgz'):
                    os.unlink('TEMP_TARBALL.tgz')

        elif splitResult[0] in ['http','https'] and splitResult[1]:
            retriever = getRetriever( splitResult[0] )
            with tempfile.NamedTemporaryFile() as tempFile:
                if setHttpProxy(tarball):
                    try:
                        print 'Fetching URL tarball %s through proxy server' % tarball
                        fileName, headers = retriever(tarball, tempFile.name)
                    except (RuntimeError, IOError):
                        del os.environ['http_proxy']
                        print 'Fetching URL tarball %s after proxy server failure' % tarball
                        fileName, headers = retriever(tarball, tempFile.name)
                else:
                    print 'Fetching URL tarball %s without proxy server' % tarball
                    fileName, headers = retriever(tarball, tempFile.name)

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
