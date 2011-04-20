#!/usr/bin/env python
"""
_UnpackUserTarball_

Unpack the user tarball and put it's contents in the right place
"""

import os
import shutil
import subprocess
import sys
import tempfile
import urllib
import urlparse

def UnpackUserTarball():
    tarballs = []
    userFiles = []
    print "Called with %s params" % len(sys.argv)
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
            print 'Fetching URL tarball %s' % tarball
            with tempfile.NamedTemporaryFile() as tempFile:
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
