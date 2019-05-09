#!/usr/bin/env python
"""
_URLFetcher_

Fetch urls based on the sandbox information in a WMStep

"""
import os
import re
import logging

import WMCore.WMSpec.WMStep as WMStep

from WMCore.WMSpec.Steps.Fetchers.FetcherInterface import FetcherInterface
from WMCore.Services                               import Requests

class URLFetcher(FetcherInterface):
    """
    _URLFetcher_

    Fetch files via URL access based on contents of step sandbox attribute

    """


    def __call__(self, wmTask):
        """
        Trip through steps, pull in files specified in the sandbox section
        PSet Tweaks etc

        """
        for t in wmTask.steps().nodeIterator():
            t = WMStep.WMStepHelper(t)
            stepPath = "%s/%s" % (self.workingDirectory(), t.name())
            for fileInfo in t.data.sandbox:
                # fileInfo.src is the source file
                # fileInfo.injob is where we stuck it
                match = re.search("^.*/(.*?)$", fileInfo.src)
                if (match):
                    fileSuffix = match.group(1)
                else:
                    fileSuffix = "sandboxFile.dat"

                fileTarget = "%s/%s" % (stepPath, fileSuffix)

                # Now build a Request object, make a request, and write
                # the output to a file:
                try:
                    request = Requests.Requests(fileInfo.src)
                    content = request.get('')[0]
                    with open(fileTarget, 'w') as f:
                        f.write(content)
                except IOError as ex:
                    msg =  "Could not write to fileTarget %s\n" % fileTarget
                    msg += str(ex)
                    logging.error(msg)
                    logging.debug("FileInfo: %s" % fileInfo)
                    raise
                fileInfo.injob = fileTarget

        return
