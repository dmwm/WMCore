#!/usr/bin/env python
"""
_Cache_

WMWorkload storage cache.

WMWorkloads are kept in a dir hierarchy built from:

- request type
- acquisition era
- group


"""

import os
import logging
import urllib
import WMCore.WMSpec.WMWorkload as WMWorkload

class WMWorkloadCache:
    """
    Cache manager

    Initialise with the toplevel cache directory

    """
    def __init__(self, cacheDirectory):
        self.cache = cacheDirectory
        self.lfnPrefix = "WMWorkload"
        self.lfnSuffix = "py"

    def getFileName(self, workload):
        #name = WMWorkload.WMWorkloadHelper(workload).name()
        name = workload.request.schema.RequestName
        return "%s-%s.%s" % (name, self.lfnPrefix, self.lfnSuffix)

    def getDir(self, workload):
        return os.path.join(self.cache, self.getPath(workload))

    def getPath(self, workload):
        return os.path.join(self.lfnPrefix,
            workload.request.schema.RequestType,
            workload.owner.Group,
            workload.owner.Requestor)

    def getLfn(self, workload):
        return os.path.join(self.getPath(workload), self.getFileName(workload))

    def getPfn(self, workload):
        return os.path.join(self.getDir(workload), self.getFileName(workload))
            
    def checkIn(self, workload):
        """
        _checkIn_

        Check a WMWorkload instance into the cache
        for the request details provided

        WMWorkload will be saved as:

        CacheDir/WMWorkload/RequestType/Group/Requestor/RequestName

        Returns the LFN
        """
        helper = WMWorkload.WMWorkloadHelper(workload)
        logging.debug("Check In Request: %s" % helper.name())


        dir = self.getDir(workload)
        if not os.path.exists(dir):
            logging.info("checkIn: Creating dir %s" % dir)
            os.makedirs(dir)

        pfn = self.getPfn(workload)
        lfn = self.getLfn(workload)
        msg = "Saving: %s \n" % pfn
        msg += "LFN: %s" % lfn
        logging.info(msg)
        helper.save(pfn)
        return lfn


    def exists(self, workload):
        return os.path.exists(self.getPfn(workload))


    def remove(self, workload):
        """
        _remove_

        Remove the file corresponding to request provided

        """
        pfn = self.getPfn(workload)
        os.remove(pfn)
        logging.info("Removed: %s" % pfn)
        return


def loadFromURL(url):
    """ Returns a WMWorkflow, downloaded from a URL """
    # makes a temp file somewhere
    fm = urllib.urlretrieve(url)
    # add a py suffix so it looks like a module
    pyFile = fm[0]+'.py'
    os.rename(fm[0], pyFile)
    helper = WMWorkload.WMWorkloadHelper()
    helper.load(pyFile)
    os.remove(pyFile)
    # and the pyc file
    if os.path.exists(pyFile+'c'):
       os.remove(pyFile+'c')
    return helper

import WMCore.RequestManager.DataStructs.Request

if __name__ == '__main__':
    cache = WMWorkloadCache('/tmp')
    requestName = 'RunToTheHills'
    workload = WMWorkload.WMWorkload(requestName)
    workload.request.section_('schema')
    workload.request.schema.RequestType = 'ReReco'
    workload.request.schema.RequestName = requestName
    workload.owner.Group = 'Offline'
    workload.owner.Requestor = 'Eddie'
    cache.checkIn(workload)
    pfn = cache.getPfn(workload)
    print pfn
    newWorkloadHelper = WMWorkload.WMWorkloadHelper()
    newWorkloadHelper.load(pfn)
    newSection = newWorkloadHelper.data.request
    #assert(newWorkloadHelper.data.request.schema.RequestName == requestName)
    assert(newWorkloadHelper.data.owner.Requestor == 'Eddie')
    cache.remove(workload)
