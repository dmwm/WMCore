#!/usr/bin/env python
"""
_FileInfo_

Library for finding the information of a file
Primarily intended for use with jobReport, adding info
Not in the Framework XML


"""
from __future__ import print_function


import os
import os.path
import logging

from Utils.FileTools import calculateChecksums


class FileInfo(object):
    """

    Package to hold all info regarding file

    """


    def __init__(self):
        """
        Doesn't do nothing

        """

    def __call__(self, fileReport, step, outputModule):
        """
        This takes a file from the report as an argument
        And extracts everything from there

        """

        pfn = fileReport.pfn
        if pfn.startswith("file:"):
            pfn = pfn.replace("file:", "")

        # First, where the hell is the file
        if not os.path.isfile(pfn):
            msg = "Fed non-valid PFN: %s" %(pfn)
            # Then figure out what to do with it
            # For now, do nothing since we can't find it
            logging.error(msg)
            print(msg)
            return
        if not os.path.isabs(pfn):
            # Get an absolute path
            pfn = os.path.abspath(pfn)


        # Now we know it, we better set it
        setattr(fileReport, 'pfn', pfn)

        # Set default of merged as false
        setattr(fileReport, 'merged', False)

        return self.processFile(filename = pfn,
                                fileReport = fileReport,
                                step = step,
                                outputModule = outputModule)




    def processFile(self, filename, fileReport, step, outputModule):
        """
        This attaches the info to the fileReport instance


        """
        # Get checksum
        (adler32, cksum) = calculateChecksums(filename)

        # Get info from spec
        output = getattr(step.output.modules, outputModule)
        disableGUID      = getattr(output, 'disableGUID', False)
        fixedLFN         = getattr(output, 'fixedLFN', False)
        primaryDataset   = output.primaryDataset
        processedDataset = output.processedDataset
        dataTier         = output.dataTier

        # Get other file information
        size = os.stat(filename)[6]

        #Get info from file
        mergedLFNBase    = getattr(fileReport, 'MergedLFNBase', None)
        mergedBySize     = getattr(fileReport, 'MergedBySize', False)
        lfn              = getattr(fileReport, 'lfn')

        # Do LFN manipulation
        # First in the standard case
        if not fixedLFN and not disableGUID:
            guid = getattr(fileReport, 'guid', None)
            if not guid:
                msg = "No GUID for file %s" %(lfn)
                logging.error(msg)
                raise Exception(msg)
            # Then we have to change the LFN to match the GUID
            dirname = os.path.dirname(lfn)
            filelfn = '%s.root' %(str(guid))
            setattr(fileReport, 'lfn', os.path.join(dirname, filelfn))
        elif not fixedLFN and mergedBySize and mergedLFNBase:
            # Then we better do the merge stuff
            # Not tested for now
            mergedLFNBase.rstrip('/')
            newLFN = os.path.join(mergedLFNBase, os.path.basename(lfn))
            setattr(fileReport, 'lfn', newLFN)




        # Attach values
        setattr(fileReport, 'checksums', {'adler32': adler32, 'cksum': cksum})
        setattr(fileReport, 'size', size)
        setattr(fileReport, "dataset", {"applicationName": "cmsRun",
                                        "applicationVersion": step.application.setup.cmsswVersion,
                                        "primaryDataset": primaryDataset,
                                        "processedDataset": processedDataset,
                                        "dataTier": dataTier})

        return fileReport
