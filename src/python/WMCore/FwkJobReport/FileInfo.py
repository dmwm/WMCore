#!/usr/bin/env python
"""
_FileInfo_

Library for finding the information of a file
Primarily intended for use with jobReport, adding info
Not in the Framework XML


"""

__version__ = "$Revision: 1.5 $"
__revision__ = "$Id: FileInfo.py,v 1.5 2010/03/19 15:29:59 mnorman Exp $"




import os
import os.path
import subprocess
import logging


def readAdler32(filename):
    """
    _readAdler32_

    Get the adler32 checksum of a file. Return None on error

    Process line by line and adjust for known signed vs. unsigned issues
      http://docs.python.org/library/zlib.html

    """
    try:
        from zlib import adler32
        sum = 1L
        f = open(filename, 'rb')
        while True:
            line = f.readline(4096) #limit so binary files don't blow up
            if not line:
                break
            sum = adler32(line, sum)
        f.close()
        return '%x' % (sum & 0xffffffffL) # +ve values and convert to hex
    except StandardError, e:
        print('Error computing Adler32 checksum of %s. %s' % (filename, str(e)))



def readCksum(filename):
    """
    _readCksum_

    Run a cksum command on a file an return the checksum value

    """
    ckproc = subprocess.Popen(['cksum', filename],
                              stdout = subprocess.PIPE,
                              stderr = subprocess.PIPE)
    ckproc.wait()
    result = ckproc.stdout.readlines()

    result = result[0]  # Get the only list element
    result = result.strip() # Get rid of crappy characters
    cksum = result.split()[0] # Take the first one
    return cksum



class FileInfo:
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

        pfn = fileReport.PFN
        if pfn.startswith("file:"):
            pfn = pfn.replace("file:", "")

        # First, where the hell is the file
        if not os.path.isfile(pfn):
            msg = "Fed non-valid PFN: %s" %(pfn)
            # Then figure out what to do with it
            # For now, do nothing since we can't find it
            logging.error(msg)
            print msg
            return
        if not os.path.isabs(pfn):
            # Get an absolute path
            pfn = os.path.abspath(pfn) 


        # Now we know it, we better set it
        setattr(fileReport, 'PFN', pfn)

        return self.processFile(filename = pfn,
                                fileReport = fileReport,
                                step = step,
                                outputModule = outputModule)




    def processFile(self, filename, fileReport, step, outputModule):
        """
        This attaches the info to the fileReport instance


        """

        # Get checksums
        adler32 = readAdler32(filename = filename)
        cksum   = readCksum(filename = filename)


        # Get info from spec

        output = getattr(step.output.modules, outputModule)
        disableGUID      = getattr(output, 'disableGUID', False)
        fixedLFN         = getattr(output, 'fixedLFN', False)
        primaryDataset   = output.primaryDataset
        processedDataset = output.processedDataset
        dataTier         = output.dataTier
        datasetPath      = '%s/%s/%s' %(primaryDataset,
                                        processedDataset,
                                        dataTier)

        # Get other file information
        size = os.stat(filename)[6]


        #Get info from file
        mergedLFNBase    = getattr(fileReport, 'MergedLFNBase', None)
        mergedBySize     = getattr(fileReport, 'MergedBySize', False)
        lfn              = getattr(fileReport, 'LFN')

        # Do LFN manipulation
        # First in the standard case
        if not fixedLFN and not disableGUID:
            guid = getattr(fileReport, 'GUID', None)
            if not guid:
                msg = "No GUID for file %s" %(lfn)
                logging.error(msg)
                raise Exception(msg)
            # Then we have to change the LFN to match the GUID
            dirname = os.path.dirname(lfn)
            filelfn = '%s.root' %(str(guid))
            setattr(fileReport, 'LFN', os.path.join(dirname, filelfn))
        elif not fixedLFN and mergedBySize and mergedLFNBase:
            # Then we better do the merge stuff
            # Not tested for now
            mergedLFNBase.rstrip('/')
            newLFN = os.path.join(mergedLFNBase, os.path.basename(lfn))
            setattr(fileReport, 'LFN', newLFN)
        



        # Attach values
        setattr(fileReport, 'checksums', {'adler32': adler32, 'cksum': cksum})
        setattr(fileReport, 'size', size)
        setattr(fileReport, 'primaryDataset', primaryDataset)
        setattr(fileReport, 'processedDataset', processedDataset)
        setattr(fileReport, 'dataTier', dataTier)
        setattr(fileReport, 'datasetPath', datasetPath)

        return fileReport



