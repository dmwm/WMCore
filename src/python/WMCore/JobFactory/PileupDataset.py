#!/usr/bin/env python
"""

_PileupDataset_

Object that retrieves and contains a list of lfns for some pileup dataset.

Provides randomisation of access to the files, with two modes:

- No Overlap:  List of LFNs diminishes, and no pileup file is used twice per job
- Overlap:  Random selection of files from the list.


"""
import random
import logging

from WMCore.WMBS.Fileset import Fileset


class PileupDataset(dict):
    """
    _PileupDataset_

    List of files in a pileup dataset.

    Includes random access with and without overlap, and also a persistency
    mechanism to save state to
    a file if required
    
    """
    def __init__(self, fileset):
        dict.__init__(self) # map of cfg node name: list of files 
        self.fileset = fileset 
        self.maxFilesPerJob = 100
        

    def getPileupFiles(self, *sites):
        """
        _getPileupFiles_

        Get next randomised set of files. Returns list of filesPerJob lfns.
        If overlap is true, then the files are not removed from the list.
        If overlap is false, then the files are pruned from the list. When the list
        runs out of files, it will throw an exception
        
        TODO: Persistency and overlap functionality is not currently supported

        """
        logging.debug("Max PU Files: %s" % self.maxFilesPerJob)
        possibleFiles = []
        if len(sites) > 0:
            logging.debug("Pileup Site Limit: %s" % sites)
            #  //
            # // Site limit - ignore files not at all requested sites
            #//
            matchedFiles = set()
            for file in self.fileset.listFiles():
                atSites = True
                for site in sites:
                    if site not in file.locations:
                        atSites = False
                        break
                if atSites:
                    matchedFiles.add(file)
                
            logging.debug("Matched Pileup Files: %s" % list(matchedFiles))
            possibleFiles.extend(matchedFiles)
            
        else:
            #  //
            # // no site limit => all files
            #//
            logging.debug("No Site Limit on Pileup")
            possibleFiles.extend(self.fileset.listFiles())

        #  //
        # // Select the files to return, start with something really simple.
        #//
        random.shuffle(possibleFiles)
        
        if len(possibleFiles) < self.maxFilesPerJob:
            return possibleFiles

        #return possibleFiles[0:self.maxFilesPerJob]
        return [x.lfn for x in possibleFiles[:self.maxFilesPerJob]]


def createPileupDatasets(workflowSpec, wmbs):
    """
    _createPileupDatasets_

    Create PileupTools.PileupDataset instances for each of
    the pileup datasets in the workflowSpec.

    Return a dictionary mapping the payload node name to the
    PileupDataset instance

    """
    result = {}
    puDatasets = workflowSpec.pileupDatasets()
    for puDataset in puDatasets:
        fileset = Fileset(puDataset.name(), wmbs=wmbs).populate()
        pudInstance = PileupDataset(fileset)
        
        if puDataset.has_key("FilesPerJob"):
            pudInstance.maxFilesPerJob = int(puDataset['FilesPerJob'])
        
        result[puDataset['NodeName']] = pudInstance

    return result



      
  




