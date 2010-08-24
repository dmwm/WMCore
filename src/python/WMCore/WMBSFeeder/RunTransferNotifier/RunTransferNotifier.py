#!/usr/bin/env python
"""
_RunTransferNotifier_

Health warning: Unfinished code (trains are great, working on Saturday isn't)

WMBS Feeder which looks for new runs, and inserts files to filesets when all
files for a given run are transferred to a site. Also handles Parent files
when required (waits for children + parents for a run to be transferred).

Algorithm:

1. Look for new runs in DBS. If new runs found, add to WatchedRuns list
2. For every fileset, ensure a dataset entry is present in each WatchedRun to
    monitor location transfers. This ensures new registered filesets will take
    advantage of currently watched runs
3. For every fileset, query DBS against all WatchedRuns to find all blocks
    associated to the run and dataset
3.1    If required, find all associated parent blocks and add to block list
3.2    For every block, query PhEDEx to find sites where block is complete
3.3    When all blocks for a dataset are present at a site, check node is not
        already in WatchedRuns dataset:location mapping
3.4    If no mapping, add node to the WatchedRuns dataset:location mapping and
        fill fileset with primary files
4. Purge WatchedRuns older than n hours

TODO:   * Handling of file locations - DataStructs.File or WMBS.File?
        * DBS and PhEDEx queries
        * Handling of parent files - registering with child file
        * Adding new locations as they become available? What about file acquire
          status?
        * Should fileset flag whether parents are required? Considering we
          determine what files to get from the fileset name, this should live
          with the fileset I think, otherwise becomes hacky use of fileset.name
          e.g. fileset.name = "/Cosmics/MyEra-v1/RECO requireParent=True"
        * Cache DBS block / file lookups? Depends on how / when blocks are migrated
          to global DBS. getEvents could use cached response from getDbsBlockFiles
          for example.
"""
__revision__ = "$Id: RunTransferNotifier.py,v 1.1 2008/10/25 16:28:35 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from sets import Set

from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser

from urllib import urlopen
from urllib import quote

from time import time

class PhEDExNotifierComponent(FeederImpl):
    """
    _PhEDExNotifierComponent_
    
    Run / transfer feeder implementation
    """
    class WatchedRun:
        """
        _WatchedRun_
        
        Records a run that is being watched, and maintains state about
        transfered files for monitored datasets
        """
        def __init__(self, run):
            self.run = run
            self.creationTime = int(time())
            self.accessTime = int(time())
            self.datasetCompletion = {}
            
        def addDatasetOfInterest(self, dataset):
            """
            Adds an entry to Dataset : CompleteLocation tracking
            """
            if not self.datasetCompletion.has_key(dataset):
                self.datasetCompletion[dataset] = Set()
                self.accessTime = int(time())
        
        def addCompletedNodes(self, dataset, nodes):
            """
            Records completed transfer for a dataset to a node
            """
            if isinstance(node, list):
                self.datasetCompletion[dataset].extend(nodes)
            else:
                self.datasetCompletion[dataset].add(nodes)
            self.accessTime = int(time())
        
        def getNewSites(self, dataset, sitesWithRun):
            """
            Returns all sites that have not been marked as complete
            """
            return sitesWithRun - self.datasetCompletion[dataset]
            
    def __init__( self, startRun = None, purgeTime = 48,
                  phedexUrl = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas",
                  dbsUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet" ):
        """
        Configure the feeder
        """
        # Runs that are being watched
        self.watchedRuns = []
        
        # The last run that was identified as new, and run purge time
        self.lastRun = 0
        self.purgeTime = purgeTime * 3600 # Convert to seconds
        
        # Configure DBS API
        try:
            self.dbsapi = DbsApi( {'url': dbsUrl } )
        except DbsApiException, ex:
            print "Caught API Exception %s: %s "  % (ex.getClassName(),
                                                     ex.getErrorMessage() )
            if ex.getErrorCode() not in (None, ""):
                print "DBS Exception Error Code: ", ex.getErrorCode()
        
        # Bootstrap run list
        if not startRun:
            # Determine the last run registered in DBS
            runs = self.dbsapi.listRuns()
            runs.sort(reverse=True)
            if len(runs) == 0:
                msg = "Could not bootstrap RunTransferNotifier feeder"
                raise RuntimeError, msg
            
            # There are runs, ensure we ignore them in first query
            self.lastRun = runs[0]
        else:
            # Ensure we include the startRun in first query
            self.lastRun = startRun - 1
    
    def __call__(self, filesets):
        """
        The algorithm itself
        """
        # Update run list
        self.getNewRuns()
        
        # Do per fileset work
        filesetList = self.makelist(filesets)
        for fileset in filesetList:
            dsName = fileset.name
            
            # Do per watcher work
            for watch in self.watchedRuns:
                # Ensure watcher has dataset listed
                watch.addDatasetOfInterest(dsName)
                
                # Do per dataset work
                for ds in watch.datasetCompletion:
                    # Query DBS to find all blocks for this run / dataset
                    blocks = self.getDbsBlockInfo(watch.run, ds)
                    
                    # Now determine all required parent blocks
                    parentBlocks = Set()
                    if fileset.requireParents:
                        primaryFiles = self.getDbsBlockFiles(blocks)
                        parentBlocks = self.getDbsParentBlocks(primaryFiles)
                    
                    # Final list of all required blocks
                    allBlocks = blocks[:]
                    allBlocks.extend(parentBlocks)
                    
                    # Find all sites where all blocks are complete
                    sites = self.getCompleteSites(blocks)
                    
                    # Get sites with newly completed transfers
                    newSites = watch.getNewSites(ds, sites)
                    
                    if len(newSites) > 0:
                        # Add the files for these blocks to the fileset
                        for block in blocks:
                            files = self.getPhEDExBlockFiles(block)
                            for file in files:
                                lfn = file['name']
                                events = self.getEvents(lfn)
                                (runs,lumis) = self.getRunLumi(lfn)
                                fileToAdd = File(lfn=lfn, size=file['bytes'],
                                               events=events, run=runs[0],
                                               lumi=lumis[0])
                                # TODO: File parentage (if required)
                                # WMBS.File doesn't take location argument...
                                # What to do here?
                                replicas = file['replica']
                                if len(replicas) > 0:
                                    locations = []
                                    for replica in replicas:
                                        locations.append(replica['node'])
                                    fileToAdd.setLocation(locations)
                                    fileset.addFile(fileToAdd)
                    
                    # Add the site info to the watcher
                    watch.addCompletedNode(ds, newSites)
        
        # Purge old runs
        self.purgeWatchedRuns()
    
    def getDbsParentBlocks(self, files):
        """
        Queries DBS to get the block for the parent of each file (if present)
        Note requires lots of queries: DBS can't give parent block information
        as it is braindead.
        """
        parentBlocks = Set()
        for f in files:
            for p in f['Parents']:
                parFile = self.dbsapi.listFiles(patternLFN=p)
                if len (parFile) != 1:
                    print "%s doesn't map to single file in DBS" % f['Parent']
                parentBlocks.add(parFile[0]['Block'])
        return parentBlocks
        
    def getDbsBlockFiles(self, blocks):
        """
        Queries DBS to get all files in a block
        """
        files = []
        if isinstance(blocks, list):
            for block in blocks:
                files.extend(self.dbsapi.listFiles(block=block,
                                          retriveList=['retrive_parent']))
        else:
            files = self.dbsapi.listFiles(block=blocks,
                                          retriveList=['retrive_parent'])
        return files

    def getPhEDExBlockFiles(self, block):
        """
        Queries PhEDEx to get all files in a block
        """
        connection = urlopen(self.nodeURL + "&block=%s" % quote(block))        
        aString = connection.read()
        connection.close()

        if aString[2:8] != "phedex":
            print "RunTransferNotifier: bad string from server follows."
            print "%s" % aString

        phedex = eval(aString.replace( "null", "None" ), {}, {})
        
        blocks = phedex['phedex']['block']
        if len(blocks) != 1:
            print "PhEDExNotifier: Found %d blocks, expected 1, will only consider first block" % len(blocks)

        return blocks[0]['file']
    
    def getCompleteSites(self, blocks):
        """
        Queries PhEDEx to determine sites where all listed blocks are present
        """
        pass
    
    def getDbsBlockInfo(self, run, dataset):
        """
        Queries DBS to get all file blocks for a given run and dataset
        """
        pass

    def getEvents( self, lfn ):
        """
        Query DBS to determine how many events are in a given file
        """
        try:
            # Get the file object, I hope there is only one!
            files = self.dbsapi.listFiles(patternLFN = lfn)
            if len ( files ) != 1:
                print "LFN doesn't map to single file in DBS! lfn=%s" % lfn
            return files[0][ 'NumberOfEvents' ]

        except DbsDatabaseError,e:
            print e
        
    def getNewRuns(self):
        """
        Queries DBS to determine what new runs are present, and adds a watcher
        """
        runs = self.dbsapi.listRuns("run > %s % self.lastRun")
        runs.sort()
        for run in runs:
            watchedRuns.add(WatchedRun(run))
            self.lastRun = run
    
    def purgeWatchedRuns(self):
        """
        Purges watched runs that were last accessed longer than purgeTime ago
        """
        validRuns = []
        for run in self.watchedRuns:
            if int(time) - run.accessTime < self.purgeTime:
                validRuns.add(run)
        self.watchedRuns = validRuns
