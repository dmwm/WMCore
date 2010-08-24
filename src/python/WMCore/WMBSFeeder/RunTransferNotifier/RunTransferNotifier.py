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
        * DBS Event query
        * DQ flags from DBS?
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
        * Exception handling (especially as propagated from DBS query errors)
"""
__revision__ = "$Id: RunTransferNotifier.py,v 1.7 2008/10/28 12:05:33 jacksonj Exp $"
__version__ = "$Revision: 1.7 $"

import logging

from sets import Set

from WMCore.WMBSFeeder.RunTransferNotifier.DbsQueryHelper import DbsQueryHelper

from WMCore.DataStructs.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

from urllib import urlopen
from urllib import quote

from time import time

class RunTransferNotifier(FeederImpl):
    """
    Run / transfer feeder implementation
    """
    class WatchedRun:
        """
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
                self.datasetCompletion[dataset].append(nodes)
            self.accessTime = int(time())
        
        def getNewSites(self, dataset, sitesWithRun):
            """
            Returns all sites that have not been marked as complete
            """
            return sitesWithRun - self.datasetCompletion[dataset]
            
    def __init__(self, startRun = None, purgeTime = 48,
                 phedexUrl = "http://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas",
                 dbsHost = "cmsweb.cern.ch/dbs_discovery/",
                 dbsInstance = "cms_dbs_prod_global",
                 dbsPort = 443):
        """
        Configure the feeder
        """
        # Configure data service look up components
        self.dbsHelper = DbsQueryHelper(dbsHost, dbsPort, dbsInstance)
        
        # Runs that are being watched
        self.watchedRuns = []
        
        # The last run that was identified as new, and run purge time
        self.lastRun = 0
        self.purgeTime = purgeTime * 3600 # Convert hours to seconds
        
        # Bootstrap run list
        if not startRun:
            # Determine the last run registered in DBS
            runs = self.dbsHelper.getRuns()
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
        for fileset in filesets:
            dsName = fileset.name
            
            # Do per watcher work
            for watch in self.watchedRuns:
                # Ensure watcher has dataset listed
                watch.addDatasetOfInterest(dsName)
                
                # Do per dataset work
                for ds in watch.datasetCompletion:
                    # Query DBS to find all blocks for this run / dataset
                    blocks = self.dbsHelper.getBlockInfo(watch.run, ds)
                    
                    # Now determine all required parent blocks
                    # This is a prime candidate for caching as requires
                    # many per file lookups in DBS
                    parentBlocks = Set()
                    if fileset.requireParents:
                        primaryFiles = self.dbsHelper.getBlockFiles(blocks)
                        parentFiles = self.dbsHelper.getParentFiles(primaryFiles)
                        parentBlocks = self.dbsHelper.getFileBlocks(parentFiles)
                    
                    # Final list of all required blocks
                    allBlocks = blocks[:]
                    allBlocks.update(parentBlocks)
                    
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

    def getPhEDExBlockFiles(self, block):
        """
        Queries PhEDEx to get all files in a block
        """
        connection = urlopen(self.phedexUrl + "&block=%s" % quote(block))        
        aString = connection.read()
        connection.close()

        if aString[2:8] != "phedex":
            print "RunTransferNotifier: bad string from server follows."
            print "%s" % aString

        phedex = eval(aString.replace( "null", "None" ), {}, {})
        
        blocks = phedex['phedex']['block']
        if len(blocks) != 1:
            print "PhEDExNotifier: Found %d blocks, will only use first" % \
                len(blocks)

        return blocks[0]['file']
    
    def getCompleteSites(self, blocks):
        """
        Queries PhEDEx to determine sites where all listed blocks are present
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
        runs = self.dbsHelper.getRuns(self.lastRun)
        runs.sort()
        for run in runs:
            watchedRuns.append(WatchedRun(run))
            self.lastRun = run
    
    def purgeWatchedRuns(self):
        """
        Purges watched runs that were last accessed longer than purgeTime ago
        """
        validRuns = []
        for run in self.watchedRuns:
            if int(time) - run.accessTime < self.purgeTime:
                validRuns.append(run)
        self.watchedRuns = validRuns
