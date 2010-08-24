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

TODO:   * Make parent relations condition on actually requiring parent relation
             -- c.f. Talk with Simon about fileset name to specify datasets
        * DQ flags from DBS?
        * Finish tracking down DBS problems
"""



import logging

from WMCore.WMBSFeeder.RunTransferNotifier.DbsQueryHelper import DbsQueryHelper
from WMCore.WMBSFeeder.RunTransferNotifier.PhEDExQueryHelper import PhEDExQueryHelper

from WMCore.WMBS.File import File
from WMCore.WMBSFeeder.FeederImpl import FeederImpl

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
                self.datasetCompletion[dataset] = set()
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
        self.phedexHelper = PhEDExQueryHelper(phedexUrl)
        
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
        
        # Do per fileset work, abandon fileset processing on exception
        for fileset in filesets:
            ds = fileset.name
            try:
                # Do per run work
                watchCompleteFiles = []
                
                for watch in self.watchedRuns:
                    # Ensure watcher has dataset listed
                    watch.addDatasetOfInterest(ds)
                    
                    # Query DBS to find all blocks for this run / dataset
                    (files, blocks, fileInfoMap) = \
                        self.dbsHelper.getFileInfo(watch.run, ds)
                        
                    # Now determine all required parent blocks
                    parentBlocks = set()
                    if fileset.requireParents:
                        parentDs = self.dbsHelper.getParentDataset(ds)
                        parentBlocks = self.dbsHelper.getBlockInfo(watch.run,
                                                                       parentDs)
                        
                    # Final list of all required blocks
                    allBlocks = blocks[:]
                    allBlocks.update(parentBlocks)
                        
                    # Find all sites where all blocks are complete
                    sites = self.phedexHelper.getCompleteSites(blocks)
                        
                    # Get sites with newly completed transfers
                    newSites = watch.getNewSites(ds, sites)
                        
                    if len(newSites) > 0:
                        # Add the files for these blocks to the fileset
                        for file in fileInfoMap:
                            fi = fileInfoMap[file]
                                
                            # First add parent file
                            if fileset.requireParents:
                                parentFile = File(lfn=fi["file.parent"])
                                parentFile.save()
                                parentFile.setLocation(newSites)
                                
                            # Add actual file
                            fileToAdd = File(lfn=file, size=fi["file.size"],
                                             events=fi["file.events"],
                                             run=watch.run, 
                                             umi=fi["file.lumi"])
                            if not fileToAdd.exists() and fileset.requireParents:
                                fileToAdd.addParent(fi["file.parent"])
    
                            # Add new locations but don't persist immediately
                            fileToAdd.setLocations(newSites, immediateSave=False)
                                
                            # Add the file to the new file list
                            fileset.addFile(fileToAdd)
                        
                    # Add the site info to the watcher list
                    watchCompleteFiles.append([watch, ds, newSites])
                
                # Commit the fileset
                fileset.commit()
                
                # Add the watched runs
                for a in watchCompleteFiles:
                    a[0].addCompletedNodes(a[1], a[2])
                
            except:
                # Reset the watch list so we re-evaluate next call
                watchCompleteFiles = []

        # Purge old runs
        self.purgeWatchedRuns()
        
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
