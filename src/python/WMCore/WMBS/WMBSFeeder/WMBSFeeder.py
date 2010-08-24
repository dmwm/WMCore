#!/usr/bin/env python

import time
import logging

from Registry import retrieveFeederImpl, RegistryError

from ProdCommon.ThreadTools import WorkQueue
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec

from WMCore.WMBS.Fileset import Fileset
#from WMCore.WMBS.Subscription import Subscription
#from WMCore.WMBS.File import File

from sqlalchemy import create_engine
import sqlalchemy.pool as pool
from sqlalchemy.exceptions import IntegrityError, OperationalError
from WMCore.WMBS.Factory import SQLFactory 


def pollExternal(fileset):
    """
    Call relevant external source and get file details
    """
    try:
        feeder = retrieveFeederImpl(fileset.source, fileset.wmbs)
    except RegistryError:
        msg = "WMBSFeeder plugin \'%s\' unknown" % fileset.name
        logging.error(msg)
        logging.error("aborting poll for...")
        raise RuntimeError, msg
    
    fileset = feeder(fileset)
    
    # do we have any parents we need
    if fileset.parents and fileset.listNewFiles() and \
                                    not fileset.listNewFiles()[0].parents():
        # get parentage from dbs
        try:
            parentFeeder = retrieveFeederImpl('dbs', fileset.wmbs)
        except RegistryError:
            msg = "WMBSFeeder plugin \'%s\' unknown" % 'dbs'
            logging.error(msg)
            logging.error("aborting poll for...")
            raise RuntimeError, msg
        
        fileset = parentFeeder.getParentsForNewFiles(fileset)
    
    return fileset


def importFileset(fileset, DBSinstance, destDBS, tolocalDBS=True, parentageLevel=0):
    """
    import a dataset into local dbs (if required) and wmbs
    """
    
    # dbs feeder needed for parentage info and dbs import
    if parentageLevel or tolocalDBS:
        try:
            dbsfeeder = retrieveFeederImpl('dbs', fileset.wmbs)
        except RegistryError:
            msg = "WMBSFeeder plugin \'%s\' unknown" % fileset.name
            logging.error(msg)
            raise RuntimeError, msg
            
        if tolocalDBS:
            dbsfeeder.importFileset(fileset, DBSinstance, destDBS, parentageLevel)

        if parentageLevel:
            fileset = dbsfeeder.fillFilesetParentage(fileset, DBSinstance, parentageLevel)
    
    fileset.create()
    return fileset


class WMBSFeeder:
    """
    Handle import of files into wmbs
    """
    
    def __init__(self, dbparams, threads=5, **args):
        
        self.ms = None
        #TODO: create connection string properly
        engine = create_engine(dbparams['dbName'], convert_unicode=True,
                                    encoding='utf-8', pool_size=10,
                                    pool_recycle=30)
        factory = SQLFactory(logging)
        self.wmbs = factory.connect(engine)       
        try:
            #TODO: move into client setup script
            self.wmbs.createWMBS()
        except OperationalError:
            pass
        
        self.newFilesets = {}
        # get open filesets that have files to poll for
        self.watchedFilesets = {}
        temp = self.wmbs.listFileSets(only_open=False)
        for fs in temp:
            fileset = Fileset(fs, self.wmbs).populate()
            # Only watch filesets coming from an external source
            if fileset.source is not None:
                self.watchedFilesets[fs] = fileset

        self.workq = WorkQueue.WorkQueue([pollExternal for _ in range(threads)])
        self.importq = WorkQueue.WorkQueue([importFileset for _ in range(threads)])
        self.localDBS = args.get('localDBS', None)
        self.globalDBS = args.get('globalDBS',
            'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')
        self.defaultSource = args.get('defaultSource', 'phedex')
        self.defaultSourceURL = args.get('defaultSourceURL', None)
            #'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet')
    
    
    def setMessager(self, ms):
        """
        set message system, ms is a function that will be called to send
        messages e.g. InputAvailable, ImportFileset etc.
        """
        self.ms = ms
        
    
    def sendMessage(self, type, payload='', delay='00:00:00'):
        """
        function that should be overrideen by implementation to send messages
        """
        
        if self.ms is not None:
            return self.ms(type, payload, delay)
    
        raise NotImplementedError, 'sendMessage not set via setMessager()'
                    
    
    def createWorkflowFilesets(self, workflowSpecFile, localDBS=None):
        """
        Import input datasets into wmbs
        if required import to local scope dbs
        
        Do this inline - do not hand of to worker thread
            ensure datasets are processed in order - parent then child
        
        """
        
        spec = WorkflowSpec()
        try:
            spec.load(workflowSpecFile)
        except Exception, msg:
            logging.error("Cannot read workflow file: " + str(msg))
            raise

        # only work on appropriate workflows
        if spec.parameters['WorkflowType'] != 'Processing':
            logging.info('Ignoring non-processing workflow %s' % \
                                                        workflowSpecFile)
            return

        logging.info("(Re)Importing filesets in %s" % workflowSpecFile)
        
        source = spec.parameters.get('DataSource', self.defaultSource)
        sourceURL = spec.parameters.get('DataSourceURL', self.defaultSourceURL)
        doLocalMigration = bool(spec.parameters.get('MigrateToLocalDBS', True))
        parentageLevel = int(spec.parameters.get('ParentageLevel', 0))
        dbsURL = spec.parameters.get('DBSURL', self.globalDBS)
        onlyClosedBlocks = bool(spec.parameters.get('OnlyClosedBlocks', True))
        isOpen = bool(spec.parameters.get('PollForNewFiles', True))
        
        #    //
        #  //     Insert pileup datasets
        #//
        for dataset in spec.pileupDatasets():
#            if self.known_datasets.has_key(dataset.name()):
#                logging.warning(
#                    "pileup dataset %s already known - skip import") % \
#                                                        dataset.name()
#                continue
            # // assume no new pileup data can be added later 
            #//       is_open=False and skip re-import
            fileset = Fileset(dataset.name(), self.wmbs, is_open=False, 
                              source=source, sourceURL=sourceURL)
            if fileset.exists():
                continue    # no point in reimporting pileup
            if doLocalMigration:    #onlyClosedBlocks
                self.importq.enqueue(workflowSpecFile, fileset, dbsURL, \
                                localDBS, doLocalMigration, parentageLevel)
                continue    # will be saved after import
            fileset.create()
 
        #    //
        #  //     Insert input datasets
        #//
        inputDatasets = []
        for dataset in spec.inputDatasets():
            # need to find out from workflow if open - default true
            fileset = Fileset(dataset.name(), wmbs=self.wmbs, open=isOpen,
                              source=source, sourceURL=sourceURL)
            inputDatasets.append(fileset)
            if doLocalMigration or parentageLevel:
                self.importq.enqueue(workflowSpecFile, fileset, dbsURL, \
                                localDBS, doLocalMigration, parentageLevel)
                continue
            if not fileset.exists():
                fileset.create()
            self.__watchDatasets([fileset])
            
 
        #    //
        #  //     Insert output datasets
        #//
        readyForSub = self.createOutputDatasets(spec, inputDatasets)
        if readyForSub:
            self.createSub(workflowSpecFile)
    
    
    def pollWatched(self):
        """
        Loop over watch datasets and import new files
        """
        # first see if any imports have finished
        if self.importq.resultsQueue:
            workflowDatasetMap = {}
            for wfspecfile, filesets in self.importq:
                workflowDatasetMap.setdefault(wfspecfile, []).extend(filesets)
            for specfile in workflowDatasetMap.keys(): 
                spec = WorkflowSpec()
                try:
                    spec.load(specfile)
                except Exception, msg:
                    logging.error("Cannot read workflow file: " + str(msg))
                    raise
                
                filesets = workflowDatasetMap[specfile]
                # import files at least once
                self.__watchDatasets(filesets, force=True)
                readyForSub = self.createOutputDatasets(spec, filesets)
                if readyForSub:
                    self.createSub(specfile)

        #  //
        # //  Now import new files
        #//
        stopWatching = []
        for name, fileset in self.watchedFilesets.items():
            logging.debug("Will poll %s" % name)
            self.workq.enqueue(name, fileset)
        for key, filesets in self.workq:
            #fileset = self.watchedFilesets[key]
            fileset = filesets[0]
            if fileset.listNewFiles():
                fileset.commit()
                self.inputAvailable(fileset.name)
            if not fileset.open: stopWatching.append(name)
            
            # stop watching closed filesets
            for name in stopWatching:
                logging.info("Fileset closed, stop polling: %s" % name)
                del self.watchedFilesets[name]
    
    
    def __watchDatasets(self, datasets, force=False):
        """
        add open input datasets to list of watched
        """
        for fileset in datasets:
            if not self.watchedFilesets.has_key(fileset.name) or \
                                                (fileset.open or force):
                self.watchedFilesets[fileset.name] = fileset
                logging.info("Now watching %s" % fileset.name)
       

    def createOutputDatasets(self, workflowSpec, inputDatasets):
        """
        Create output datasets for given workflow
        """
        outputFilesetCreated = False
        logging.info("Create output fileset for workflow %s" % workflowSpec.workflowName)
        
        # are all required input datasets imported?
        insets = [Fileset(name, wmbs=self.wmbs) for name in workflowSpec.outputDatasets()]
        for inset in insets:
            if not inset.exists():
                logging.info("Waiting for all input filesets to be imported")
                return outputFilesetCreated
        
        for dataset in workflowSpec.outputDatasets():
            fileset = Fileset(dataset.name(), self.wmbs, is_open=True, 
                              parents=insets)
            outputFilesetCreated = False
            if not fileset.exists():
                fileset.create()
                outputFilesetCreated = True
        
        return outputFilesetCreated



    def inputAvailable(self, fileset):
        return self.sendMessage('InputAvailable', fileset)
    
    def createSub(self, workflowSpec):
        return self.sendMessage('WMBSAccountant:HandleWorkflow', workflowSpec)