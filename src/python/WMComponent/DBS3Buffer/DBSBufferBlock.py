#!/usr/bin/env python
"""
DBSBufferBlock

This is a block object which will be uploaded to DBS
"""

from builtins import object
import time
import logging
import copy

from WMCore import Lexicon
from WMCore.Services.Requests import JSONRequests
from WMCore.WMException import WMException



class DBSBufferBlockException(WMException):
    """
    _DBSBufferBlockException_

    Container class for exceptions for the DBSBufferBlock
    """



class DBSBufferBlock(object):
    """
    _DBSBufferBlock_

    """

    def __init__(self, name, location, datasetpath):
        """
        Just the necessary objects

        Expects:
          name:  The blockname in full
          location: The PNN of the site the block is at
        """


        self.data      = {'dataset_conf_list':    [],   # List of dataset configurations
                          'file_conf_list':       [],   # List of files, the configuration for each
                          'files':                [],   # List of file objects
                          'block':                {},   # Dict of block info
                          'processing_era':       {},   # Dict of processing era info
                          'acquisition_era':      {},   # Dict of acquisition era information
                          'primds':               {},   # Dict of primary dataset info
                          'dataset':              {},   # Dict of processed dataset info
                          'file_parent_list':     [],   # List of file parents
                          'dataset_parent_list':  [],   # List of parent datasets (DBS requires this as list although it only allows one parent)
                          'close_settings':       {}}   # Dict of info about block close settings

        self.files        = []
        self.encoder      = JSONRequests()
        self.status       = 'Open'
        self.inBuff       = False
        self.startTime    = time.time()
        self.name         = name
        self.location     = location
        self.datasetpath  = datasetpath
        self.workflows    = set()

        self.data['block']['block_name']       = name
        self.data['block']['origin_site_name'] = location
        self.data['block']['open_for_writing'] = 1

        self.data['block']['create_by'] = "WMAgent"
        self.data['block']['creation_date'] = int(time.time())
        self.data['block']['block_size'] = 0
        self.data['block']['file_count'] = 0
        self.data['block']['block_events'] = 0

        self.data['close_settings'] = {}
        self.data['close_settings']['block_close_max_wait_time'] = None
        self.data['close_settings']['block_close_max_events'] = None
        self.data['close_settings']['block_close_max_size'] = None
        self.data['close_settings']['block_close_max_files'] = None
        return


    def encode(self):
        """
        _encode_

        Turn this into a JSON object for transmission
        to DBS
        """

        return self.encoder.encode(data = self.data)



    def addFile(self, dbsFile, datasetType, primaryDatasetType):
        """
        _addFile_

        Add a DBSBufferFile object to our block
        """
        if dbsFile['id'] in [x['id'] for x in self.files]:
            msg =  "Duplicate file inserted into DBSBufferBlock: %i\n" % (dbsFile['id'])
            msg += "Ignoring this file for now!\n"
            logging.error(msg)
            logging.debug("Block length: %i", len(self.files))
            l = sorted([x['id'] for x in self.files])
            logging.debug("First file: %s    Last file: %s", l[0], l[-1])
            return

        for setting in self.data['close_settings']:
            if self.data['close_settings'][setting] is None:
                self.data['close_settings'][setting] = dbsFile[setting]

        self.workflows.add(dbsFile['workflow'])

        self.files.append(dbsFile)
        self.data['block']['block_size'] += int(dbsFile['size'])
        self.data['block']['file_count'] += 1
        self.data['block']['block_events'] += int(dbsFile['events'])

        # Assemble information for the file itself
        fileDict = {}
        fileDict['file_type']              =  'EDM'
        fileDict['logical_file_name']      = dbsFile['lfn']
        fileDict['file_size']              = dbsFile['size']
        fileDict['event_count']            = dbsFile['events']
        fileDict['last_modified_by'] = "WMAgent"
        fileDict['last_modification_date'] = int(time.time())
        fileDict['auto_cross_section'] = 0.0

        # Do the checksums
        for cktype in dbsFile['checksums']:
            cksum = dbsFile['checksums'][cktype]
            if cktype.lower() == 'cksum':
                fileDict['check_sum'] = cksum
            elif cktype.lower() == 'adler32':
                fileDict['adler32'] = cksum
            elif cktype.lower() == 'md5':
                fileDict['md5'] = cksum

        # Do the runs
        lumiList = []
        for run in dbsFile.getRuns():
            for lumi in run.lumis:
                dbsLumiDict = {'lumi_section_num': lumi, 'run_num': run.run}
                if run.getEventsByLumi(lumi) is not None:
                    # if events is not None update event for dbs upload
                    dbsLumiDict['event_count'] = run.getEventsByLumi(lumi)
                lumiList.append(dbsLumiDict)
        fileDict['file_lumi_list'] = lumiList

        # Append to the files list
        self.data['files'].append(fileDict)

        # If dataset_parent_list is defined don't add the file parentage.
        # This means it is block from StepChain workflow and parentage of file will be resloved later
        if not self.data['dataset_parent_list']:
            # now add file to data
            parentLFNs = dbsFile.getParentLFNs()
            for lfn in parentLFNs:
                self.addFileParent(child = dbsFile['lfn'], parent = lfn)


        # Do the algo
        algo = self.addConfiguration(release = dbsFile['appVer'],
                                     psetHash = dbsFile['psetHash'],
                                     appName = dbsFile['appName'],
                                     outputLabel = dbsFile['appFam'],
                                     globalTag = dbsFile['globalTag'])

        # Now add the file with the algo
        # Try to avoid messing with pointers here
        fileAlgo = {}
        fileAlgo.update(algo)
        fileAlgo['lfn'] = dbsFile['lfn']
        self.data['file_conf_list'].append(fileAlgo)

        if dbsFile.get('acquisition_era', False):
            self.setAcquisitionEra(dbsFile['acquisition_era'])
        elif dbsFile.get('acquisitionEra', False):
            self.setAcquisitionEra(dbsFile['acquisitionEra'])
        if dbsFile.get('processingVer', False):
            self.setProcessingVer(dbsFile['processingVer'])
        elif dbsFile.get('processing_ver', False):
            self.setProcessingVer(dbsFile['processing_ver'])

        return

    def addFileParent(self, child, parent):
        """
        _addFileParent_

        Add file parents to the data block
        """
        info = {'parent_logical_file_name': parent,
                'logical_file_name': child}
        self.data['file_parent_list'].append(info)

        return

    def addBlockParent(self, parent):
        """
        _addBlockParent_

        Add the parents of the block
        """

        self.data['block_parent_list'].append({'block_name': parent})
        return

    def addDatasetParent(self, parent):
        """
        _addDatasetParent_

        Add the parent datasets to the data block
        """
        self.data['dataset_parent_list'].append(parent)
        return

    def setProcessingVer(self, procVer):
        """
        _setProcessingVer_

        Set the block's processing version.
        """
        pver = procVer or 0
        try:
            pver = int(pver)
        except TypeError:
            msg = "Provided procVer=%s of type %s cannot be converted to int" \
                    % (procVer, type(procVer))
            raise TypeError(msg) from None
        self.data["processing_era"]["processing_version"] = pver
        self.data["processing_era"]["create_by"] = "WMAgent"
        self.data["processing_era"]["description"] = ""
        return

    def setAcquisitionEra(self, era, date = 123456789):
        """
        _setAcquisitionEra_

        Set the acquisition era for the block
        """
        self.data['acquisition_era']['acquisition_era_name'] = era
        self.data['acquisition_era']['start_date']           = date
        return

    def setPhysicsGroup(self, group):
        """
        _setPhysicsGroup_

        Sets the name of the physics group to which the dataset is attached
        """

        self.data['dataset']['physics_group_name'] = group
        return

    def getDatasetPath(self):
        """
        _getDatasetPath_

        Return the datasetpath
        """
        return self.datasetpath

    def getDataset(self):
        """
        _getDataset_

        Return the dataset (None if not set)
        """
        return self.data['dataset'].get('dataset', None)

    def setDataset(self, datasetName, primaryType,
                   datasetType, physicsGroup = None,
                   prep_id  = None, overwrite = False):
        """
        _setDataset_

        Set all the information concerning a single dataset, including
        the primary, processed and tier info
        """
        if self.getDataset() != None and not overwrite:
            # Do nothing, we already have a dataset
            return

        Lexicon.primaryDatasetType(primaryType)

        _, primary, processed, tier = datasetName.split('/')

        # Do the primary dataset
        self.data['primds']['primary_ds_name'] = primary
        self.data['primds']['primary_ds_type'] = primaryType
        self.data['primds']['create_by'] = "WMAgent"
        self.data['primds']['creation_date'] = int(time.time())

        # Do the processed
        self.data['dataset']['physics_group_name']  = physicsGroup
        self.data['dataset']['processed_ds_name']   = processed
        self.data['dataset']['data_tier_name']      = tier
        self.data['dataset']['dataset_access_type'] = datasetType
        self.data['dataset']['dataset']             = datasetName
        self.data['dataset']['prep_id'] = prep_id
        # Add misc meta data.
        self.data['dataset']['create_by'] = "WMAgent"
        self.data['dataset']['last_modified_by'] = "WMAgent"
        self.data['dataset']['creation_date'] = int(time.time())
        self.data['dataset']['last_modification_date'] = int(time.time())
        return


    def addConfiguration(self, release, psetHash,
                         appName = 'cmsRun', outputLabel = 'Merged', globalTag = 'None'):
        """
        _addConfiguration_

        Add the algorithm config to the data block
        """

        algo = {'release_version': release,
                'pset_hash': psetHash,
                'app_name': appName,
                'output_module_label': outputLabel,
                'global_tag': globalTag}

        if not algo in self.data['dataset_conf_list']:
            self.data['dataset_conf_list'].append(algo)


        return algo

    def getNFiles(self):
        """
        _getNFiles_

        Return the number of files in the block
        """

        return len(self.files)


    def getSize(self):
        """
        _getSize_

        Get size of block
        """
        return self.data['block']['block_size']

    def getNumEvents(self):
        """
        _getNumEvents_

        Get the number of events in the block
        """
        return self.data['block']['block_events']

    def getTime(self):
        """
        _getTime_

        Return the time the block has been running
        """

        return time.time() - self.startTime

    def getMaxBlockTime(self):
        """
        _getMaxBlockTime_

        Return the max time that the block should stay open
        """
        return self.data['close_settings']['block_close_max_wait_time']

    def getMaxBlockSize(self):
        """
        _getMaxBlockSize_

        Return the max size allowed for the block
        """
        return self.data['close_settings']['block_close_max_size']

    def getMaxBlockNumEvents(self):
        """
        _getMaxBlockNumEvents_

        Return the max number of events allowed for the block
        """
        return self.data['close_settings']['block_close_max_events']

    def getMaxBlockFiles(self):
        """
        _getMaxBlockFiles_

        Return the max number of files allowed for the block
        """
        return self.data['close_settings']['block_close_max_files']

    def getName(self):
        """
        _getName_

        Get Name
        """

        return self.name

    def getLocation(self):
        """
        _getLocation_

        Get location
        """

        return self.location

    def getStartTime(self):
        """
        _getStartTime_

        Get the time the block was opened at
        """

        return self.startTime


    def FillFromDBSBuffer(self, blockInfo):
        """
        _FillFromDBSBuffer_

        Take the info provided by LoadBlocks and
        use it to create a block object
        """
        # Blocks loaded out of the buffer should
        # have both a creation time, and should
        # be in the buffer (duh)
        self.startTime = blockInfo.get('creation_date')
        self.inBuff    = True

        if 'status' in blockInfo:
            self.status = blockInfo['status']
            if self.status == "Pending":
                self.data['block']['open_for_writing'] = 0

            del blockInfo['status']

        for key in blockInfo:
            self.data['block'][key] = blockInfo.get(key)

    def convertToDBSBlock(self):
        """
        convert to DBSBlock structure to upload to dbs
        TODO: check file lumi event and validate event is not null
        """
        block = {}

        #TODO: instead of using key to remove need to change to keyToKeep
        # Ask dbs team to publish the list (API)
        keyToRemove = ['insertedFiles', 'newFiles', 'file_count', 'block_size',
                       'origin_site_name', 'creation_date', 'open',
                       'Name', 'close_settings']

        nestedKeyToRemove = ['block.block_events', 'block.datasetpath', 'block.workflows']

        dbsBufferToDBSBlockKey = {'block_size': 'BlockSize',
                                  'creation_date': 'CreationDate',
                                  'file_count': 'NumberOfFiles',
                                  'origin_site_name': 'location'}

        # clone the new DBSBlock dict after filtering out the data.
        for key in self.data:
            if key in keyToRemove:
                continue
            elif key in dbsBufferToDBSBlockKey:
                block[dbsBufferToDBSBlockKey[key]] = copy.deepcopy(self.data[key])
            else:
                block[key] = copy.deepcopy(self.data[key])

        # delete nested key dictionary
        for nestedKey in nestedKeyToRemove:
            firstkey, subkey = nestedKey.split('.', 1)
            if firstkey in block and subkey in block[firstkey]:
                del block[firstkey][subkey]

        return block

    def setPendingAndCloseBlock(self):
        "set the block status as Pending for upload as well as closed"
        # Pending means ready to upload
        self.status = "Pending"
        # close block on DBS3 status
        self.data['block']['open_for_writing'] = 0
