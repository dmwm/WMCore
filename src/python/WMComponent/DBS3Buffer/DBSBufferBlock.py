#!/usr/bin/env python


"""
DBSBlock

This is a block object which will be uploaded to DBS
"""

import time
import logging

from WMCore.Services.Requests import JSONRequests

from WMCore.WMException import WMException



class DBSBlockException(WMException):
    """
    _DBSBlockException_

    Container class for exceptions for the DBSBlock
    """



class DBSBlock:
    """
    DBSBlock

    Class for holding all the necessary equipment for a DBSBlock
    """

    def __init__(self, name, location, das):
        """
        Just the necessary objects

        Expects:
          name:  The blockname in full
          location: The SE-name of the site the block is at
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
                          'close_settings':       {}}   # Dict of info about block close settings

        self.files     = []
        self.encoder   = JSONRequests()
        self.status    = 'Open'
        self.inBuff    = False
        self.startTime = time.time()
        self.name      = name
        self.location  = location
        self.das       = das

        self.data['block']['block_name']       = name
        self.data['block']['origin_site_name'] = location
        self.data['block']['open_for_writing'] = 0

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



    def addFile(self, dbsFile):
        """
        _addFile_

        Add a DBSBufferFile object to our block
        """
        if dbsFile['id'] in [x['id'] for x in self.files]:
            msg =  "Duplicate file inserted into DBSBlock: %i\n" % (dbsFile['id'])
            msg += "Ignoring this file for now!\n"
            logging.error(msg)
            logging.debug("Block length: %i" % len(self.files))
            l = [x['id'] for x in self.files]
            l.sort()
            logging.debug("First file: %s    Last file: %s" % (l[0], l[-1]))
            return

        for setting in self.data['close_settings']:
            if self.data['close_settings'][setting] is None:
                self.data['close_settings'][setting] = dbsFile[setting]

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
        fileDict['adler32'] = "NOTSET"
        fileDict['md5'] = "NOTSET"
        fileDict['last_modified_by'] = "WMAgent"
        fileDict['last_modification_date'] = int(time.time())
        fileDict['auto_cross_section'] = 0.0
        
        # Do the checksums
        for cktype in dbsFile['checksums'].keys():
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
                lumiList.append({'lumi_section_num': lumi, 'run_num': run.run})
        fileDict['file_lumi_list'] = lumiList

        # Append to the files list
        self.data['files'].append(fileDict)

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

        # Take care of the dataset
        self.setDataset(datasetName  = dbsFile['datasetPath'],
                        primaryType  = dbsFile.get('primaryType', 'DATA'),
                        datasetType  = dbsFile.get('datasetType', 'PRODUCTION'),
                        physicsGroup = dbsFile.get('physicsGroup', None))

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

        self.data['ds_parent_list'].append({'parent_dataset': parent})
        return

    def setProcessingVer(self, procVer):
        """
        _setProcessingVer_

        Set the block's processing version.
        """
        if procVer.count("-") == 1:
            (junk, self.data["processing_era"]["processing_version"]) = procVer.split("-v")
        else:
            self.data["processing_era"]["processing_version"] = procVer

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

    def hasDataset(self):
        """
        _hasDataset_

        Check and see if the dataset has been properly set
        """
        return self.data['dataset'].get('dataset', False)

    def setDataset(self, datasetName, primaryType,
                   datasetType, physicsGroup = None, overwrite = False, valid = 1):
        """
        _setDataset_

        Set all the information concerning a single dataset, including
        the primary, processed and tier info
        """
        if self.hasDataset() and not overwrite:
            # Do nothing, we already have a dataset
            return

        if not primaryType in ['MC', 'DATA', 'TEST']:
            msg = "Invalid primaryDatasetType %s\n" % primaryType
            logging.error(msg)
            raise DBSBlockException(msg)

        if not datasetType in ['VALID', 'PRODUCTION', 'INVALID', 'DEPRECATED', 'DELETED']:
            msg = "Invalid processedDatasetType %s\n" % datasetType
            logging.error(msg)
            raise DBSBlockException(msg)

        try:
            if datasetName[0] == '/':
                junk, primary, processed, tier = datasetName.split('/')
            else:
                primary, processed, tier = datasetName.split('/')
        except Exception, ex:
            msg = "Invalid dataset name %s" % datasetName
            logging.error(msg)
            raise DBSBlockException(msg)

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

        if 'status' in blockInfo.keys():
            self.status = blockInfo['status']
            del blockInfo['status']
            
        for key in blockInfo.keys():
            if key == "DatasetAlgo":
                continue
            self.data['block'][key] = blockInfo.get(key)
