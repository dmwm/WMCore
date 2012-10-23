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
                          'file_conf_list':       [],   # List of files, with the configuration for each
                          'files':                [],   # List of file objects
                          'block':                {},   # Dict of block info
                          'block_parent_list':    [],   # List of block parents
                          'processing_era':       {},   # Dict of processing era info
                          'ds_parent_list':       [],   # List of parent datasets
                          'acquisition_era':      {},   # Dict of acquisition era information
                          'primds':               {},   # Dict of primary dataset info
                          'dataset':              {},   # Dict of processed dataset info
                          'physics_group_name':   {},   # Physics Group Name
                          'file_parent_list':     []}   # List of file parents

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
        self.data['block']['open_for_writing'] = 0  # If we're sending a block, it better be open

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

        self.files.append(dbsFile)
        # Assemble information for the file itself
        fileDict = {}
        fileDict['file_type']              =  'EDM'
        fileDict['logical_file_name']      = dbsFile['lfn']
        fileDict['file_size']              = dbsFile['size']
        fileDict['event_count']            = dbsFile['events']
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

    def setProcessingVer(self, era):
        """
        _setProcessingEra_

        Set the block's processing era
        """

        self.data['processing_era']['processing_version'] = era
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


        # Do the processed
        self.data['dataset']['physics_group_name']  = physicsGroup
        self.data['dataset']['processed_ds_name']   = processed
        self.data['dataset']['data_tier_name']      = tier
        self.data['dataset']['dataset_access_type'] = datasetType
        self.data['dataset']['dataset']             = datasetName

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

        size = 0

        for x in self.files:
            size += x.get('size', 0)

        return size

    def getTime(self):
        """
        _getTime_

        Return the time the block has been running
        """

        return time.time() - self.startTime


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

        for key in blockInfo.keys():
            self.data['block'][key] = blockInfo.get(key)
