"""
TODO add dbsAPI call to get the file 
"""
import logging
import time

from DBSAPI.dbsApi import DbsApi
#might need later for error handling
#from DBSAPI.dbsException import *
#from DBSAPI.dbsApiException import *

#from DBSAPI.dbsFileBlock import DbsFileBlock
#from DBSAPI.dbsFile import DbsFile

class DBSHelper(object):
    def __init__(self, dbsUrl):
        self.dbsApi = DbsApi({'url':dbsUrl})
        
    def getBlockInfo(self, blockName):
        """
        _getBlockInfo_ 
        
        Returns DbsFileBlock object from dbs server by a given block name.
         
        "DbsFileBlock" : {
         "Name" : { "Comment" : "Required and UNIQUE", "Validator" : isStringType },
         "StorageElementList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "BlockSize" : { "Comment" : "User may not need to set this variable always", "Validator" : isLongType },
         "NumberOfFiles" : { "Comment" : "Optional, Defaulted to ZERO for new block", "Validator" : isLongType },
         "NumberOfEvents" : { "Comment" : "Optional, Defaulted to ZERO for new block", "Validator" : isLongType },
         "OpenForWriting" : { "Comment" : "Optional, Defaulted to 'y' for new block", "Validator" : isStringType },
         "CreationDate" : { "Comment" : "TimeStamp, object created in database (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
         "CreatedBy" : { "Comment" : "User DN, who created this object (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
         "LastModificationDate" : { "Comment" : "Last Modification, (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
         "CreatedBy" : { "Comment" : "User DN of who last modified this object (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
          },       
        """
        #blockInfoList should contain only one value
        # since one blockName is passed
        blockInfoList = self.dbsApi.listBlocks(block_name=blockName)
        
        if blockInfoList:
            return blockInfoList[0]['StorageElementList']
        else:
            return None
        
    def getBlockLocations(self, blockName):
        """
        TODO: currently it just returns block location list.
        But blockInfo contains other information 
        NumberOfFiles and NumerOfEvent can be used to estimate number of jobs.
        than use getBlockInfo function instead
        """
        blockInfo = self.getBlockInfo(blockName)
        if blockInfo != None:
            return blockInfo['StorageElementList']
        else:
            return None
        
    def getFilesInBlock(self, blockName):
        """
        _getFilesInBlock_
        
        returns list of DbsFile object in the given block
        
        T0DO: convert information for WMBS update
        
        DbsFile" : {
         "Checksum" : { "Comment" : "Required", "Validator" : isStringType },
         "Adler32" : { "Comment" : "Optional", "Validator" : isStringType },
         "Md5" : { "Comment" : "Optional", "Validator" : isStringType },
         "LogicalFileName" : { "Comment" : "REQUIRED and UNIQUE", "Validator" : isStringType },
         "QueryableMetadata" : { "Comment" : "User may not need to set this variable always", "Validator" : isStringType },
         "NumberOfEvents" : { "Comment" : "Required", "Validator" : isLongType },
         "FileSize" : { "Comment" : "Required", "Validator" : isLongType },
         "Status" : { "Comment" : "Required", "Validator" : isStringType },
         "FileType" : { "Comment" : "Required", "Validator" : isStringType },
         "ValidationStatus" : { "Comment" : "User may not need to set this variable always", "Validator" : isStringType },
         "Dataset" : { "Comment" : "User may not need to set this variable always", "Validator" : isDictType },
         "Block" : { "Comment" : "Required", "Validator" : isDictType },
         "LumiList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "LumiExcludedList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "TierList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "AlgoList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "ChildList" : { "Comment" : "List of THIS file's children files", "Validator" : isListType },
         "RunsList" : { "Comment" : "List of THIS file's Runs", "Validator" : isListType },
         "ParentList" : { "Comment" : "User may not need to set this variable always", "Validator" : isListType },
         "BranchList" : { "Comment" : "List of ROOT Branch names", "Validator" : isListType },
         "BranchHash" : { "Comment" : "HASH for ROOT Branch names, Optional", "Validator" : isStringType},
         "AutoCrossSection" : { "Comment" : "User may not need to set this variable always", "Validator" : isFloatType },
         "CreationDate" : { "Comment" : "TimeStamp, object created in database (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
         "CreatedBy" : { "Comment" : "User DN, who created this object (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
         "LastModificationDate" : { "Comment" : "Last Modification, (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
         "CreatedBy" : { "Comment" : "User DN of who last modified this object (AUTO set by DBS, you can over ride, why ?)", "Validator" : isStringType },
          },
        """
        fileInfoList = self.dbsApi.listFiles(blockName=blockName)
        
        return fileInfoList