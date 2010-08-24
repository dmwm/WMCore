#!/usr/bin/env python
"""
_DatasetInfo_

Serialisable container for information about a dataset

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: DatasetInfo.py,v 1.1 2008/10/08 15:34:15 fvlingen Exp $"
__author__ = "evansde@fnal.gov"


from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery


class DatasetInfo(dict):
    """
    _DatasetInfo_

    Serialisable container for a Dataset including the information
    required to create/locate a dataset in DBS and the details required
    to match the dataset to CMSSW objects

    Inherit from dict, assume all values are strings.
    
    """
    
    def __init__(self):
        dict.__init__(self)
        self.setdefault("PrimaryDataset", None)
        self.setdefault("ProcessedDataset", None)
        self.setdefault("ParentDataset", None)
        
        self.setdefault("ApplicationName",  None)
        self.setdefault("ApplicationProject" , None)
        self.setdefault("ApplicationVersion" , None)
        self.setdefault("ApplicationFamily", None)
        
        self.setdefault("DataTier" , None)
        
        self.setdefault("Conditions" , None)
        self.setdefault("PSetHash", None)

        self.setdefault("InputModuleName" , None)
        self.setdefault("OutputModuleName" , None)
        

    def __str__(self):
        """string rep as XML for printouts"""
        return str(self.save())

    def name(self):
        """
        _name_

        Construct a string giving the name of this dataset as a path,
        using /<PrimaryDataset>/<DataTier>/<ProcessedDataset>

        ParentDataset will be a string of this format
        
        """
        result = "/%s" % self['PrimaryDataset']
        result += "/%s" % self['ProcessedDataset']
        if self['DataTier'] != None:
            result += "/%s" % self['DataTier']
            
        # NOTE: Include AnalysisDataset in this object in future???

        return result
        

    def save(self):
        """
        _save_

        Return an improvNode structure containing details
        of this object so it can be saved to a file

        """
        improvNode = IMProvNode(self.__class__.__name__)
        for key, val in self.items():
            if val == None:
                continue
            node = IMProvNode("Entry", str(val), Name = key)
            improvNode.addNode(node)
        return improvNode


    def load(self, improvNode):
        """
        _load_

        Populate this instance with data extracted from the improvNode
        provided. The Argument should be an improvNode created with the
        same structure as the result of the save method of this class

        """
        if improvNode.name != self.__class__.__name__:
            #  //
            # // Not the right node type
            #//
            return

        entryQ = IMProvQuery("/%s/Entry" % self.__class__.__name__)
        entries = entryQ(improvNode)
        for entry in entries:
            key = str(entry.attrs["Name"])
            value = str(entry.chardata)
            self[key] = value
        return
    
        
        
        



    

        
