#!/usr/bin/env python
"""
_DBSUpload.FindUploadableDatasets_

Find the datasets that have files that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableDatasets.py,v 1.1 2009/06/04 21:50:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableDatasets import FindUploadableDatasets as MySQLFindUploadableDatasets

class FindUploadableDatasets(MySQLFindUploadableDatasets):
    
    """
Oracle implementation for finding datasets that have files that need to be uploaded into DBS
    """


    def makeDS(self, results):
        ret=[]
        for r in results:
            entry={}
            entry['ID']       = r['id']
            entry['Path']     = r['path']
            if not r['algo']  == None:
                entry['Algo'] = int(r['algo'])
            else:
                entry['Algo'] = None
            if not r['algoindbs']  == None:
                entry['AlgoInDBS'] = int(r['algoindbs'])
            else:
                entry['AlgoInDBS'] = None
            ret.append(entry)
        return ret
