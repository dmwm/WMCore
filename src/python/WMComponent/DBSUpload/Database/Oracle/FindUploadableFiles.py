#!/usr/bin/env python
"""
_DBSUpload.FindUploadableFiles_

Find the files in a datasets that needs to be uploaded to DBS

"""
__revision__ = "$Id: FindUploadableFiles.py,v 1.1 2009/06/04 21:50:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.FindUploadableFiles import FindUploadableFiles as MySQLFindUploadableFiles

class FindUploadableFiles(MySQLFindUploadableFiles):
    
    """
Oracle implementation to find files in datasets that need to be uploaded to DBS
    """
    
    sql = """SELECT dbsfile.id as ID FROM dbsbuffer_file dbsfile where dbsfile.dataset=:dataset and dbsfile.status =:status AND ROWNUM < :maxfiles""" 


    def makeFile(self, results):
        ret=[]
        for r in results:
                entry={}
                entry['ID']=r['id']
                ret.append(entry)
        return ret
