from sets import Set
from WMCore.DataStructs.Pickleable import Pickleable 
class File(Pickleable):
    """
    _File_
    Data object that contains details for a single file
    """
    def __init__(self, lfn='', size=0, events=0, run=0, lumi=0, parents=Set()):
        self.dict = {}
        self.dict["lfn"] = lfn
        self.dict["size"] = size
        self.dict["events"] = events
        self.dict["run"] = run
        self.dict["lumi"] = lumi
        self.dict["parents"] = parents