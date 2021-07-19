"""
File       : MSUnmergedRSE.py
Description: Provides a document Template for the MSUnmerged MicroServices
"""


class MSUnmergedRSE(dict):
    """
    A minimal RSE information representation to serve the needs
    of the MSUnmerged Micro Service.
    """
    def __init__(self, rseName, **kwargs):
        super(MSUnmergedRSE, self).__init__(**kwargs)

        # NOTE: totalNumFiles reflects the total number of files at the RSE as
        #       fetched from the Rucio Consistency Monitor. Once the relevant
        #       protected paths have been filtered out and the path been cut to the
        #       proper depth (as provided by the WMStats Protected LFNs interface),
        #       then the final number (but on a directory level rather than on
        #       files granularity level) will be put in the counter 'toDelete'

        # NOTE: The type of msUnmergedRSE['files']['toDelete'] is a dictionary of
        #       of generators holding the filters for the files to be deleted e.g.:
        #       msUnmergedRSE['files']['toDelete'] = {
        #          '/store/unmerged/Run2018B/TOTEM42/MINIAOD/22Feb2019-v1': <filter at 0x7f3699d93208>,
        #          '/store/unmerged/Run2018B/TOTEM21/AOD/22Feb2019-v1': <filter at 0x7f3699d93128>,
        #          '/store/unmerged/Run2018D/MuonEG/RAW-RECO/TopMuEG-12Nov2019_UL2018-v1': <filter at 0x7f3699d93668>}
        self.allUnmerged = []
        myDoc = {
            "name": rseName,
            "pfnPrefix": None,
            "isClean": False,
            "counters": {"totalNumFiles": 0,
                         "dirsToDeleteAll": 0,
                         "dirsToDelete": 0,
                         "filesToDelete": 0,
                         "deletedSuccess": 0,
                         "deletedFail": 0},
            "files": {"allUnmerged": [],
                      "toDelete": {},
                      "protected": {},
                      "deletedSuccess": [],
                      "deletedFail": []},
            "dirs": {"allUnmerged": set(),
                     "toDelete": set(),
                     "protected": set(),
                     "empty": []}}
        self.update(myDoc)
