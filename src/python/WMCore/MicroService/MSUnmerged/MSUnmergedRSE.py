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

        self.allUnmerged = []
        myDoc = {
            "name": rseName,
            "delInterface": "",
            "isClean": False,
            "counters": {"totalNumFiles": 0,
                         "toDelete": 0,
                         "deletedSuccess": 0,
                         "deletedFail": 0},
            "files": {"allUnmerged": set(),
                      "toDelete": set(),
                      "protected": set(),
                      "deletedSuccess": [],
                      "deletedFail": []},
            "dirs": {"allUnmerged": [],
                     "protected": [],
                     "nonEmpty": [],
                     "empty": []}}
        self.update(myDoc)
