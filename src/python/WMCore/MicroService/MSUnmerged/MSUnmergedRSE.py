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

        # NOTE: totalNumFiles reflects the total number of files eligible for deletion
        #       once the relevant protected files have been filtered out, rather
        #       than the total number of files found at the RSE
        myDoc = {
            "name": rseName,
            "delInterface": "",
            "isClean": False,
            "counters": {"totalNumFiles": 0,
                         "deletedSuccess": 0,
                         "deletedFail": 0},
            "files": {"allUnmerged": [],
                      "toDelete": [],
                      "protected": [],
                      "deletedSuccess": [],
                      "deletedFail": []},
            "dirs":  {"allUnmerged": [],
                      "protected": [],
                      "nonEmpty": [],
                      "empty": []}}
        self.update(myDoc)
