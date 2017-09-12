"""
_CreateWMBS_

Implementation of CreateWMBS for T0AST.

Inherit from Oracle.CreateWMBS, and add T0AST specific changes.
"""

from WMCore.WMBS.Oracle.Create import Create as OracleCreate


class CreateWMBS(OracleCreate):
    """
    Class to set up the WMBS schema in the T0AST database
    """

    def __init__(self, logger, dbInterface):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        OracleCreate.__init__(self, logger, dbInterface)

        self.create["02wmbs_file_details"] = None  # This would be a view

        self.create["05wmbs_file_runlumi_map"] = None  # This would be a view

        for subType in ("Processing", "Merge", "Job"):
            subTypeQuery = "INSERT INTO wmbs_subs_type (name) values ('%s')" % subType
            self.inserts["wmbs_subs_type_%s" % subType] = subTypeQuery
