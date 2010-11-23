#!/usr/bin/env python
"""
_ListThresholdsForSubmit_

Query WMBS and ResourceControl to determine how many jobs are still running so
that we can schedule jobs that have just been created.
"""




from WMCore.ResourceControl.MySQL.ThresholdBySite import ThresholdBySite as MySQLThresholdBySite

class ThresholdBySite(MySQLThresholdBySite):
    """
    Oracle version

    """
