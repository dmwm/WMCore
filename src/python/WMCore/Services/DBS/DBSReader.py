#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""

from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.DBS.DBS3Reader import DBS3Reader


def DBSReader(endpoint, **kwargs):
    """Function to find and instantiate desired DBSReader object"""

    try:
        dbs = DBS3Reader(endpoint, **kwargs)
        # if this doesn't throw endpoint is dbs3
        dbs.dbs.serverinfo()
        return dbs
    except Exception as ex:
        msg = 'Instantiating DBS3Reader failed with %s\n' % str(ex)
        raise DBSReaderError("Can't contact DBS at %s, got errors %s" % (endpoint, msg))
