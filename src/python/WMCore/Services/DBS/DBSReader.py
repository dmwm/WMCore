#!/usr/bin/env python
"""
_DBSReader_

Readonly DBS Interface

"""
import urlparse

from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.Services.EmulatorSwitch import emulatorHook

# make assumption that same host won't be used for both
# this check should catch most deployed servers
DBS2HOST = 'cmsdbsprod.cern.ch'
DBS3HOST = 'cmsweb.cern.ch'

@emulatorHook
def DBSReader(endpoint, **kwargs):
    """Function to find and instantiate desired DBSReader object"""
    endpoint_components = urlparse.urlparse(endpoint)

    if endpoint_components.hostname == DBS3HOST:
        return _getDBS3Reader(endpoint, **kwargs)
    elif endpoint_components.hostname == DBS2HOST:
        return _getDBS2Reader(endpoint, **kwargs)

    # try with a dbs2 instance, if that fails try dbs3
    try:
        dbs = _getDBS2Reader(endpoint, **kwargs)
        # if this doesn't throw endpoint is dbs2
        dbs.dbs.getServerInfo()
        return dbs
    except Exception, ex:
        msg += 'Instantiating DBS2Reader failed with %s\n' % str(ex)

    msg = ''
    try:
        dbs = _getDBS3Reader(endpoint, **kwargs)
        # if this doesn't throw endpoint is dbs3
        dbs.dbs.serverinfo()
        return dbs
    except Exception, ex:
        msg += 'Instantiating DBS3Reader failed with %s\n' % str(ex)
    raise DBSReaderError("Can't contact DBS at %s, got errors %s" % (endpoint, msg))

def _getDBS2Reader(endpoint, **kwargs):
    from WMCore.Services.DBS.DBS2Reader import DBS2Reader as DBSReader
    return DBSReader(endpoint, **kwargs)

def _getDBS3Reader(endpoint, **kwargs):
    from WMCore.Services.DBS.DBS3Reader import DBS3Reader as DBSReader
    return DBSReader(endpoint, **kwargs)

__all__ = [DBSReader]
