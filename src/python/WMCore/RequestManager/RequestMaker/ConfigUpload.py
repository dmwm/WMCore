import sys
import tempfile
import os
import PSetTweaks.WMTweak as WMTweak
from WMCore.Cache.WMConfigCache import ConfigCache


def uploadConfigFile(couchDB, configCacheURL, configFile):
    # maybe I'll get this from the ReqMgr someday
    #configCache = WMConfigCache(couchDB, configCacheURL)
    configCache = WMConfigCache(couchDB, configCacheURL)
    #(configDbId, rev) = configCache.addConfig(configFile)
    configDbId, rev = configCache.addConfig(configFile)

    # Make a file to hold the tweaks
    #configCache.addTweakFile(configDbId, rev, configFile)
    f = open(configFile, 'r')
    configText = f.read()
    f.close()
    exec(configText)
    if not 'process' in locals().keys():
        raise RuntmieError, 'Cannot find a process in the file'
    process = locals()['process']

    tweak = WMTweak.makeTweak(process)
    tweakDict = tweak.jsondictionary()
    (tmpobj, tmpname) = tempfile.mkstemp()
    tweakFile = tweak.persist(tmpname, format="json")
    (configDbId, rev) = configCache.addTweakFile(configDbId, rev, tmpname, tweakDict)
    os.close(tmpobj)
    os.remove(tmpname)
    return configDbId, rev

if __name__ == "__main__":
    if (len(sys.argv) != 4):
        print "Usage: %s couchUrl database_name input_file" % sys.argv[0]
        sys.exit(1)

    couchUrl  = sys.argv[1]
    couchDB   = sys.argv[2]
    inputFile = sys.argv[3]

    docid, revision = uploadConfigFile(couchDB, couchUrl, inputFile)
    print "Added file to config cache"
    print "DocID:    %s" % docid
    print "Revision: %s" % revision

