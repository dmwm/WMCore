import pickle
import tempfile
import PSetTweaks.WMTweak as WMTweak
from WMCore.Cache.ConfigCache import WMConfigCache

def uploadConfigFile(configFile, configCacheURL):
    f = open(configFile, 'r')
    text = f.read()
    f.close()
    return uploadConfigText(text, configCacheURL)


def uploadConfigText(configText, configCacheURL):
    """ Puts the config into a given ConfigCache, and returns
    a string consisting of the configCache url / cacheId """

    exec(configText)
    if not 'process' in locals().keys():
        raise RuntmieError, 'Cannot find a process in the file'
    process = locals()['process']
    # I wish there were a better interface to ConfigCache than having
    # to make a temp file
    tmpPickleFile = tempfile.NamedTemporaryFile(suffix='.py', mode='w')
    pickle.dump(process.dumpPython(), tmpPickleFile)

    # maybe I'll get this from the ReqMgr someday
    configCache = WMConfigCache('reqmgr', configCacheURL)
    (configDbId, rev) = configCache.addConfig(tmpPickleFile.name)
    tmpPickleFile.close()

    # save the original config text
    tmpConfigTextFile =  tempfile.NamedTemporaryFile(suffix='.py', mode='w')
    tmpConfigTextFile.write(configText)
    (configDbId, rev) = configCache.addOriginalConfig(configDbId, rev, tmpConfigTextFile.name)
    tmpConfigTextFile.close()

    # Make a file to hold the tweaks
    tweak = WMTweak.makeTweak(process)
    tweakFile = tempfile.NamedTemporaryFile(suffix='.py', mode='w')
    tweak.persist(tweakFile.name, format='pickle')
    (configDbId, rev) = configCache.addTweakFile(configDbId, rev, tweakFile.name)
    return configDbId

