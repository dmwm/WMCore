#!/usr/bin/env python
import logging

try:
    import gfal2
except ImportError:
    # in case we do not have gfal2 installed
    print("FAILED to import gfal2. Use it only in emulateGfal2=True mode!!!")
    gfal2 = None

def createGfal2Context(logLevel="normal", emulate=False):
    """
    Create a gfal2 context object
    :param logLevel: string with the gfal2 log level
    :param emulate: boolean to be used by unit tests
    :return: the gfal2 context object
    """
    if emulate:
        return None
    ctx = gfal2.creat_context()
    gfal2.set_verbose(gfal2.verbose_level.names[logLevel])
    return ctx

def testGFAL(ctx):
    logger = logging.getLogger()
    rseDirs = ["/store/unmerged/Run3Summer22EENanoAODv11/Wto2Q-3Jets_HT-200to400_TuneCP5_13p6TeV_madgraphMLM-pythia8/NANOAODSIM/126X_mcRun3_2022_realistic_postEE_v1-v3",
                "/store/unmerged/RunIISummer20UL18NanoAODv9/GluGluHoffshell_HToWWToENuTauNu_TuneCP5_13TeV_MCFM701-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2"]

    for dirPfn in rseDirs:
        try:
            # NOTE: For gfal2 rmdir() exit status of 0 is success
            rmdirSuccess = ctx.rmdir(dirPfn) == 0
        except gfal2.GError as gfalExc:
            logger.warning("MISSING directory: %s, gfal code=%s", dirPfn, gfalExc.code)

def main():
    ctx = createGfal2Context()
    testGFAL(ctx)
    print("succeeded")

if __name__ == '__main__':
    main()