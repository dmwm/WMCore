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
    rseDirs = ["davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/1",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/2",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/3",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/4",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/5",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/6",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/7",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/8",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/9",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/10",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/11",
               "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/12"]
    #rseDirs = ["davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/10/1/amaltaro_SC_MultiPU_Oct2021_Val_211001_104516_5285",
    #           "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/10/1/cmsunified_ACDC0_task_BPH-RunIISummer20UL17MiniAODv2-00124__v1_T_211001_030122_7466"]
    pfns = ["davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/10/1/amaltaro_SC_MultiPU_Oct2021_Val_211001_104516_5285/GenSimFull/0000/0/0f39520e-e7c0-4dc5-908d-7b8945b54229-0-0-logArchive.tar.gz",
            "davs://eoscms.cern.ch:443/eos/cms/store/unmerged/logs/prod/2021/10/1/cmsunified_ACDC0_task_BPH-RunIISummer20UL17MiniAODv2-00124__v1_T_211001_030122_7466/BPH-RunIISummer20UL17MiniAODv2-00124_0/10000/0/7cb9168e-550b-4af8-b45d-5d5a96dc663e-0-0-logArchive.tar.gz"]
    for dirPfn in rseDirs:
        try:
            # NOTE: For gfal2 rmdir() exit status of 0 is success
            logger.warning(f"Removing directory {dirPfn} ...")
            rmdirSuccess = ctx.rmdir(dirPfn) == 0
        except gfal2.GError as gfalExc:
            logger.warning(f"MISSING directory: {dirPfn}, gfal code={gfalExc.code}, message={gfalExc.message}, args={gfalExc.args}")

def main():
    ctx = createGfal2Context()
    testGFAL(ctx)
    print("succeeded")

if __name__ == '__main__':
    main()