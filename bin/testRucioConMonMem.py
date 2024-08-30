import os
import sys
import logging
from memory_profiler import profile
from WMCore.Services.RucioConMon.RucioConMon import RucioConMon

RSE_NAME = "T2_AT_Vienna"
RUCIO_CONMON_URL = "https://cmsweb.cern.ch/rucioconmon/unmerged"

def loggerSetup(logLevel=logging.INFO):
    logger = logging.getLogger(__name__)
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    logger.addHandler(outHandler)
    logger.setLevel(logLevel)
    return logger


profileFp = open('getUnmergedFiles.log', 'w+')
@profile(stream=profileFp)
def getUnmergedFiles(rucioConMon, logger, compressed=False):
    dirs = set()
    counter = 0
    logger.info("Fetching data from Rucio ConMon for RSE: %s.", RSE_NAME)
    for lfn in rucioConMon.getRSEUnmerged(RSE_NAME, zipped=compressed):
        dirPath = _cutPath(lfn)
        dirs.add(dirPath)
        counter =+ 1
    logger.info(f"Total files received: {counter}, unique dirs: {len(dirs)}")
    return dirs


def _cutPath(filePath):
    newPath = []
    root = filePath
    while True:
        root, tail = os.path.split(root)
        if tail:
            newPath.append(tail)
        else:
            newPath.append(root)
            break
    newPath.reverse()
    # Cut/slice the path to the level/element required.
    newPath = newPath[:7]
    # Build the path out of all that is found up to the deepest level in the LFN tree
    finalPath = os.path.join(*newPath)
    return finalPath


def main():
    logger = loggerSetup()
    zipped=False
    rucioConMon = RucioConMon(RUCIO_CONMON_URL, logger=logger)
    logger.info(f"Fetching unmerged dump for RSE: {RSE_NAME} with compressed data: {zipped}")
    getUnmergedFiles(rucioConMon, logger, compressed=zipped)

    zipped=True
    logger.info(f"Fetching unmerged dump for RSE: {RSE_NAME} with compressed data: {zipped}")
    getUnmergedFiles(rucioConMon, logger, compressed=zipped)
    logger.info("Done!")


if __name__ == "__main__":
    sys.exit(main())
