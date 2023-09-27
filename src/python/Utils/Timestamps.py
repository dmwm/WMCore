#!/usr/bin/env python
"""
_Timestamps_ module designed to insert proper WM timing into WMCore FJR.
Usage: python3 Timestamps.py --reportFile=$outputFile --wmJobStart=<sec> --wmJobEnd=<sec>
where outputFile represents output FJR file, and wmJobStart/wmJobEnd represent
start and end time of WM job, respectively.

NOTE: this script should reside in Utils area of WMCore repository since it is required
at run time on condor node where there is no WMCore installation and we only provide WMCore.zip
archive, and worker node may not have unzip tool to extract this script.

"""

import getopt
import logging
import os
import pickle
import sys


# script options
options = {"reportFile=": "", "wmJobStart=": '', "wmJobEnd=": ''}


def addTimestampMetrics(config, wmJobStart, wmJobEnd):
    """
    Adjust timestamp metrics of provied FJR data.

    :param config: input FJR data
    :param wmJobStart: start time of WM job in seconds
    :param wmJobEnd: end time of WM job in seconds
    """
    wmTiming = config.section_('WMTiming')
    wmTiming.WMJobStart = wmJobStart
    wmTiming.WMJobEnd = wmJobEnd
    wmTiming.WMTotalWallClockTime = wmJobEnd - wmJobStart
    return config


def main():
    """
    Main function of the script performs business logic:
    - parse input arguments
    - read provided input FJR
    - adjust timestamp metrics of FJR data
    - write out FJR
    """
    try:
        opts, _ = getopt.getopt(sys.argv[1:], "", list(options.keys()))
    except getopt.GetoptError as ex:
        msg = "Error processing commandline args:\n"
        msg += str(ex)
        logging.error(msg)
        sys.exit(1)

    for opt, arg in opts:
        if opt == "--reportFile":
            reportFile = arg
        if opt == "--wmJobStart":
            wmJobStart = float(arg)
        if opt == "--wmJobEnd":
            wmJobEnd = float(arg)

    # read content of given FJR report file
    data = {}
    with open(reportFile, 'rb') as istream:
        data = pickle.load(istream)

    # adjust FJR data with provided metrics
    data = addTimestampMetrics(data, wmJobStart, wmJobEnd)

    # write content of FJR back to report file
    reportOutFile = reportFile + ".new"
    msg = f"Adding wmJobTime metric to {reportFile}"
    with open(reportOutFile, 'wb') as ostream:
        logging.info(msg)
        pickle.dump(data, ostream)

    # if we successfully wrote reportOutFile we can swap it with input one
    sizeStatus = os.path.getsize(reportOutFile) >= os.path.getsize(reportFile)
    if os.path.isfile(reportOutFile) and sizeStatus:
        os.rename(reportOutFile, reportFile)


if __name__ == '__main__':
    main()
