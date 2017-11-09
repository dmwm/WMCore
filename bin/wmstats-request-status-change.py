#!/usr/bin/env python
from __future__ import print_function
import os
import sys
from argparse import ArgumentParser
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Configuration import loadConfigurationFile

if __name__ == "__main__":

    if "WMAGENT_CONFIG" not in os.environ:
        print("The WMAGENT_CONFIG environment variable needs to be set before this can run")
        sys.exit(1)

    wmagentConfig = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if hasattr(wmagentConfig, "AnalyticsDataCollector") and hasattr(wmagentConfig.AnalyticsDataCollector, "centralWMStatsURL"):
        wmstats = WMStatsWriter(wmagentConfig.AnalyticsDataCollector.centralWMStatsURL)
    else:
        print("AnalyticsDataCollector.centralWMStatsURL is not specified")
        sys.exit(1)

    parser = ArgumentParser(usage="wmstats-request-status-chagne [agent_url:port]")

    parser.add_argument("-r", "--request", dest = "request",
                        help = "resquest name")

    parser.add_argument("-s", "--status", dest = "newstatus",
                        help = "set to new status")

    options = parser.parse_args()

    if not options.request:
        print("request name needs to be set")
        sys.exit(1)

    if not options.newstatus:
        print("new status needs to be set")
        sys.exit(1)

    answer = input("%s change to %s in wmstats db (yes, no)?" % (options.request, options.newstatus))
    if not answer.lower() == "yes":
        print("Canceled")
        sys.exit(1)

    report = wmstats.updateRequestStatus(options.request, options.newstatus)

    print(report)
