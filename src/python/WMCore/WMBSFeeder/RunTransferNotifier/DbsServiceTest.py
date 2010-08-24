#!/usr/bin/env python

from DbsQueryHelper import DbsQueryHelper
import sys
from xml.dom.minidom import parseString

qh = DbsQueryHelper("cmsweb.cern.ch/dbs_discovery/", 443, "cms_dbs_prod_global")

print qh.queryBlockInfo(sys.argv[1])