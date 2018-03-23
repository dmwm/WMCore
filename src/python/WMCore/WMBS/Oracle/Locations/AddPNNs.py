#!/usr/bin/env python
"""
Oracle implementation of AddPNNs
"""
from __future__ import division

from WMCore.WMBS.MySQL.Locations.AddPNNs import AddPNNs as AddPNNsMySQL


class AddPNNs(AddPNNsMySQL):

    sql = """INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX (wmbs_pnns (pnn)) */
               INTO wmbs_pnns (id, pnn) VALUES (wmbs_pnns_SEQ.nextval, :pnn)"""