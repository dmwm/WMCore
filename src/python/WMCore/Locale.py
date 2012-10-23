#!/usr/bin/env python
# encoding: utf-8
"""
Locale.py

Template for a locale configuration that contains all the stuff that agent operators want to customise
on a regular basis

Created by Dave Evans on 2011-05-20.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import socket

from WMCore.Configuration import Configuration

config = Configuration()
config.section_('locale')

#
# Define the required sections
#
config.locale.section_("reqmgr")
config.locale.section_("gwq")
config.locale.section_("lwq")
config.locale.section_("agent")
config.locale.section_("crabserver")
config.locale.section_("couch")
config.locale.section_("mysql")
config.locale.section_("certificates")

# canned settings for each section
# request manager
config.locale.reqmgr.hostname = "cmssrv98.fnal.gov"
config.locale.reqmgr.port = "9798"
config.locale.reqmgr.database = "reqmgrdb"

# global workqueue
config.locale.gwq.hostname = "cmssrv98.fnal.gov"
config.locale.gwq.port = "9996"
config.locale.gwq.database = "workqueuedb"

# local workqueue
config.locale.lwq.port = "9997"
config.locale.lwq.database = "workqueuedb"

#crabserver
config.locale.crabserver.host = "cmssrv98.fnal.gov"
config.locale.crabserver.port = "9600"

# couch installation
config.locale.couch.port = "5984"
config.locale.couch.host = "0.0.0.0"
config.locale.couch.hostname = socket.gethostname()
config.locale.couch.url = None

#mysql settings
config.locale.mysql.socket = None
config.locale.mysql.url = None

# agent settings
config.locale.agent.teamName = "DMWM"
config.locale.agent.name = os.environ['USER']
config.locale.agent.hostname = socket.gethostname()
config.locale.agent.database = "wmagentdb"

# make it easy to import the Locale config as Locale:
Locale = config
