#!/usr/bin/env python
"""
Creates all the tables required to test an agent
"""

import commands
import logging
import os
import threading
import time


from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

# Make the DB Tables
def makeTables(component):
    """ Defines the database tables for a component, such as "WMCore.ThreadPool" """
    myThread = threading.currentThread()
    myThread.transaction = Transaction(myThread.dbi)
    myThread.transaction.begin()
    factory = WMFactory(component, component + "." + \
        myThread.dialect)
    create = factory.loadObject("Create")
    createworked = create.execute(conn = myThread.transaction.conn)
    if createworked:
        print ("Tables for "+ component + " created")
    else:
        print ("Tables " + component + \
        " could not be created, already exists?")
    myThread.transaction.commit()

def setUp():
    """
    setup for test.
    """
    myThread = threading.currentThread()
    myThread.logger = logging.getLogger()
    myThread.dialect = 'MySQL'

    options = {}
    options['unix_socket'] = os.getenv("DBSOCK")
    dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
        options)

    myThread.dbi = dbFactory.connect()

def tearDown():
    """
    Database deletion
    """
    # call the script we use for cleaning:
    command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
    result = commands.getstatusoutput(command)
    for entry in result:
        print(str(entry))

setUp()
tearDown()
makeTables("WMCore.WMBS")
makeTables("WMCore.Agent.Daemon")
makeTables("WMCore.MsgService")
makeTables("WMCore.ThreadPool")
makeTables("WMComponent.FeederManager.Database")