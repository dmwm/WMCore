#!/usr/bin/env python
"""
_PhEDExNotifierComponent_

ProdAgent Component to notify clients of new transfers

"""

import os
import logging

from ProdCommon.Database import Session
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
import ProdAgentCore.LoggingUtils as LoggingUtils

from xml.dom.minidom import Document
from xml.dom import minidom
from urllib import urlretrieve

class PhEDExNotifierComponent:
    """
    _PhEDExNotifierComponent_

    """
    
    def __init__(self, **args):
        
        self.args = {}
        self.args['ComponentDir'] = None
        self.args['Logfile'] = None
        self.args['Node'] = None
        self.args.update(args)
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(
                self.args['ComponentDir'],
                "ComponentLog")
        LoggingUtils.installLogHandler(self)

        if self.args['Node'] == None:
            logging.error("PhEDExNotifier: no node specified")
        self.baseURL = "https://cmsweb.cern.ch/phedex/datasvc/xml/prod/blockReplicas?node=%s" % self.args['Node']
        self.timestamp = 0
        self.doc = Document()
        self.ms = None
        msg = "PhEDExNotifier Component Started\n"
        logging.info(msg)


    def __call__(self, event, payload):
        """
        _operator(message, payload)_

        Respond to messages from the message service

        """
        logging.debug("Message=%s Payload=%s" % (event, payload))


        if event == "PhEDExNotifier:Poll":
            self.doPoll()
            return

        if event == "PhEDExNotifier:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "PhEDExNotifier:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        
        return
    
    def startComponent(self):
        """
        _startComponent_
        
        Start the servers required for this component
        
        """                                   
        # create message service
        self.ms = MessageService()
        
        # register
        self.ms.registerAs("PhEDExNotifier")                                                                                
        # subscribe to messages
        self.ms.subscribeTo("PhEDExNotifier:Poll")
        self.ms.subscribeTo("PhEDExNotifier:StartDebug")
        self.ms.subscribeTo("PhEDExNotifier:EndDebug")
        
        # Get current state from PhEDEx or local file if it exists
        baseFileName = self.args['ComponentDir'] + "/base.xml"
        if not os.path.isfile( baseFileName ):
            logging.debug("PhEDExNotifier: downloading base.xml file")
            urlretrieve( self.baseURL, baseFileName )
        self.doc = minidom.parse( baseFileName )
        phedex = self.doc.getElementsByTagName( "phedex" ).item( 0 )        
        self.timestamp = phedex.getAttribute( "request_timestamp" )

        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            msgtype, payload = self.ms.get()
            self.ms.commit()
            logging.debug("PhEDExNotifier: %s, %s" % (msgtype, payload))
            self.__call__(msgtype, payload)
            Session.commit_all()
            Session.close_all()
            
           
    def doPoll(self):
        """
        _doPoll_

        Polls PhEDEx webservice for any new or updated blocks
        since last run.

        """

        updatedURL = self.baseURL + "&updated_since=%s" % self.timestamp
        createdURL = self.baseURL + "&create_since=%s" % self.timestamp

        updatedFile = self.args['ComponentDir'] + "/updated.xml"
        createdFile = self.args['ComponentDir'] + "/created.xml"

        urlretrieve( updatedURL, updatedFile )
        urlretrieve( createdURL, createdFile )

        updated = minidom.parse( updatedFile )
        created = minidom.parse( createdFile )

        updatedBlocks = updated.getElementsByTagName( "block" )
        if updatedBlocks.length > 0:
            logging.debug( "PhEDExNotifier: Found %d updated blocks" % updatedBlocks.length )

        createdBlocks = created.getElementsByTagName( "block" )
        if createdBlocks.length > 0:
            logging.debug( "PhEDExNotifier: Found %d created blocks" % createdBlocks.length )
        
        self.handleUpdates( updatedBlocks )

        self.handleCreated( createdBlocks )

    def handleUpdates( self, updates ):

        for update in updates:
            blockId = update.getAttribute( "id" )
            for block in self.doc.getElementsByTagName( "block" ):
                if block.getAttribute( "id" ) == blockId:
                    filesInUpdate = int( update.getElementsByTagName( "replica" )[0].getAttribute( "files" ) )
                    filesInBase = int( block.getElementsByTagName( "replica" )[0].getAttribute( "files" ) )
                    newFiles = filesInUpdate - filesInBase
                    if filesInUpdate > filesInBase:
                        logging.debug( "PhEDExNotifier: block %s has %d new files" % ( update.getAttribute( "name" ), newFiles ) )

                    

    def handleCreated( self, theCreated ):

        for aCreated in theCreated:
            filesInCreated = int( aCreated.getElementsByTagName( "replica" )[0].getAttribute( "files" ) )
            logging.debug( "PhEDExNotifier: new block %s has %d files" % ( aCreated.getAttribute( "name" ), filesInCreated ) )
