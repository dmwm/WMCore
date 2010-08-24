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

from urllib import urlretrieve
from urllib import urlopen

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
        self.baseURL = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockReplicas?node=%s" % self.args['Node']
        self.timestamp = 0
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
        baseFileName = self.args['ComponentDir'] + "/base.json"
        if not os.path.isfile( baseFileName ):
            logging.debug("PhEDExNotifier: downloading base.json file")
            urlretrieve( self.baseURL, baseFileName )
        f = open( baseFileName )
        self.base = eval( f.read(), {}, {} )
        f.close()
        phedex = self.base[ 'phedex' ]
        self.timestamp = phedex[ 'request_timestamp' ]

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

        updatedConnection = urlopen( updatedURL )
        createdConnection = urlopen( createdURL )

        updatedString = updatedConnection.read()
        createdString = createdConnection.read()

        updatedConnection.close()
        createdConnection.close()

        updated = eval( updatedString, {}, {} )
        created = eval( createdString, {}, {} )
        
        updatedBlocks = updated[ 'phedex' ][ 'block' ]
        if len( updatedBlocks ) > 0:
            logging.debug( "PhEDExNotifier: Found %d updated blocks" % len( updatedBlocks) )

        createdBlocks = created[ 'phedex' ][ 'block' ]
        if len( createdBlocks ) > 0:
            logging.debug( "PhEDExNotifier: Found %d created blocks" % len( createdBlocks ) )
        
        self.handleUpdates( updatedBlocks )

        self.handleCreated( createdBlocks )

    def handleUpdates( self, updates ):

        for update in updates:
            blockId = update[ 'id' ]
            for block in self.base[ 'phedex' ][ 'block' ]:
                if block[ 'id' ] == blockId:
                    filesInUpdate = int( update[ 'replica' ][0][ 'files' ] )
                    filesInBase = int( block[ 'replica' ][0][ 'files' ] )
                    newFiles = filesInUpdate - filesInBase
                    if filesInUpdate > filesInBase:
                        logging.debug( "PhEDExNotifier: block %s has %d new files" % ( update[ 'name' ], newFiles ) )

                    

    def handleCreated( self, theCreated ):

        for aCreated in theCreated:
            filesInCreated = int( aCreated[ 'replica' ][0][ 'files' ] )
            logging.debug( "PhEDExNotifier: new block %s has %d files" % ( aCreated[ 'name' ], filesInCreated ) )
