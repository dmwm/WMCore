#!/usr/bin/env python
"""
_Credential_
Parent of all credentials class.
Childs class should implement methods of this class.
"""
from builtins import object
class Credential(object):
    """
    An abstract credential
    """

    def __init__( self, args ):

        self.serverKey = args.get( "server_key", "$HOME/.globus/hostkey.pem")
        self.serverCert = args.get( "server_cert", "$HOME/.globus/hostcert.pem")
        self.minTimeLeft = args.get( "min_time_left", 3600 )
        self.credServerPath = args.get( "cred_storage_path", '/tmp')
        self.userName = args.get( "userName", '')

    def create( self ):
        """
        Init the user credential
        """
        raise NotImplementedError

    def renew( self ):
        """
        Renew the user credential
        """
        raise NotImplementedError

    def get( self ):
        """
        Get user credential
        """
        raise NotImplementedError

    def store( self ):
        """
        Store user credential path in DB
        """
        raise NotImplementedError

    def check( self, credential=None ):
        """
        Check user credential
        """
        # Get the credential time left in second
        timeLeftLocal = self.getTimeLeft( credential )

        if timeLeftLocal and int( timeLeftLocal ) < self.minTimeLeft :
            return False
        else:
            return True

    def delegate( self, credential=None, serverRenewer = None ):
        """
        Delegate the user credential
        """
        raise NotImplementedError

    def getUserName( self, credential=None ):
        """
        Get the user name
        """
        raise NotImplementedError

    def getTimeLeft( self, credential=None ):
        """
        Get credential time left
        """
        raise NotImplementedError

    def destroy( self, credential = None ):
        """
        Destroy user credential
        """
        raise NotImplementedError
