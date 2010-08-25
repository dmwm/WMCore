#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
__MsgService__

This class calls the MySQL MsgService for use with SQLite

"""






#This is the SQLite placeholder for the MySQL MsgService.
#It does nothing, and needs to do nothing, everything is
#done properly in MySQL.
# -mnorman

from WMCore.MsgService.MySQL.MsgService import MsgService as MySQLMsgService

class MsgService(MySQLMsgService):

    """
    __MsgService__

    This class calls the MySQL MsgService for use with SQLite
    
    """

    def MsgServiceDialect(self):

        return 'SQLite'
