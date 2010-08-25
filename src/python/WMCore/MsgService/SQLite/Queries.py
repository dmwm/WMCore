#!/usr/bin/env python
#pylint: disable-msg=E1103

#FIXME: there are many commit statements
# in these methods. Perhaps they can be factored out.
# but be careful this can lead to deadlock exceptions.
"""
_Queries_

This module implements the SQLite backend for the message
service through the MySQL version

"""

__revision__ = ""
__version__ = ""
__author__ = "mnorman@fnal.gov"

import threading

from WMCore.MsgService.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
	"""
	_Queries_
	
	This module implements the SQLite backend for the message
	service through the MySQL version
	
	"""
	

	def lastInsertId(self):
		"""
		__lastInsertId__

		Checks for last inserted id 
		"""

                #Changed for SQLite
		sqlStr = """
SELECT LAST_INSERT_ROWID()
		"""
		result = self.execute(sqlStr, {})
		return self.formatOne(result)[0]


	def setBufferState(self, args):
		"""
		__setBufferState__
		
		Sets the state of a buffer
		"""

                #Changed to INSERT OR IGNORE from MySQL INSERT IGNORE
	        #Should have the same functionality.
	        #TODO: Should this be REPLACE?
                # -mnorman
        
		sqlStr = """
INSERT OR IGNORE INTO ms_check_buffer(buffer, status) VALUES(:buffername,:state)
                """
		self.execute(sqlStr,args)


	def getBufferState(self, args):
		"""
		__bufferState__
		
		Returns the state of the buffer using 
		"""

	        #FOR UPDATE portion removed: SQLite claims that row locking is
                #redundant in their data structure.
                #TODO: Test that
                # -mnorman
        
		if len(args) == 0:
			return []
		if len(args) == 1:
			sqlStr = """
SELECT buffer, status FROM ms_check_buffer WHERE buffer='%s' """ %(str(args[0]))
		else:
			sqlStr = """
SELECT buffer, status FROM ms_check_buffer WHERE buffer IN %s """ %(str(tuple(args)))

		result = self.execute(sqlStr,{})
		return self.format(result)
