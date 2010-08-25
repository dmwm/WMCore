#!/usr/bin/env python
"""
_AddRunLumi_

MySQL implementation of AddRunLumi
"""

__revision__ = "$Id: AddRunLumi.py,v 1.8 2009/12/16 17:45:41 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter

class AddRunLumi(DBFormatter):

    sql = """insert wmbs_file_runlumi_map (file, run, lumi) 
            select id, :run, :lumi from wmbs_file_details 
            where lfn = :lfn"""

    def getBinds(self, file=None, runs=None):

	binds = []

	if type(file) == type('string'):
		lfn = file
		
	elif type(file) == type({}):
		lfn = file('lfn')
	else:
	    raise Exception, "Type of file argument is not allowed: %s" \
                                % type(file)

	if isinstance(runs, set):
		for run in runs:
			for lumi in run: 
				binds.append({'lfn': lfn,
						'run': run.run,
						'lumi':lumi})
	else:
            raise Exception, "Type of runs argument is not allowed: %s" \
                                % type(runs)
	return binds
				
    def format(self, result):
        return True
    
    def execute(self, file=None, runs=None, conn = None, transaction = False):
        binds = self.getBinds(file, runs)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)



