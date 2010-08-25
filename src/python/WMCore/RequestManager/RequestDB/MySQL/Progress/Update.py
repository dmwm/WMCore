#!/usr/bin/env python
"""
_Progress.Update_

API for creating a new progress update for a request

"""



from WMCore.Database.DBFormatter import DBFormatter

class Update(DBFormatter):
    """
    _Update_

    Insert a progress update for a request

    """
    def execute(self, requestId, conn = None, trans = False, **params):
        """
        _execute_

        Add a progress update to teh request Id provided, params can
        optionally contain:

        - *events_written* Int
        - *events_merged*  Int
        - *files_written*  Int
        - *files_merged*   int
        - *percent_success* Float
        - *percent_complete Float
        - *associated_dataset*        string (dataset name)
        - *time_per_event int
        - *size_per_event int

        """

        self.sql = "INSERT INTO reqmgr_progress_update "
        self.sql += "(request_id, update_time,"

        values = " VALUES ( %s, CURRENT_TIMESTAMP," % requestId

        for key, value in params.iteritems():
            if value != None and key != 'requestName':
                self.sql += key+', '
                if str(value).isdigit():
                    values += " %s," % value
                else:
                    values += " \'%s\'," % value
                    print values

        #if params.get("events_written", None) != None:
        #    self.sql += "events_written, "
        #    values += " %s," % params['events_written']

        #if params.get("events_merged", None) != None:
        #    self.sql += "events_merged, "
        #    values += " %s," % params['events_merged']

        #if params.get("files_written", None) != None:
        #    self.sql += "files_written, "
        #    values += " %s," % params['files_written']
        #if params.get("files_merged", None) != None:
        #    self.sql += "files_merged, "
        #    values += " %s," % params['files_merged']

        #if params.get("dataset", None) != None:
        #    self.sql += "dataset, "
        #    values += " \'%s\'," % params['dataset']

        self.sql = self.sql.rstrip().rstrip(',')
        values = values.rstrip(',')
        self.sql += ") %s)" % values
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = trans)
