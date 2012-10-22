"""
_Destroy_

Implementation of Destroy for Oracle

"""

from WMCore.Database.DBFormatter import DBFormatter

class Destroy(DBFormatter):

    def execute(self, subscription = None, conn = None, transaction = False):

        sql = """DECLARE
                 BEGIN

                   execute immediate 'purge recyclebin';

                   -- Tables
                   FOR o IN (SELECT table_name name FROM user_tables) LOOP
                     execute immediate 'drop table ' || o.name || ' cascade constraints';
                   END LOOP;

                   -- Sequences
                   FOR o IN (SELECT sequence_name name FROM user_sequences) LOOP
                     execute immediate 'drop sequence ' || o.name;
                   END LOOP;

                   -- Triggers
                   FOR o IN (SELECT trigger_name name FROM user_triggers) LOOP
                     execute immediate 'drop trigger ' || o.name;
                   END LOOP;

                   -- Synonyms
                   FOR o IN (SELECT synonym_name name FROM user_synonyms) LOOP
                     execute immediate 'drop synonym ' || o.name;
                   END LOOP;

                   -- Functions
                   FOR o IN (SELECT object_name name FROM user_objects WHERE object_type = 'FUNCTION') LOOP
                     execute immediate 'drop function ' || o.name;
                   END LOOP;

                   -- Procedures
                   FOR o IN (SELECT object_name name FROM user_objects WHERE object_type = 'PROCEDURE') LOOP
                     execute immediate 'drop procedure ' || o.name;
                   END LOOP;

                   execute immediate 'purge recyclebin';

                 END;"""

        self.dbi.processData(sql, {}, conn = conn,
                             transaction = transaction)

        return
