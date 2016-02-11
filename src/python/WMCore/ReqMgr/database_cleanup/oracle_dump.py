"""
Dump of Oracle database into Python dictionary.

Usage:
   python oracle_dump.py user/password@server [TABLE_NAME] > output.dump
   python ./oracle_dump.py user/password@server reqmgr_request > \
       oracle_dump_request_table.py
       
"""
from __future__ import print_function


import sys
import cx_Oracle
from .oracle_tables import reqmgr_oracle_tables_defition


def main():
    if len(sys.argv) < 2:
        print("Missing the connect TNS argument.")
        sys.exit(1)
    tns = sys.argv[1]
    # tables is dictionary:
    # {"table1": [col1, col2, ...], "table2": [col1, col2, ...], ...}

    # printout statements with # (comment, result must be functional Python
    # file)

    # if another argument is specified, it's assumed it's the table name
    # which only shall be dumped
    tables = {}
    if len(sys.argv) > 2:
        table_name = sys.argv[2]
        print("# Dumping only '%s' ..." % table_name)
        tables[table_name] = reqmgr_oracle_tables_defition[table_name]
    else:
        tables = reqmgr_oracle_tables_defition

    connection = cx_Oracle.Connection(tns)
    cursor = cx_Oracle.Cursor(connection)
    
    for table in tables:
        cmd = "select %s from %s" % (", ".join(tables[table]), table)
        print("# %s" % cmd)
        cursor.prepare(cmd)
        cursor.execute(cmd)
        print("%s = [" % table)
        for row in cursor.fetchall():
            print("{")
            for k, v in zip(tables[table], row):
                print("\t'%s': '%s'," % (k, v))
            print("},")
        print("]")    
        print("# rowcount: %s\n\n\n" % cursor.rowcount)

    cursor.close()


if __name__ == "__main__":
    main()
