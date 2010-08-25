#!/usr/bin/env python
"""
_promptSkimInjector_

"""

from WMCore.Database.DBFactory import DBFactory

def determineSchema(remoteT0AST, tableName):
    """
    _determineSchema_

    Use the meta data maintained by Oracle to determine the schema.  The data
    is returned as a dictionary keyed by tablename.  The values are a list of
    columns where each column is a dictionary with information about the column.
    """
    sqlQuery = "SELECT tname, colno, cname, coltype, width FROM col WHERE tname = %s" % tableName
    resultSets = remoteT0AST.processData(sqlQuery)

    results = []
    for resultSet in resultSets:
        results.extend(resultSet.fetchall())

    tableInfo = {}
    for result in results:
        if result[0] not in tableInfo.keys():
            tableInfo[result[0]] = []

        newColumn = {}
        newColumn["NUMBER"] = int(result[1])
        newColumn["NAME"] = result[2]
        newColumn["TYPE"] = result[3]
        newColumn["WIDTH"] = result[4]

        tableInfo[result[0]].append(newColumn)

    return tableInfo

def compareColumns(a, b):
    """
    _compareColumns_

    Compare two columns by their column number.
    """
    if a["NUMBER"] > b["NUMBER"]:
        return 1
    elif a["NUMBER"] == b["NUMBER"]:
        return 0
    else:
        return -1

def createSchema(localT0AST, tableInfo):
    """
    _createSchema_

    Create the Oracle schema in a SQLite database.
    """
    for tableName in tableInfo.keys():
        columns = tableInfo[tableName]
        columns.sort(compareColumns)

        createString = "CREATE TABLE %s (" % tableName.lower()

        for column in columns:
            if column["TYPE"] == "NUMBER":
                colStr = "%s INTEGER," % column["NAME"].lower()
            elif column["TYPE"] == "FLOAT":
                colStr = "%s REAL," % column["NAME"].lower()
            elif column["TYPE"] == "VARCHAR2":
                colStr = "%s VARCHAR(%s)," % (column["NAME"].lower(), column["WIDTH"])
            elif column["TYPE"] == "CHAR":
                colStr = "%s VARCHAR(%s)," % (column["NAME"].lower(), column["WIDTH"])
            elif column["TYPE"] == "CLOB":
                colStr = "%s CLOB," % (column["NAME"].lower())
            elif column["TYPE"] == "TIMESTAMP":
                colStr = "%s TIMESTAMP," % (column["NAME"].lower())
            elif column["TYPE"] == "TIMESTAMP(6)":
                colStr = "%s TIMESTAMP," % (column["NAME"].lower())
            else:
                print "Unknown column type: %s" % column["TYPE"]
                colStr = None
                
            createString += colStr

        createString = createString[:-1] + ")"
        localT0AST.processData(createString, None, None, None)

    return

def copyData(remoteT0AST, localT0AST, tableNames):
    """
    _copyData_

    Copy data between the Oracle and SQLite databases.
    """
    print "Copying some data:"
    
    tableInfo = []
    for tableName in tableNames:
        tableInfo.append(determineSchema(remoteT0AST, tableName))
    
    for tableName in tableInfo.keys():
        rowQuery = "SELECT count(*) FROM %s" % tableName
        resultSets = remoteT0AST.processData(rowQuery)
        result = resultSets[0].fetchall()[0][0]

        print "  %s -> %s rows" % (tableName, result)

        selectQuery = "SELECT "
        insertQuery = "INSERT INTO %s (" % tableName

        columns = tableInfo[tableName]
        columns.sort(compareColumns)
        for column in columns:
            selectQuery += "%s," % column["NAME"]
            insertQuery += "%s," % column["NAME"]

        selectQuery = selectQuery[:-1] + " FROM %s" % tableName
        insertQuery = insertQuery[:-1] + ") VALUES ("

        for i in range(len(columns)):
            insertQuery += ":p_%s," % i

        insertQuery = insertQuery[:-1] + ")"

        t0astDBConn.execute(selectQuery)
        results = t0astDBConn.fetchall()

        for result in results:
            bindVars = {}
            bindVarCounter = 0
            for col in result:
                bindVars["p_%s" % bindVarCounter] = col
                bindVarCounter += 1

            localT0AST.processData(insertQuery, bindVars, None, None)

localT0ASTUrl = "oracle://sfoulkes:sfoulkes_cms2008@cmscald:1521"

# Connect to remote T0AST
# Connect to local T0AST
# Install WMBS in local t0ast

tableNames = ["processed_dataset", "primary_dataset", "data_tier",
              "dataset_path", "run_status", "block_status",
              "block_migrate_status", "run_stream_cmssw_assoc",
              "cmssw_version", "t1skim_config", "phedex_subscription",
              "storage_node"]

