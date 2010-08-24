#!/usr/bin/env python
"""
_promptSkimInjector_

Recreate portions of the T0AST schema that are necessary to test PromptSkimming
for one run and copy all necessary data as well.
"""

import logging
import sys

from WMCore.Database.DBFactory import DBFactory
from WMCore.WMBS.Oracle.Create import Create

def fixupLocal(localT0AST):
    """
    _fixupLocal_

    Change the acquisition era to something so that we can easily distinguish
    dataproduced by this as bogus and reset all block status so that they get
    skimmed.
    """
    acqEra = "UPDATE run SET acq_era = 'PromptSkimTest'"
    block = """UPDATE block SET status =
                 (SELECT id FROM block_status WHERE status = 'Exported')"""

    localT0AST.processData(acqEra)
    localT0AST.processData(block)
    return

def copyTableSubset(remoteT0AST, localT0AST, tableName, selectSQL, selectBinds):
    """
    _copyTableSubset_

    Copy a subset of the data out of a remote T0AST instance into a local T0AST
    instance.
    """
    print "\nCopying %s..." % tableName,

    resultSets = remoteT0AST.processData(selectSQL, selectBinds)
    results = []
    for resultSet in resultSets:
        results.extend(resultSet.fetchall())

    numCols = len(results[0])

    insert = "INSERT INTO %s VALUES(" % tableName
    for i in range(numCols):
        insert += ":p_%i, " % i
    insert = insert[:-2] + ")"

    binds = []
    for result in results:
        newBind = {}
        for i in range(numCols):
            newBind["p_%i" % i] = result[i]
        binds.append(newBind)

    localT0AST.processData(insert, binds)
    print "done."
    return

def determineSchema(remoteT0AST):
    """
    _determineSchema_

    Use the meta data maintained by Oracle to determine the schema.  The data
    is returned as a dictionary keyed by tablename.  The values are a list of
    columns where each column is a dictionary with information about the column.
    """
    sqlQuery = """SELECT table_name, column_id, column_name, data_type, data_length
                  FROM all_tab_columns WHERE owner = 'CMS_T0AST_1'"""
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

    Create the T0AST schema in the local database instance.
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

def copyData(remoteT0AST, localT0AST, tableInfo, runNumber):
    """
    _copyData_

    Copy data from the remote T0AST to the localT0AST for all the tables
    in the tableInfo parameter.  If the table has a RUN_ID column limit rows
    copied to those from the given run.
    """
    print "\nCopying T0AST metadata for run %s:" % runNumber
       
    for tableName in tableInfo.keys():
        hasRun = False
        tableCols = tableInfo[tableName]
        for tableCol in tableCols:
            if tableCol["NAME"] == "RUN_ID":
                hasRun = True
                break
        
        rowQuery = "SELECT count(*) FROM %s" % tableName
        if hasRun:
            rowQuery += " WHERE run_id = %s" % runNumber
            
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
        if hasRun:
            selectQuery += " WHERE run_id = %s" % runNumber            

        insertQuery = insertQuery[:-1] + ") VALUES ("
        for i in range(len(columns)):
            insertQuery += ":p_%s," % i

        insertQuery = insertQuery[:-1] + ")"

        resultSets = remoteT0AST.processData(selectQuery)
        results = []
        for resultSet in resultSets:
            results.extend(resultSet.fetchall())

        for result in results:
            bindVars = {}
            bindVarCounter = 0
            for col in result:
                bindVars["p_%s" % bindVarCounter] = col
                bindVarCounter += 1

            localT0AST.processData(insertQuery, bindVars, None, None)

if len(sys.argv) != 4:
    print "Usage:"
    print "  ./promptSkimInjector LOCAL_T0AST_URL PROD_T0AST_READER_URL RUN_NUMBER"
    print ""
    sys.exit(0)

localT0ASTUrl = sys.argv[1]
remoteT0ASTUrl = sys.argv[2]
runNum = int(sys.argv[3])

localDBFactory = DBFactory(logging, localT0ASTUrl)
remoteDBFactory = DBFactory(logging, remoteT0ASTUrl)

localDbi = localDBFactory.connect()
remoteDbi = remoteDBFactory.connect()

print "\nCreating WMBS Schema in local Oracle...",
wmbsCreate = Create(logging, localDbi)
wmbsCreate.execute()
print "done."

copyTableNames = ["processed_dataset", "primary_dataset", "data_tier",
                  "dataset_path", "run_status", "block_status",
                  "block_migrate_status", "run_stream_cmssw_assoc",
                  "cmssw_version", "t1skim_config", "phedex_subscription",
                  "storage_node", "run", "block_run_assoc"]

createTableNames = ["processed_dataset", "primary_dataset", "data_tier",
                    "dataset_path", "run_status", "block_status",
                    "block_migrate_status", "run_stream_cmssw_assoc",
                    "cmssw_version", "t1skim_config", "phedex_subscription",
                    "storage_node", "run", "block_run_assoc", "block",
                    "wmbs_file_dataset_path_assoc", "wmbs_file_block_assoc",
                    "block_parentage"]

tableInfo = determineSchema(remoteDbi)
for tableName in tableInfo.keys():
    if tableName.lower() not in createTableNames:
        del tableInfo[tableName]

createSchema(localDbi, tableInfo)

for tableName in tableInfo.keys():
    if tableName.lower() not in copyTableNames:
        del tableInfo[tableName]
copyData(remoteDbi, localDbi, tableInfo, runNum)

blockSelect = """SELECT id, dataset_path_id, block_size, file_count, status,
                        migrate_status, delete_status, name, export_start_time,
                        export_end_time FROM block
                   INNER JOIN block_run_assoc ON
                     block.id = block_run_assoc.block_id
                 WHERE block_run_assoc.run_id = :runid"""

copyTableSubset(remoteDbi, localDbi, "block", blockSelect, {"runid": runNum})

blockParentSelect = """SELECT input_id, output_id FROM block_parentage
                         INNER JOIN
                           (SELECT DISTINCT(id) AS id FROM block
                              INNER JOIN block_run_assoc ON
                                block.id = block_run_assoc.block_id
                            WHERE block_run_assoc.run_id = :runid) block_run ON
                           input_id = block_run.id"""

copyTableSubset(remoteDbi, localDbi, "block_parentage", blockParentSelect,
                {"runid": runNum})

fileBlockSelect = """SELECT DISTINCT(file_id), block_id FROM wmbs_file_block_assoc
                       INNER JOIN wmbs_file_runlumi_map ON
                         wmbs_file_block_assoc.file_id = wmbs_file_runlumi_map.fileid
                     WHERE wmbs_file_runlumi_map.run = :runid"""

copyTableSubset(remoteDbi, localDbi, "wmbs_file_block_assoc", fileBlockSelect,
                {"runid": runNum})

fileDatasetSelect = """SELECT DISTINCT(file_id), dataset_path_id FROM wmbs_file_dataset_path_assoc
                         INNER JOIN wmbs_file_runlumi_map ON
                           wmbs_file_dataset_path_assoc.file_id = wmbs_file_runlumi_map.fileid
                       WHERE wmbs_file_runlumi_map.run = :runid"""

copyTableSubset(remoteDbi, localDbi, "wmbs_file_dataset_path_assoc", fileDatasetSelect,
                {"runid": runNum})

fileDetailsSelect = """SELECT DISTINCT(id), lfn, filesize, events, first_event, last_event, merged
                         FROM wmbs_file_details
                         INNER JOIN wmbs_file_runlumi_map ON
                           wmbs_file_details.id = wmbs_file_runlumi_map.fileid
                       WHERE wmbs_file_runlumi_map.run = :runid"""

copyTableSubset(remoteDbi, localDbi, "wmbs_file_details", fileDetailsSelect,
                {"runid": runNum})

fileRunSelect = "SELECT fileid, run, lumi FROM wmbs_file_runlumi_map WHERE run = :runid"

copyTableSubset(remoteDbi, localDbi, "wmbs_file_runlumi_map", fileRunSelect, {"runid": runNum})

fileParentsSelect = """SELECT child, parent FROM wmbs_file_parent
                         INNER JOIN
                           (SELECT DISTINCT(id) AS id FROM wmbs_file_details
                              INNER JOIN wmbs_file_runlumi_map ON
                                wmbs_file_details.id = wmbs_file_runlumi_map.fileid
                            WHERE wmbs_file_runlumi_map.run = :runid) run_files ON
                           wmbs_file_parent.parent = run_files.id"""

copyTableSubset(remoteDbi, localDbi, "wmbs_file_parent", fileParentsSelect,
                {"runid": runNum})

fixupLocal(localDbi)
