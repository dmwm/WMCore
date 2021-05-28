#!/usr/bin/env python
"""
_DBCore_t_

Unit tests for the DBInterface class
"""




from builtins import range

import unittest
import threading

from WMQuality.TestInit import TestInit

class DBCoreTest(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMQuality.TestDB"],
                                useDefault = False)

        return

    def tearDown(self):
        """
        Delete the databases
        """
        self.testInit.clearDatabase()
        return

    def testBuildBinds(self):
        """
        Test class for DBCore.buildbinds()

        """

        #This class may become obselete soon.  There is a TODO stuck onto DBCore.buildbinds()
        #This just checks to see that the sequence properly packages the first value is set
        #So that it sets the key seqname to the proper name in the files list
        #Also right now tests that it sets the keys right in each dict, but this seems redundant
        # -mnorman

        seqname       = 'file'
        dictbinds     = {'lumi':123, 'test':100}
        files         = ['testA', 'testB', 'testC']

        myThread = threading.currentThread()

        testInterface = myThread.dbi

        binds         = testInterface.buildbinds(files, seqname, dictbinds)

        #This should return a dict for every value in files with additional elements
        #from dictbinds.  We then loop over every dict (which should be equal to the
        #number of elements in files), and look to see that the filename matches and that
        #At least one of the dictbinds keys matches to its proper element

        for i in range(len(files)):
            self.assertEqual(binds[i][seqname], files[i])
            self.assertEqual(binds[i][next(iter(dictbinds))], dictbinds[next(iter(dictbinds))])


        return

    def testProcessDataNoBinds(self):
        """
        _testProcessDataNoBinds_

        Verify that insert and select queries work when no binds are used.
        """
        insertSQL = "INSERT INTO test_tablea VALUES (1, 2, 'three')"
        selectSQL = "SELECT column1, column2, column3 from test_tablea"

        myThread = threading.currentThread()
        myThread.dbi.processData(insertSQL)
        resultSets = myThread.dbi.processData(selectSQL)

        assert len(resultSets) == 1, \
               "Error: Wrong number of ResultSets returned."

        results = resultSets[0].fetchall()

        assert len(results) == 1, \
               "Error: Wrong number of rows returned."
        assert len(results[0]) == 3, \
               "Error: Wrong number of columns returned."
        assert results[0][0] == 1, \
               "Error: Column one is wrong."
        assert results[0][1] == 2, \
               "Error: Column two is wrong."
        assert results[0][2] == "three", \
               "Error: Column three is wrong."

        return

    def testProcessDataOneBind(self):
        """
        _testProcessDataOneBind_

        Verify that insert and select queries work with one set of bind variables.
        """
        bindsA = {"one": 1, "two": 2, "three": "three"}
        bindsB = {"one": 3, "two": 2, "three": "one"}
        insertSQL = "INSERT INTO test_tablea VALUES (:one, :two, :three)"
        selectSQL = \
          """SELECT column1, column2, column3 FROM test_tablea
             WHERE column1 = :one AND column2 = :two AND column3 = :three"""

        myThread = threading.currentThread()
        myThread.dbi.processData(insertSQL, binds = bindsA)
        myThread.dbi.processData(insertSQL, binds = bindsB)

        resultSets = myThread.dbi.processData(selectSQL, bindsA)

        assert len(resultSets) == 1, \
               "Error: Wrong number of ResultSets returned."

        results = resultSets[0].fetchall()

        assert len(results) == 1, \
               "Error: Wrong number of rows returned."
        assert len(results[0]) == 3, \
               "Error: Wrong number of columns returned."
        assert results[0][0] == 1, \
               "Error: Column one is wrong."
        assert results[0][1] == 2, \
               "Error: Column two is wrong."
        assert results[0][2] == "three", \
               "Error: Column three is wrong."

        resultSets = myThread.dbi.processData(selectSQL, bindsB)

        assert len(resultSets) == 1, \
               "Error: Wrong number of ResultSets returned."

        results = resultSets[0].fetchall()

        assert len(results) == 1, \
               "Error: Wrong number of rows returned."
        assert len(results[0]) == 3, \
               "Error: Wrong number of columns returned."
        assert results[0][0] == 3, \
               "Error: Column one is wrong."
        assert results[0][1] == 2, \
               "Error: Column two is wrong."
        assert results[0][2] == "one", \
               "Error: Column three is wrong."

        return

    def testProcessDataSeveralBinds(self):
        """
        _testProcessDataSeveralBinds_

        Verify that insert and select queries work with several binds.
        """
        bindsA = [{"one": 1, "two": 2, "three": "three"},
                  {"one": 3, "two": 2, "three": "one"},
                  {"one": 4, "two": 5, "three": "six"},
                  {"one": 6, "two": 5, "three": "four"}]
        bindsB = [{"one": 10, "two": 11, "three": "twelve"},
                  {"one": 12, "two": 11, "three": "ten"}]

        insertSQL = "INSERT INTO test_tablea VALUES (:one, :two, :three)"
        selectSQL = \
          """SELECT column1, column2, column3 FROM test_tablea
             WHERE column1 = :one AND column2 = :two AND column3 = :three"""

        myThread = threading.currentThread()
        myThread.dbi.processData(insertSQL, binds = bindsA)
        myThread.dbi.processData(insertSQL, binds = bindsB)

        resultSets = myThread.dbi.processData(selectSQL, bindsA)

        assert len(resultSets) == 1, \
               "Error: Wrong number of ResultSets returned."

        results = resultSets[0].fetchall()

        assert len(results) == 4, \
               "Error: Wrong number of rows returned."

        for result in results:
            assert len(result) == 3, \
                   "Error: Wrong number of columns returned."
            for bind in bindsA:
                if bind["one"] == result[0] and bind["two"] == result[1] and \
                   bind["three"] == result[2]:
                    bindsA.remove(bind)
                    break

        assert len(bindsA) == 0, \
               "Error: Missing rows from select."

        resultSets = myThread.dbi.processData(selectSQL, bindsB)

        assert len(resultSets) == 1, \
               "Error: Wrong number of ResultSets returned."

        results = resultSets[0].fetchall()

        assert len(results) == 2, \
               "Error: Wrong number of rows returned."

        for result in results:
            assert len(result) == 3, \
                   "Error: Wrong number of columns returned."
            for bind in bindsB:
                if bind["one"] == result[0] and bind["two"] == result[1] and \
                   bind["three"] == result[2]:
                    bindsB.remove(bind)
                    break

        assert len(bindsB) == 0, \
               "Error: Missing rows from select."

        return

    def testProcessDataHugeBinds(self):
        """
        _testProcessDataHugeBinds_

        Verify that select and insert queries work with several thousand binds.
        """
        bindsA = []
        bindsB = []
        for i in range(3001):
            bindsA.append({"one": i, "two": i * 2, "three": str(i * 3)})

        for i in range(1501):
            bindsB.append({"one": (i + 1) * 2, "two": i, "three": str(i * 5)})

        insertSQL = "INSERT INTO test_tablea VALUES (:one, :two, :three)"
        selectSQL = \
          """SELECT column1, column2, column3 FROM test_tablea
             WHERE column1 = :one AND column2 = :two AND column3 = :three"""

        myThread = threading.currentThread()
        myThread.dbi.processData(insertSQL, binds = bindsA)
        myThread.dbi.processData(insertSQL, binds = bindsB)

        resultSets = myThread.dbi.processData(selectSQL, bindsA)
        results = []
        for resultSet in resultSets:
            results.extend(resultSet.fetchall())

        assert len(results) == 3001, \
               "Error: Wrong number of rows returned: %d" % len(results)

        for result in results:
            assert len(result) == 3, \
                   "Error: Wrong number of columns returned."
            for bind in bindsA:
                if bind["one"] == result[0] and bind["two"] == result[1] and \
                   bind["three"] == result[2]:
                    bindsA.remove(bind)
                    break

        assert len(bindsA) == 0, \
               "Error: Missing rows from select."

        resultSets = myThread.dbi.processData(selectSQL, bindsB)
        results = []
        for resultSet in resultSets:
            results.extend(resultSet.fetchall())

        assert len(results) == 1501, \
               "Error: Wrong number of rows returned."

        for result in results:
            assert len(result) == 3, \
                   "Error: Wrong number of columns returned."
            for bind in bindsB:
                if bind["one"] == result[0] and bind["two"] == result[1] and \
                   bind["three"] == result[2]:
                    bindsB.remove(bind)
                    break

        assert len(bindsB) == 0, \
               "Error: Missing rows from select."

        return

    def testInsertHugeNumber(self):
        """
        _testInsertHugeNumber_

        Verify that we can insert and select huge numbers.
        """
        insertSQL = "INSERT INTO test_bigcol VALUES(:val1)"
        selectSQL = "SELECT * FROM test_bigcol"

        bindsA = {"val1": 2012211901}
        bindsB = {"val1": 20122119010}

        myThread = threading.currentThread()
        myThread.dbi.processData(insertSQL, binds = bindsA)
        myThread.dbi.processData(insertSQL, binds = bindsB)

        resultSets = myThread.dbi.processData(selectSQL)
        results = []
        for resultSet in resultSets:
            for row in resultSet.fetchall():
                results.append(row[0])

        assert len(results) == 2, \
               "Error: Wrong number of results."
        assert bindsA["val1"] in results, \
               "Error: Value one is missing."
        assert bindsB["val1"] in results, \
               "Error: Value one is missing."

        return

if __name__ == "__main__":
    unittest.main()
