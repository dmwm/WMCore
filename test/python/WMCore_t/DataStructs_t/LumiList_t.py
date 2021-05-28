#! /usr/bin/env python

from builtins import zip, str, range
from future.utils import viewitems

import unittest

# import FWCore.ParameterSet.Config as cms
from WMCore.DataStructs.LumiList import LumiList


class LumiListTest(unittest.TestCase):
    """
    _LumiListTest_

    """

    def setUp(self):
        jsonFile = open('lumiTest.json', 'w')
        jsonFile.write('{"1": [[1, 33], [35, 35], [37, 47]], "2": [[49, 75], [77, 130], [133, 136]]}')
        jsonFile.close()

    def notestRead(self):
        """
        Test reading from JSON
        """
        exString = "1:1-1:33,1:35,1:37-1:47,2:49-2:75,2:77-2:130,2:133-2:136"
        exDict = {'1': [[1, 33], [35, 35], [37, 47]],
                  '2': [[49, 75], [77, 130], [133, 136]]}
        exVLBR = cms.VLuminosityBlockRange('1:1-1:33', '1:35', '1:37-1:47', '2:49-2:75', '2:77-2:130', '2:133-2:136')

        jsonList = LumiList(filename='lumiTest.json')
        lumiString = jsonList.getCMSSWString()
        lumiList = jsonList.getCompactList()
        lumiVLBR = jsonList.getVLuminosityBlockRange(True)

        self.assertTrue(lumiString == exString)
        self.assertTrue(lumiList == exDict)
        self.assertTrue(lumiVLBR == exVLBR)

    def testList(self):
        """
        Test constructing from list of pairs
        """

        listLs1 = list(range(1, 34)) + [35] + list(range(37, 48))
        listLs2 = list(range(49, 76)) + list(range(77, 131)) + list(range(133, 137))
        lumis = list(zip([1] * 100, listLs1)) + list(zip([2] * 100, listLs2))

        jsonLister = LumiList(filename='lumiTest.json')
        jsonString = jsonLister.getCMSSWString()
        jsonList = jsonLister.getCompactList()

        pairLister = LumiList(lumis=lumis)
        pairString = pairLister.getCMSSWString()
        pairList = pairLister.getCompactList()

        self.assertTrue(jsonString == pairString)
        self.assertTrue(jsonList == pairList)

    def testRuns(self):
        """
        Test constructing from run and list of lumis
        """
        runsAndLumis = {
            1: list(range(1, 34)) + [35] + list(range(37, 48)),
            2: list(range(49, 76)) + list(range(77, 131)) + list(range(133, 137))
        }
        runsAndLumis2 = {
            '1': list(range(1, 34)) + [35] + list(range(37, 48)),
            '2': list(range(49, 76)) + list(range(77, 131)) + list(range(133, 137))
        }
        blank = {
            '1': [],
            '2': []
        }

        jsonLister = LumiList(filename='lumiTest.json')
        jsonString = jsonLister.getCMSSWString()
        jsonList = jsonLister.getCompactList()

        runLister = LumiList(runsAndLumis=runsAndLumis)
        runString = runLister.getCMSSWString()
        runList = runLister.getCompactList()

        runLister2 = LumiList(runsAndLumis=runsAndLumis2)
        runList2 = runLister2.getCompactList()

        runLister3 = LumiList(runsAndLumis=blank)

        self.assertTrue(jsonString == runString)
        self.assertTrue(jsonList == runList)
        self.assertTrue(runList2 == runList)
        self.assertTrue(len(runLister3) == 0)

    def testFilter(self):
        """
        Test filtering of a list of lumis
        """
        runsAndLumis = {
            1: list(range(1, 34)) + [35] + list(range(37, 48)),
            2: list(range(49, 76)) + list(range(77, 131)) + list(range(133, 137))
        }

        completeList = list(zip([1] * 150, list(range(1, 150)))) + \
                       list(zip([2] * 150, list(range(1, 150)))) + \
                       list(zip([3] * 150, list(range(1, 150))))

        smallList = list(zip([1] * 50, list(range(1, 10)))) + list(zip([2] * 50, list(range(50, 70))))
        overlapList = list(zip([1] * 150, list(range(30, 40)))) + \
                      list(zip([2] * 150, list(range(60, 80))))
        overlapRes = list(zip([1] * 9, list(range(30, 34)))) + [(1, 35)] + \
                     list(zip([1] * 9, list(range(37, 40)))) + \
                     list(zip([2] * 30, list(range(60, 76)))) + \
                     list(zip([2] * 9, list(range(77, 80))))

        runLister = LumiList(runsAndLumis=runsAndLumis)

        # Test a list to be filtered which is a superset of constructed list
        filterComplete = runLister.filterLumis(completeList)
        # Test a list to be filtered which is a subset of constructed list
        filterSmall = runLister.filterLumis(smallList)
        # Test a list to be filtered which is neither
        filterOverlap = runLister.filterLumis(overlapList)

        self.assertTrue(filterComplete == runLister.getLumis())
        self.assertTrue(filterSmall == smallList)
        self.assertTrue(filterOverlap == overlapRes)

    def testDuplicates(self):
        """
        Test a list with lots of duplicates
        """
        result = list(zip([1] * 100, list(range(1, 34)) + list(range(37, 48))))
        lumis = list(zip([1] * 100, list(range(1, 34)) + list(range(37, 48)) + list(range(5, 25))))

        lister = LumiList(lumis=lumis)
        self.assertTrue(lister.getLumis() == result)

    def testNull(self):
        """
        Test a null list
        """

        runLister = LumiList(lumis=None)

        self.assertTrue(runLister.getCMSSWString() == '')
        self.assertTrue(runLister.getLumis() == [])
        self.assertTrue(runLister.getCompactList() == {})

    def testSubtract(self):
        """
        a-b for lots of cases
        """

        alumis = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)),
                  '2': list(range(6, 20)) + list(range(30, 40)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                  }
        blumis = {'1': list(range(1, 6)) + list(range(12, 13)) + list(range(16, 30)) + list(range(40, 50)) + list(
            range(33, 36)),
                  '2': list(range(10, 35)),
                  '3': list(range(10, 15)) + list(range(35, 40)) + list(range(45, 51)) + list(range(59, 70)),
                  }
        clumis = {'1': list(range(1, 6)) + list(range(12, 13)) + list(range(16, 30)) + list(range(40, 50)) + list(
            range(33, 36)),
                  '2': list(range(10, 35)),
                  }
        result = {'1': list(range(6, 12)) + list(range(13, 16)) + list(range(31, 33)) + list(range(36, 39)),
                  '2': list(range(6, 10)) + list(range(35, 40)),
                  '3': list(range(15, 20)) + list(range(30, 35)) + list(range(51, 59)),
                  }
        result2 = {'1': list(range(6, 12)) + list(range(13, 16)) + list(range(31, 33)) + list(range(36, 39)),
                   '2': list(range(6, 10)) + list(range(35, 40)),
                   '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                   }
        a = LumiList(runsAndLumis=alumis)
        b = LumiList(runsAndLumis=blumis)
        c = LumiList(runsAndLumis=clumis)
        r = LumiList(runsAndLumis=result)
        r2 = LumiList(runsAndLumis=result2)

        self.assertTrue((a - b).getCMSSWString() == r.getCMSSWString())
        self.assertTrue((a - b).getCMSSWString() != (b - a).getCMSSWString())
        # Test where c is missing runs from a
        self.assertTrue((a - c).getCMSSWString() == r2.getCMSSWString())
        self.assertTrue((a - c).getCMSSWString() != (c - a).getCMSSWString())
        # Test empty lists
        self.assertTrue(str(a - a) == '{}')
        self.assertTrue(len(a - a) == 0)

    def testOr(self):
        """
        a|b for lots of cases
        """

        alumis = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)),
                  '2': list(range(6, 20)) + list(range(30, 40)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                  }
        blumis = {'1': list(range(1, 6)) + list(range(12, 13)) + list(range(16, 30)) + list(range(40, 50)) + list(
            range(39, 80)),
                  '2': list(range(10, 35)),
                  '3': list(range(10, 15)) + list(range(35, 40)) + list(range(45, 51)) + list(range(59, 70)),
                  }
        clumis = {'1': list(range(1, 6)) + list(range(12, 13)) + list(range(16, 30)) + list(range(40, 50)) + list(
            range(39, 80)),
                  '2': list(range(10, 35)),
                  }
        result = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)) + list(range(1, 6)) + list(
            range(12, 13)) + list(range(16, 30)) + list(range(40, 50)) + list(range(39, 80)),
                  '2': list(range(6, 20)) + list(range(30, 40)) + list(range(10, 35)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)) + list(range(10, 15)) + list(
                      range(35, 40)) + list(range(45, 51)) + list(range(59, 70)),
                  }
        a = LumiList(runsAndLumis=alumis)
        b = LumiList(runsAndLumis=blumis)
        c = LumiList(runsAndLumis=blumis)
        r = LumiList(runsAndLumis=result)
        self.assertTrue((a | b).getCMSSWString() == r.getCMSSWString())
        self.assertTrue((a | b).getCMSSWString() == (b | a).getCMSSWString())
        self.assertTrue((a | b).getCMSSWString() == (a + b).getCMSSWString())

        # Test list constuction (faster)

        multiple = [alumis, blumis, clumis]
        easy = LumiList(runsAndLumis=multiple)
        hard = a + b
        hard += c
        self.assertTrue(hard.getCMSSWString() == easy.getCMSSWString())

    def testAnd(self):
        """
        a&b for lots of cases
        """

        alumis = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)),
                  '2': list(range(6, 20)) + list(range(30, 40)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                  '4': list(range(1, 100)),
                  }
        blumis = {'1': list(range(1, 6)) + list(range(12, 13)) + list(range(16, 25)) + list(range(25, 40)) + list(
            range(40, 50)) + list(range(33, 36)),
                  '2': list(range(10, 35)),
                  '3': list(range(10, 15)) + list(range(35, 40)) + list(range(45, 51)) + list(range(59, 70)),
                  '5': list(range(1, 100)),
                  }
        result = {'1': list(range(2, 6)) + list(range(12, 13)) + list(range(16, 20)) + list(range(31, 39)) + list(
            range(45, 49)),
                  '2': list(range(10, 20)) + list(range(30, 35)),
                  '3': list(range(10, 15)) + list(range(35, 40)) + list(range(50, 51)) + list(range(59, 60)),
                  }
        a = LumiList(runsAndLumis=alumis)
        b = LumiList(runsAndLumis=blumis)
        r = LumiList(runsAndLumis=result)
        self.assertTrue((a & b).getCMSSWString() == r.getCMSSWString())
        self.assertTrue((a & b).getCMSSWString() == (b & a).getCMSSWString())
        self.assertTrue((a | b).getCMSSWString() != r.getCMSSWString())

    def testRemoveSelect(self):
        """
        a-b on runs for lots of cases
        """

        alumis = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)),
                  '2': list(range(6, 20)) + list(range(30, 40)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                  '4': list(range(10, 20)) + list(range(30, 80)),
                  }

        result = {'2': list(range(6, 20)) + list(range(30, 40)),
                  '4': list(range(10, 20)) + list(range(30, 80)),
                  }

        rem = LumiList(runsAndLumis=alumis)
        sel = LumiList(runsAndLumis=alumis)
        res = LumiList(runsAndLumis=result)

        rem.removeRuns([1, 3])
        sel.selectRuns([2, 4])

        self.assertTrue(rem.getCMSSWString() == res.getCMSSWString())
        self.assertTrue(sel.getCMSSWString() == res.getCMSSWString())
        self.assertTrue(sel.getCMSSWString() == rem.getCMSSWString())

    def testURL(self):
        URL = 'https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions12/8TeV/Reprocessing/Cert_190456-195530_8TeV_08Jun2012ReReco_Collisions12_JSON.txt'
        ll = LumiList(url=URL)
        self.assertTrue(len(ll) > 0)

    def testWrite(self):
        alumis = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)),
                  '2': list(range(6, 20)) + list(range(30, 40)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                  '4': list(range(1, 100)),
                  }
        a = LumiList(runsAndLumis=alumis)
        a.writeJSON('newFile.json')

    def testCompact(self):
        acl = {'1': [[1, 2], [3, 4], [8, 9]]}
        bcl = {'1': [[8, 9], [3, 4], [1, 2]]}
        ccl = {'1': [[1, 4], [8, 9]]}
        dcl = {'1': [[1, 4], [2, 3], [8, 9]]}

        self.assertEqual(LumiList(compactList=acl).getCMSSWString(), LumiList(compactList=bcl).getCMSSWString())
        self.assertEqual(LumiList(compactList=acl).getCMSSWString(), LumiList(compactList=ccl).getCMSSWString())
        self.assertEqual(LumiList(compactList=acl).getCMSSWString(), LumiList(compactList=dcl).getCMSSWString())

    def testWMagentFormat(self):
        """
        Test the constuction of the double-string-based version of lumi lists
        used in some places by WMAgent
        """

        clumis = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)),
                  '2': list(range(6, 20)) + list(range(30, 40)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                  '4': list(range(1, 100)),
                  }
        runs = [1, 2, 3, 4]
        lumis = ['2,19,31,38,45,48', '6,19,30,39', '10,19,30,39,50,59', '1,99']

        c = LumiList(runsAndLumis=clumis)
        w = LumiList(wmagentFormat=(runs, lumis))
        self.assertEqual(w.getCMSSWString(), c.getCMSSWString())

        clumis = {'12': [4, 5]}
        runs = [12]
        lumis = ['4,5']

        c = LumiList(runsAndLumis=clumis)
        w = LumiList(wmagentFormat=(runs, lumis))
        self.assertEqual(w.getCMSSWString(), c.getCMSSWString())

        aruns = [1]
        alumis = ['1,10,11,20']
        bruns = [1]
        blumis = ['1,20']
        self.assertEqual(LumiList(wmagentFormat=(aruns, alumis)).getCMSSWString(),
                         LumiList(wmagentFormat=(bruns, blumis)).getCMSSWString())

        with self.assertRaises(RuntimeError):
            w = LumiList(wmagentFormat=([1, 2, 3]))  # No lumis
        with self.assertRaises(RuntimeError):
            w = LumiList(wmagentFormat=([1], ['1,2,3']))  # Need twice as many lumis as runs

    def testLumisWithEvents(self):
        """Make sure that runs and lumis with event counts as used in CRAB3 works
        """
        clumis = {'1': list(range(2, 20)) + list(range(31, 39)) + list(range(45, 49)),
                  '2': list(range(6, 20)) + list(range(30, 40)),
                  '3': list(range(10, 20)) + list(range(30, 40)) + list(range(50, 60)),
                  '4': list(range(1, 100)),
                  }
        # create a lumilist like {'21' : {'2':None ...} ...}
        lumis = {}
        for run, ls in viewitems(clumis):
            newrun = str(int(run) + 20)
            lumis[newrun] = {str(l): None for l in ls}

        c1 = LumiList(runsAndLumis=clumis)
        w1 = LumiList(runsAndLumis=lumis)
        c1 += w1

        c2 = LumiList(runsAndLumis=clumis)
        w2 = LumiList(runsAndLumis=lumis)
        w2 += c2

        self.assertEqual(c1.getCMSSWString(), w2.getCMSSWString())


if __name__ == '__main__':
    unittest.main()
