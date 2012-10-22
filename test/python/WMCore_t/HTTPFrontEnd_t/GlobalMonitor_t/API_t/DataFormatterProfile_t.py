import unittest
import cProfile
import pstats

from WMCore.HTTPFrontEnd.GlobalMonitor.API.DataFormatter \
    import combineListOfDict, add, addToList


class DataFormatterProfileTest(unittest.TestCase):
    """
    """

    def setUp(self):
        self.data = []
        for i in range(1000):
            self.data.append({'name': 'test_%s' % i, 'size': 1, 'list': [i],
                          'error_key': "error_key"})

    def createProfile(self, name, function):
        file = name
        prof = cProfile.Profile()
        prof.runcall(function)
        prof.dump_stats(file)
        p = pstats.Stats(file)
        p.strip_dirs().sort_stats('cumulative').print_stats(0.1)
        p.strip_dirs().sort_stats('time').print_stats(0.1)
        p.strip_dirs().sort_stats('calls').print_stats(0.1)
        #p.strip_dirs().sort_stats('name').print_stats(10)

    def testCombineListOfDict(self):
        self.createProfile('combineListOfDict.prof', self.fastAlgo)

    def fastAlgo(self):
        combineListOfDict('name', self.data, self.data,
                                errorKey = 'error_key',
                                size = add, list = addToList)

if __name__ == "__main__":
    unittest.main()
