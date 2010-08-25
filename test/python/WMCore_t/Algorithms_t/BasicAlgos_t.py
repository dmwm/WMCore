#!/usr/bin/env python
"""
_BasicAlgos_t_

Test class for Basic Algorithms
"""







import os
import unittest
import tempfile

#from WMCore.Algorithms.BasicAlgos import *
import WMCore.Algorithms.BasicAlgos as BasicAlgos





class testBasicAlgos(unittest.TestCase):
    """
    Test to see whether we can do Linux


    """



    def setUp(self):
        """
        Do nothing

        """


        return


    def tearDown(self):
        """
        Do nothing
        

        """


        return



    def test_tail(self):
        """
        _tail_

        Can we tail a file?
        """



        a = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nm\nn\no\np\n"

        f = open('tmpfile.tmp', 'w')
        f.write(a)
        f.close()



        self.assertEqual(BasicAlgos.tail('tmpfile.tmp', 10),
                         ['g\n', 'h\n', 'i\n', 'j\n', 'k\n',
                          'l\n', 'm\n', 'n\n', 'o\n', 'p\n'])

        self.assertEqual(BasicAlgos.tail('tmpfile.tmp', 2),
                         ['o\n', 'p\n'])


        os.remove('tmpfile.tmp')


        return




if __name__ == "__main__":
    unittest.main() 



        
