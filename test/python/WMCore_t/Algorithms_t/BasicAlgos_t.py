#!/usr/bin/env python
"""
_BasicAlgos_t_

Test class for Basic Algorithms
"""

import os
import hashlib
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

    def test_MD5(self):
        """
        _MD5_

        Check if we can create an MD5 checksum
        """

        silly = "This is a rather ridiculous string"
        filename = '/tmp/md5test.test'

        f = open(filename, 'w')
        f.write(silly)
        f.close()

        self.assertEqual(BasicAlgos.getMD5(filename = filename),
                         hashlib.md5(silly).hexdigest())

        os.remove(filename)
        return


if __name__ == "__main__":
    unittest.main() 



        
