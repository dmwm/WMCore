#!/usr/bin/env python
"""
Tests for ConfigSectionTree
"""

from builtins import str

import unittest

from WMCore.WMSpec.ConfigSectionTree import ConfigSectionTree
from WMCore.WMSpec.ConfigSectionTree import TreeHelper
from WMCore.WMSpec.ConfigSectionTree import findTopNode, addNode


class ConfigSectionTreeTest(unittest.TestCase):
    """
    TestCase for ConfigSectionTree

    """
    def testA(self):
        """instantiation"""
        try:
            cst = ConfigSectionTree("node1")
        except Exception as ex:
            msg = "Error instantiating ConfigSectionTree:\n"
            msg += str(ex)
            self.fail(msg)

        try:
            helper = TreeHelper(cst)
        except Exception as ex:
            msg = "Error instantiating TreeHelper:\n"
            msg += str(ex)
            self.fail(msg)

    def testB(self):
        """building tree"""

        nodeNameOrder = []
        cst1 = TreeHelper(ConfigSectionTree("node1"))
        nodeNameOrder.append(cst1.name())

        cst2a = TreeHelper(ConfigSectionTree("node2a"))
        cst2b = TreeHelper(ConfigSectionTree("node2b"))
        cst2c = TreeHelper(ConfigSectionTree("node2c"))


        cst1.addNode(cst2a)
        nodeNameOrder.append(cst2a.name())
        cst1.addNode(cst2b)
        nodeNameOrder.append(cst2b.name())
        cst1.addNode(cst2c)
        nodeNameOrder.append(cst2c.name())


        self.assertEqual(
            cst1.listNodes(), nodeNameOrder)


        cst3a = TreeHelper(ConfigSectionTree("node3a"))
        cst3b = TreeHelper(ConfigSectionTree("node3b"))
        cst3c = TreeHelper(ConfigSectionTree("node3c"))
        cst3d = TreeHelper(ConfigSectionTree("node3d"))
        cst3e = TreeHelper(ConfigSectionTree("node3e"))
        cst3f = TreeHelper(ConfigSectionTree("node3f"))
        cst3g = TreeHelper(ConfigSectionTree("node3g"))
        cst3h = TreeHelper(ConfigSectionTree("node3h"))
        cst3i = TreeHelper(ConfigSectionTree("node3i"))

        cst2a.addNode(cst3a)
        nodeNameOrder.insert(nodeNameOrder.index(cst2a.name())+1,
                             cst3a.name())
        cst2a.addNode(cst3b)
        nodeNameOrder.insert(nodeNameOrder.index(cst3a.name())+1,
                             cst3b.name())
        cst2a.addNode(cst3c)
        nodeNameOrder.insert(nodeNameOrder.index(cst3b.name())+1,
                             cst3c.name())

        cst2b.addNode(cst3d)
        nodeNameOrder.insert(nodeNameOrder.index(cst2b.name())+1,
                             cst3d.name())
        cst2b.addNode(cst3e)
        nodeNameOrder.insert(nodeNameOrder.index(cst3d.name())+1,
                             cst3e.name())
        cst2b.addNode(cst3f)
        nodeNameOrder.insert(nodeNameOrder.index(cst3e.name())+1,
                             cst3f.name())

        cst2c.addNode(cst3g)
        nodeNameOrder.insert(nodeNameOrder.index(cst2c.name())+1,
                             cst3g.name())
        cst2c.addNode(cst3h)
        nodeNameOrder.insert(nodeNameOrder.index(cst3g.name())+1,
                             cst3h.name())
        cst2c.addNode(cst3i)
        nodeNameOrder.insert(nodeNameOrder.index(cst3h.name())+1,
                             cst3i.name())

        self.assertEqual(cst1.listNodes(), nodeNameOrder)

    def testC(self):
        """parent hunting"""




        node1 = ConfigSectionTree("node1")
        node2 = ConfigSectionTree("node2")
        node3 = ConfigSectionTree("node3")


        addNode(node1, node2)
        addNode(node2, node3)


        topNode = TreeHelper(findTopNode(node3))
        self.assertEqual(topNode.name(), "node1")

        topNode = TreeHelper(findTopNode(node2))
        self.assertEqual(topNode.name(), "node1")

        topNode = TreeHelper(findTopNode(node1))
        self.assertEqual(topNode.name(), "node1")

        helper = TreeHelper(node2)
        helper.setTopOfTree()

        # searching for the tree top above the set tree top
        # means you cant even find the tree.
        # not sure if this is a problem....
        topNode = TreeHelper(findTopNode(node1))
        self.assertEqual(topNode.name(), "node1")

        topNode = TreeHelper(findTopNode(node2))
        self.assertEqual(topNode.name(), "node2")
        topNode = TreeHelper(findTopNode(node3))
        self.assertEqual(topNode.name(), "node2")









if __name__ == '__main__':
    unittest.main()
