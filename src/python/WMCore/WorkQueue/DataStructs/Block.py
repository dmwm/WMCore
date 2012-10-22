"""
_Block_

A dictionary based object meant to represent a row in the active_block/block table.
"""




class Block(dict):
    """
    _Block_

    A dictionary based object meant to represent subset of dbs block.
    Which will just need for workQueue update.
    It contains the following keys:
      Name
      Size
      NumEvent
      NumFiles
    """
    def __init__(self, **args):
        """
        ___init___

        Initialize all attributes.
        """
        dict.__init__(self)

        self.setdefault("Name", None)
        self.setdefault("Size", None)
        self.setdefault("NumEvents", None)
        self.setdefault("NumFiles", None)
        self.update(args)

    @staticmethod
    def getBlock(blockInfo):
        """
        convert to the Block structure from db column format
        """
        block = Block()

        block["Name"] = blockInfo[0]["name"]
        block["NumFiles"] = blockInfo[0]["num_files"]
        block["NumEvents"] = blockInfo[0]["num_events"]
        block["Size"] = blockInfo[0]["block_size"]

        return block
