#!/usr/bin/env python
"""
_DBS3Buffer.CreateBlocks_

Create new block in dbsbuffer_block
"""

from WMComponent.DBS3Buffer.MySQL.CreateBlocks import CreateBlocks as MySQLCreateBlocks

class CreateBlocks(MySQLCreateBlocks):

    sql = """INSERT INTO dbsbuffer_block
             (id, dataset_id, blockname, location, status, create_time)
             SELECT dbsbuffer_block_seq.nextval,
                    (SELECT id from dbsbuffer_dataset WHERE path = :dataset),
                    :block,
                    (SELECT id FROM dbsbuffer_location WHERE pnn = :location),
                    :status,
                    :time
             FROM DUAL
             """
