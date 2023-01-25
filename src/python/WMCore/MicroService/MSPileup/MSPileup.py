"""
File       : MSPileup.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileup provides logic behind the pileup WMCore module.
"""

# WMCore modules
from WMCore.MicroService.MSCore.MSCore import MSCore


class MSPileup(MSCore):
    """
    MSPileup provides whole logic behind the pileup WMCore module.
    """

    def __init__(self, msConfig, logger=None, **kwargs):
        super(MSPileup, self).__init__(msConfig, **kwargs)
