"""
File       : MSPileup.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileup class provide whole logic behind
the pileup WMCore module.
"""

# WMCore modules
from WMCore.MicroService.MSCore.MSCore import MSCore


class MSPileup(MSCore):
    """
    MSPileup class provide whole logic behind
    the pileup WMCore module.
    """

    def __init__(self, msConfig, logger=None, **kwargs):
        super(MSPileup, self).__init__(msConfig, **kwargs)
