#!/usr/bin/env python
"""
_BossLiteDBInterface_

"""

__version__ = "$Id: BossLiteDBInterface.py,v 1.1 2010/05/03 13:01:32 spigafi Exp $"
__revision__ = "$Revision: 1.1 $"


class BossLiteDBInterface(object):
    """
    _BossLiteDBInterface_
    
    This class is *strongly* specialized to use WMCore DB back-end
    """
    
    engine = None
    
    ##########################################################################

    def __init__(self):
        """
        __init__
        """
        

    ##########################################################################
    # Methods for BossLiteAPI
    ##########################################################################
    
    def insert(self, obj):
        """
        put your description here...
        """
        
        raise NotImplementedError

    ##########################################################################

    def select(self, template, strict = True):
        """
        put your description here...
        """
        
        raise NotImplementedError

    ##########################################################################

    def update(self, template, skipAttributes = None):
        """
        put your description here...
        """
        
        raise NotImplementedError

    ##########################################################################

    def delete(self, template):
        """
        put your description here...
        """
        
        raise NotImplementedError
     
    ##########################################################################
    # Methods for DbObjects
    ##########################################################################
    
    def objExists(self, obj):
        """
        put your description here...
        """
        
        raise NotImplementedError
        
    ##########################################################################
    
    def objSave(self, obj):
        """
        put your description here...
        """
        
        raise NotImplementedError
        
    ##########################################################################
    
    def objCreate(self, obj):
        """
        put your description here...
        """
        
        raise NotImplementedError   
        
    ##########################################################################
    
    def objLoad(self, obj):
        """
        put your description here...
        """
        
        raise NotImplementedError      
        
    ##########################################################################
    
    def objUpdate(self, obj):
        """
        put your description here...
        """
        
        raise NotImplementedError     
        
    ##########################################################################
    
    def objRemove(self, obj):
        """
        put your description here...
        """
        
        raise NotImplementedError  
