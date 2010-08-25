#!/usr/bin/env python
"""
Extend the default sreg library to support permissions and DN for a user
"""

__revision__ = "$Id: cms_sreg.py,v 1.5 2010/01/28 17:55:27 metson Exp $"
__version__ = "$Revision: 1.5 $"

from openid.extensions.sreg import SRegRequest
from openid.extensions.sreg import SRegResponse
#from openid.extensions.sreg import sendSRegFields
from openid.extensions.sreg import data_fields
from openid.extensions.sreg import ns_uri
from openid.extensions.sreg import supportsSReg

# The data fields that are listed in the sreg spec
data_fields.update({'permissions': 'Permissions',
                    'dn': 'Distinguished Name'})


__all__ = [
    'SRegRequest',
    'SRegResponse',
#    'sendSRegFields',
    'data_fields',
    'ns_uri',
    'supportsSReg',
    ]
