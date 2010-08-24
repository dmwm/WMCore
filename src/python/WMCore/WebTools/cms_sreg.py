#!/usr/bin/env python
"""
Extend the default sreg library to support permissions and DN for a user
"""




# All these imports are just to set __all__ correctly which unfortunately seems
# necessary. Really this is a passthrough for the sreg code from the openid 
# package
# pylint: disable-msg=W0611

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
