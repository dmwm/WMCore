#!/usr/bin/env python
"""
_Template_

Template class for all Step Template implementations to inherit and implement
the API

"""



class Template:
    """
    _Template_

    Base interface definition for any WMStep Template

    """

    def install(self, wmStep):
        """
        _install_

        Override this method to install the required attributes
        in the wmStep Instance provided

        """
        msg = "WMSpec.Steps.Template.install method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError, msg


    def helper(self, wmStep):
        """
        _helper_

        Wrap the wmStep instance in a helper class tailored to this particular
        step type

        """
        msg = "WMSpec.Steps.Template.helper method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError, msg

