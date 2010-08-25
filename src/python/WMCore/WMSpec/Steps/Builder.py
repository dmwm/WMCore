#!/usr/bin/env python
"""
_Builder_

Interface definition for Step Builder implementations

"""


class Builder:
    """
    _Builder_

    base interface for any WMStep Builder

    """

    def build(self, step, workingDirectory, **args):
        """
        _build_

        Build the step into the working area provided
        args is for all the things we havent thought of yet

        """
        msg = "WMSpec.Steps.Builder.build method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError, msg


    def installWorkingArea(self, step, workingArea, **args):
        """
        _installWorkingArea_

        Install working directory information into the step in a standard
        way.

        """
        step.section_("builder")
        step.builder.workingDir = workingArea
        return
