from WMCore.WebTools.RESTModel import RESTModel
from cherrypy import response, request, HTTPError

class NestedModel(RESTModel):
    """
    A RESTModel that can also be used as a container for other RESTModels
    """

    def handler(self, verb, args=[], kwargs={}):
        """
        Call the appropriate method from self.methods for a given VERB. kwargs
        are query string parameters (e.g. method?thing1=abc&thing2=def).

        args are URI path elements, the first (arg[0]) is the base method name,
        other elements (arg[1:]) are either positional arguments to the base
        method or further components of the URI path, pointing at other methods.

        The default method should have a small number of arguments (preferably
        none). Any arguments passed to method beyond that number are checked to
        be path components, e.g. other method names.
        """
        verb = verb.upper()
        self._classifyHTTPError(verb, args, kwargs)
        args = list(args)
        basemethnom = args[0]
        basemethod = self.methods[verb][basemethnom]
        children = list(self.methods[verb][basemethnom])
        method = children.pop(children.index('default'))
        try:
            # is there a method in the keywords?
            for a in kwargs:
                if a in children:
                    method = a
                    if not len(kwargs[a]):
                        kwargs.pop(a)
            # is there a method in the positional args?
            for a in args[1:]:
                if a in children:
                    method = args.pop(args.index(a))

            data = basemethod[method]['call'](*args[1:], **kwargs)
        # in case sanitise_input is not called with in the method, if args doesn't
        # match throws the 400 error
        except TypeError as e:
            raise HTTPError(400, str(e))

        if 'expires' in basemethod[method]:
            return data, basemethod[method]['expires']
        else:
            return data, False
