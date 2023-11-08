#! /usr/bin/env python

import contextlib
import os


@contextlib.contextmanager
def tmpEnv(**environ):
    """
    Temporarily set the process environment variables.

    >>> with tmpEnv(PLUGINS_DIR=u'test/plugins'):
    ...   "PLUGINS_DIR" in os.environ
    True

    >>> "PLUGINS_DIR" in os.environ
    False

    :param environ: Environment variables to set
    """
    oldEnviron = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(oldEnviron)
