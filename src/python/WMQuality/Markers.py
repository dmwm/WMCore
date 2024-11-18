import os

import pytest

noproxy = pytest.mark.skipif(
    os.environ.get('X509_USER_PROXY', None),
    reason='Only run if an X509 proxy is present'
)
