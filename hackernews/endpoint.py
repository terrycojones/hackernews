import os

from txfluiddb.client import Endpoint, BasicCreds

from hackernews.defaults import HACKERNEWS_USERNAME, HACKERNEWS_PASSWORD_VAR

def hackernewsPassword():
    try:
        return os.environ[HACKERNEWS_PASSWORD_VAR]
    except KeyError:
        raise Exception('Please set %r in your environment.' %
                        HACKERNEWS_PASSWORD_VAR)

def hackernewsEndpoint(URL):
    return Endpoint(baseURL=URL,
                    creds=BasicCreds(HACKERNEWS_USERNAME,
                                     hackernewsPassword()))
