#!/usr/bin/python

from tickery.options import EndpointOptions

from twisted.internet import reactor, defer
from twisted.web import http, error

from txfluiddb.client import Endpoint, BasicCreds, Namespace

from hackernews.defaults import (HACKERNEWS_USERNAME,
    HACKERNEWS_TOP100_TAG_NAME, HACKERNEWS_RATIO_TAG_NAME)
from hackernews.endpoint import hackernewsPassword

class Options(EndpointOptions):
    pass


def report(result, msg):
    print msg
    return result

def ignorePreexistingErrror(failure, msg):
    failure.trap(error.Error)
    if int(failure.value.status) == http.PRECONDITION_FAILED:
        print 'Ignoring precondition error (encountered in %r)' % (msg,)
    else:
        return failure

def addCallbacks(d, msg):
    d.addCallbacks(report, ignorePreexistingErrror,
                   callbackArgs=(msg,), errbackArgs=(msg,))

@defer.inlineCallbacks
def create(endpoint, username):
    ns = Namespace(username)

    for name, desc in (
        (HACKERNEWS_TOP100_TAG_NAME, 'Hacker News top 100 Tweeter.'),
        (HACKERNEWS_RATIO_TAG_NAME, 'Hacker News top Tweeter followers %.'),
        ):
        d = ns.createTag(endpoint, name, desc, False)
        addCallbacks(d, 'Created tag %r.' % name)
        yield d
    

if __name__ == '__main__':

    def nok(failure):
        print 'Failed:', failure
        if hasattr(failure.value, 'response_headers'):
            foundFluidDBHeader = False
            for header in failure.value.response_headers:
                if header.startswith('x-fluiddb-'):
                    foundFluidDBHeader = True
                    print '\t%s: %s' % (
                        header, failure.value.response_headers[header][0])
            if not foundFluidDBHeader:
                print 'Headers: %r' % (failure.value.response_headers)
        else:
            return failure

    def stop(_):
        reactor.stop()

    options = Options()
    options.parseOptions()

    print 'Using endpoint', options['endpoint']
    creds = BasicCreds(HACKERNEWS_USERNAME, hackernewsPassword())
    endpoint = Endpoint(baseURL=options['endpoint'], creds=creds)
    d = create(endpoint, HACKERNEWS_USERNAME)
    d.addCallback(report, 'Done.')
    d.addErrback(nok)    
    d.addBoth(stop)
    reactor.run()
