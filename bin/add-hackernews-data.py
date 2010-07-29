#!/usr/bin/python

"""
Feed data to this script on stdin. Input needs to look like

username ratio

Where username is a Twitter username, and ratio is the percentage
of Hacker News readers that follow that Twitter user. The data from
http://bit.ly/aXjMTP that inspired this quick hack is available in
../data/top-100

"""

import sys

from tickery.options import EndpointOptions

from twisted.internet import reactor, defer, task
from twisted.web import client
from twisted.python import log

from txfluiddb.client import Endpoint, BasicCreds, Object, Tag

from hackernews.defaults import (HACKERNEWS_USERNAME,
    HACKERNEWS_TOP100_TAG_NAME, HACKERNEWS_RATIO_TAG_NAME)
from hackernews.endpoint import hackernewsPassword

MAX_SIMULTANEOUS_REQUESTS = 5
TICKERY_URI = 'http://tickery.net/api/screennames/'


class Options(EndpointOptions):
    pass

def report(result, msg):
    print msg
    return result

def create(endpoint, data):
    top100Tag = Tag(HACKERNEWS_USERNAME, HACKERNEWS_TOP100_TAG_NAME)
    ratioTag = Tag(HACKERNEWS_USERNAME, HACKERNEWS_RATIO_TAG_NAME)

    def ok(_, username):
        print 'Tagged %r.' % username

    def failed(failure, username):
        print 'Tagging %r failed: %s' % (username, failure)
        if hasattr(failure.value, 'response_headers'):
            foundFluidDBHeader = False
            for header in failure.value.response_headers:
                if header.startswith('x-fluiddb-'):
                    foundFluidDBHeader = True
                    print '\t%s: %s' % (
                        header, failure.value.response_headers[header][0])
            if not foundFluidDBHeader:
                print 'Headers: %r' % (failure.value.response_headers)
    
    def tagIt(objectId, ratio, rank):
        o = Object(objectId)
        return defer.gatherResults([o.set(endpoint, ratioTag, float(ratio)),
                                    o.set(endpoint, top100Tag, rank)])
    
    def makeJobs():
        for i, (username, ratio) in enumerate(data):
            d = client.getPage('%s/%s' % (TICKERY_URI, username))
            d.addCallback(tagIt, ratio, i + 1)
            d.addCallback(ok, username)
            d.addErrback(failed, username)
            yield d
            
    jobs = makeJobs()
    deferreds = []
    coop = task.Cooperator()
    for i in xrange(MAX_SIMULTANEOUS_REQUESTS):
        d = coop.coiterate(jobs)
        d.addErrback(log.err)
        deferreds.append(d)

    return defer.DeferredList(deferreds)
    

if __name__ == '__main__':

    def nok(failure):
        print 'Failed:', failure

    def stop(_):
        reactor.stop()

    options = Options()
    options.parseOptions()

    print 'Using FluidDB endpoint', options['endpoint']
    creds = BasicCreds(HACKERNEWS_USERNAME, hackernewsPassword())
    endpoint = Endpoint(baseURL=options['endpoint'], creds=creds)
    data = [L[:-1].split() for L in sys.stdin.readlines()]
    print 'Adding %s tags to %d Tweeters.' % (HACKERNEWS_USERNAME, len(data))
    d = create(endpoint, data)
    d.addCallback(report, 'Done.')
    d.addErrback(nok)    
    d.addBoth(stop)
    reactor.run()
