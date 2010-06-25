#!/usr/bin/env python

import kestrel

k = kestrel.connection(["localhost:22133"], "python-kestrel-test")

max = 500

mod = int(0.13 * max)
print "we'll fake an aborted message every %d tries or so" % mod

for i in xrange(0, max):
    m = "message #%d" % i
    k.enqueue(m)
    pass

print "messages all queued up; last message was: '%s'" % m

total = 0
counter = 0 


# for i in xrange(0, max):

while True:
    l = k.dequeue()
    
    #print l
    if l == None:
        break
    last = l
    if counter % mod == 0:
        print "faked aborted read at #%d" % counter
        k.dequeue_abort()
    else:
        k.dequeue_finish()
        total += 1
        pass
    
    counter += 1
    pass

print "we got back %d messages in %d tries" % (total, counter)
print "last message was: '%s'" % last
