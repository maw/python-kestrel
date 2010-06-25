#!/usr/bin/env python

import kestrel

k = kestrel.connection(["localhost:22133"], "this-should-be-empty")

print "After about two seconds, I should print 'None'"
x = k.dequeue(timeout=2000)
print x
