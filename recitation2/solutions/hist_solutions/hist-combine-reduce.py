#!/usr/bin/env python
import sys
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)

count = 0
currentKey = None

for line in sys.stdin:
    line = line.strip()
    key, pcount = line.split('\t') # read stdin and get (key, pcount)

    if currentKey == key:
        count = count + int(pcount) # if same key encountered in consecutive reads increase count
    else:
        if currentKey is not None:
            print '%s\t%d' % (currentKey, count)
        count = int(pcount)  # set count and currentKey
        currentKey = key

if currentKey is not None:
    print '%s\t%d' % (currentKey, count)
