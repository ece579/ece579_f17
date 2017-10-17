#!/usr/bin/env python
import sys
import numpy

### Prevents pipe IO errors for large files.
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL)
###

# read parameters from distributed cache - nbins, minmeanmax

f = open('nbins','r') # read nbins file
params = f.readline().strip().split() # read nbins file
nbins = int(params[0]) # set maximum number of bins
f.close()

f = open('mmm','r') # open mmm file

'''
Read mmm file and assign min, max and mean values.
File read operation in similar to above reading procedure.
'''
params =  f.readline().strip().split() 
xmax = float(params[1])
params = f.readline().strip().split()
xmean = float(params[1])
params = f.readline().strip().split() 
xmin = float(params[1])
f.close()


'''
Bin width = (max - min) / total bins
Calculate bin width using formula
'''
dx = (xmax-xmin)/nbins # bin width calculation 


# compute bin centre
def binCentre(xmin, dx, value):
    binno = int((value - xmin)/dx)
    bincentre = binno*dx + xmin + dx/2.0
    return bincentre


# process input data
for line in sys.stdin:
    line = line.strip()

    words = line.split()
    xi = float(words[0])
    bincentre = binCentre(xmin, dx, xi) # assign bincentre for each value

    print '%f\t%d' % (bincentre, 1) # make (key, value) pairs

    #sys.stdout.write('%f\t%d' % (bincentre, 1))
