#!/usr/bin/env python

import os, sys, datetime, optparse

def filesize(size):
    """Accept values in multiple size ranges from peta, tera, gig, mega
    and return the translated value in megabytes"""
    try:
        digits = int(filter(str.isdigit, size))
        factor = filter(str.isalpha, size).lower()
        scalar = ['m', 'g', 't', 'p', 'e']
        if factor in scalar:
            return digits * 1024**scalar.index(factor)
        else:
            return 0 #digits
    except (ValueError), e:
        return 0

def unitoftime(tslice):
    """Accept values in various time representations and return conversion to seconds"""
    try:
        digits = int(filter(str.isdigit, tslice))
        factor = filter(str.isalpha, tslice).lower()
        scalar = { 's': 1, 'm': 60, 'h': 3600, 'd':86400, 'w': 604800 }
        if scalar.has_key(factor):
            return digits * scalar.get(factor,1)
        else:
            return 0
    except (ValueError), e:
        return 0

def humantime(s):
    """seconds to a readable human format"""
    return str(datetime.timedelta(seconds=int(s)))

def hadoopcalc(gigs=0, mbps=0, nodes=0, seconds=0):
    """Summarized Equation := (gigs * 1024) * 8 / (mbps * nodes) = seconds
                           :=  seconds * (mbps * nodes) / 8192   = gigs
		           :=  gigs * 8192 / seconds / mbps      = nodes
		           :=  gigs * 8192 / seconds / nodes     = mbps"""
    if (mbps and nodes and seconds) and not gigs:
        # Solve for gigs
        return seconds * (mbps * nodes) / 8192
    elif (gigs and nodes and seconds) and not mbps:
        # Calculate aggregate throughput
        return gigs * 8192 / seconds / nodes
    elif (gigs and mbps and seconds) and not nodes:
        # Return number of nodes required
        return gigs * 8192 / seconds / mbps
    elif (gigs and mbps and nodes) and not seconds:
        # see how long it takes to process
        return (gigs * 8192) / (mbps * nodes)
    else:
        raise Exception, "Need to specify 3 of the 4 parameters of gigs, mbps, nodes or seconds. No more, no less."

if __name__ == '__main__':
    
    from optparse import OptionParser
    USAGE = """Usage: %s <options>
       filesizes can be in exa(e), peta(p), tera(t), gig(g), mega(m), kilo(k).
         E.g. 2t or 250g
       Or you may specify time units and you will then get estimates on how much data you can churn in that timeframe.
       Please abbreviate common units to a single letters: second=s, minute=m, hour=h, day=d, week=w
         E.g. 2h or 2d""" % \
            (os.path.basename(sys.argv[0]))
    parser = OptionParser(usage=USAGE)
    parser.add_option("-n", "--nodes", dest="nodes", default=10,
                      help="number of nodes in your cluster")
    parser.add_option("-c", "--copies", dest="copies", default=3,
                      help="number of copies made per file in your HDFS")
    parser.add_option("-m", "--mbps", dest="mbps", default=1000,
                      help="network speed measured in Mbps")
    parser.add_option("-t", "--throughput", dest="tput", default=150,
                      help="aggregate throughput for cluster processing in Mbps")

    (opt, args) = parser.parse_args()

    total_mb = sum(map(filesize, args))
    total_s  = sum(map(unitoftime, args))
    print 'DEBUG: total_mb = %d, total_s = %d' % (total_mb, total_s)
    
    if total_mb:
        print 'In theory, you should be able to transfer your %.2f Gigs of data in %s' % \
            (total_mb / 1024.0, humantime(total_mb / int(opt.nodes) * int(opt.copies) * 8 / int(opt.mbps)))

        print '...but more importantly...'

        print 'Given your aggregate throughput of %s Mbps, representing cluster hardware and mapreduce job:\nYou should be able to process your %d Gigs of data in %s' \
            % (opt.tput, total_mb / 1024, humantime(total_mb * 8 / (int(opt.tput) * int(opt.nodes))))

    if total_s:
        print 'In theory, you should be able to transfer %.2f Gigs in the specified time of %s' % \
            (total_s * int(opt.mbps) * int(opt.nodes) / (int(opt.copies) * 8) / 1024, humantime(total_s))

        print '...but more importantly...'

        print 'Given your aggregate throughput of %s Mbps, representing cluster hardware and mapreduce job:\nYou should be able to process %d Gigs of data in your specified %s amount of time.' \
            % (opt.tput, total_s * int(opt.tput) * int(opt.nodes) / 8192, humantime(total_s))
