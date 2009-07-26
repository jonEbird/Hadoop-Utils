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
            return digits
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

def hadoopcalc(megs=0, mbps=0, nodes=0, seconds=0):
    """Summarized Equation := (gigs * 1024) * 8 / (mbps * nodes) = seconds
                           :=  seconds * (mbps * nodes) / 8192   = gigs
		           :=  gigs * 8192 / seconds / mbps      = nodes
		           :=  gigs * 8192 / seconds / nodes     = mbps"""
    if (mbps and nodes and seconds) and not megs:
        return seconds * (mbps * nodes) * 1024 / 8192  #= megs
    elif (megs and nodes and seconds) and not mbps:
        return megs * 8 / seconds / nodes              #= aggregate throughput (mbps)
    elif (megs and mbps and seconds) and not nodes:
        return megs * 8 / seconds / mbps               #= nodes
    elif (megs and mbps and nodes) and not seconds:
        return megs * 8 / mbps / nodes                 #= seconds
    else:
        raise Exception, "Need to specify 3 of the 4 parameters of megs, mbps, nodes or seconds. No more, no less."

if __name__ == '__main__':
    
    from optparse import OptionParser
    USAGE = """Usage: %prog <options> gigs|mbps|nodes|seconds
       Solve for one of the four variables while specifying the other three."""

    parser = OptionParser(usage=USAGE)
    parser.add_option("-d", "--data", dest="data", default="100g",
                      help="Amount of data we're talking about. Filesizes can be in exa(e), peta(p), tera(t), gig(g), mega(m), kilo(k)")
    parser.add_option("-m", "--mbps", dest="mbps", default=150,
                      help="aggregate throughput for cluster processing in Mbps")
    parser.add_option("-n", "--nodes", dest="nodes", default=10,
                      help="number of nodes in your cluster")
    parser.add_option("-t", "--time", dest="time", default="30m",
                      help="How much time are you giving us? Time can be in second(s), minute(m), hour(h), day(d), week(w)")
    parser.add_option("-q", "--quick", dest="quick", action="store_true", default=False,
                      help="Quickly and quietly, just give me the number without the fluff")

    (opt, args) = parser.parse_args()

    if not args or not args[0] in ['gigs', 'mbps', 'nodes', 'seconds']:
        parser.print_help()
        sys.exit(1)

    # gather our defaults
    defs = {
        'megs': filesize(opt.data),
        'mbps': int(opt.mbps),
        'nodes': int(opt.nodes),
        'seconds': unitoftime(opt.time) }

    if args[0] == 'gigs':
        defs['megs'] = hadoopcalc(mbps=defs['mbps'], nodes=defs['nodes'], seconds=defs['seconds'])
    elif args[0] == 'mbps':
        defs['mbps'] = hadoopcalc(megs=defs['megs'], nodes=defs['nodes'], seconds=defs['seconds'])
    elif args[0] == 'nodes':
        defs['nodes'] = hadoopcalc(megs=defs['megs'], mbps=defs['mbps'], seconds=defs['seconds'])
    elif args[0] == 'seconds':
        defs['seconds'] = hadoopcalc(megs=defs['megs'], mbps=defs['mbps'], nodes=defs['nodes'])

    defs['gigs'] = defs['megs'] / 1024

    if opt.quick:
        print defs[args[0]]
        sys.exit(0)
    else:
        print '%s = %d\nBreakdown:' % (args[0], defs[args[0]])
        print '  %d Gigs of data' % (defs['gigs'])
        print '  %d Mbps aggregate throughput' % (defs['mbps'])
        print '  %d number of nodes in the cluster' % (defs['nodes'])
        print '  %s amount of time to do it all...' % (humantime(defs['seconds']))
