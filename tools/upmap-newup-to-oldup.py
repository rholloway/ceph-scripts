#!/usr/bin/env python

import sys, json
from time import sleep
from multiprocessing import Pool
from subprocess import Popen, check_call, PIPE, CalledProcessError

cmd_ceph = "ceph pg ls --format=json".split(' ')
#cmd_jq   = 'jq --stream -Mcr select(.[0][1]=="pgid"or.[0][1]=="up"or.[0][1]=="acting")|"\(.[0][1][:1])_\(.[1])"'.split(' ')
cmd_jq = 'jq --stream -Mcr select(.[0][1]=="pgid"or.[0][1]=="up")|"\(.[0][1][:1])_\(.[1])"'.split(' ')
cmd_upmap = "ceph osd pg-upmap {id} {osds}"

def upmap(pgid, osds):
	global cmd_upmap
	check_call(cmd_upmap.format(id=pgid, osds=' '.join(osds)).split(' '))

if __name__ == '__main__':
    pgid = None
    pgmiss = set()
    up = {}
    act = {}

    p_ceph = Popen(cmd_ceph, stdout=PIPE)
    p_jq = Popen(cmd_jq, stdin=p_ceph.stdout, stdout=PIPE)

    #def init_pg(k, v):
    #    global pgid, up, act
    #    pgid = v
    #    up[v] = []
    #    act[v] = []
    #
    #
    #case = {
    #    'a' : lambda k, v: act[k].append(v),
    #    'u' : lambda k, v: up[k].append(v),
    #    'p' : init_pg
    #}

    print 'Snapshotting the PG state...'

    while True:
        line = p_jq.stdout.readline()
        if line == '' and p_jq.poll() != None:
            break
        try:
            k, v = line[0:-1].split('_')
        except:
            continue
    #   if v != 'null': case[k](pgid, v)
        if k == 'u' and v != 'null':
            up[pgid].append(v)
        elif k == 'p':
            pgid = v
            up[pgid] = []
    #

    p_ceph.wait()
    p_jq.wait()
    try:
        check_call('ceph osd set norebalance'.split(' '))
        check_call('ceph osd set norecover'.split(' '))
        check_call('ceph osd set nobackfill'.split(' '))
    except CalledProcessError as e:
        print 'There was an error setting the norebalance/norecover/nobackfill flags'

    while True:
        _input = raw_input("Do the change, wait for ceph status to stabilize, then yes/no to continue or exit (yes/no or y/n): ")
        if _input in ['y', 'yes']: break
        if _input in ['n', 'no']: sys.exit(0)

    p_ceph = Popen(cmd_ceph, stdout=PIPE)
    p_jq = Popen(cmd_jq, stdin=p_ceph.stdout, stdout=PIPE)

    index = -1
    #def set_pg(k, v):
    #    global pgid
    #    pgid = v
    #
    #def check_up(k, v):
    #    global index, pgmiss, up
    #    if v == 'null':
    #        if k not in pgmiss:
    #            del up[k]
    #        index = -1
    #    else:
    #        index = index + 1
    #        if up[k][index] != v:
    #            pgmiss.add(k)
    #
    #
    #def check_act(k, v):
    #    global index, pgmiss, act
    #    if v == 'null':
    #        if k not in pgmiss:
    #            del act[k]
    #        index = -1
    #    else:
    #        index = index + 1
    #        if act[k][index] != v:
    #            pgmiss.add(k)
    #
    #
    #case = {
    #    'a': check_act,
    #    'u': check_up,
    #    'p': set_pg
    #}

    while True:
        line = p_jq.stdout.readline()
        if line == '' and p_jq.poll() != None:
            break
        try:
            k, v = line[0:-1].split('_')
        except:
            continue
    #   case[k](pgid, v)
        if k == 'u':
            if v != 'null':
                index = index + 1
                if up[pgid][index] != v:
                    pgmiss.add(pgid)
            else:
                if pgid not in pgmiss:
                    del up[pgid]
                index = -1
        else:
            pgid = v
    #
    pool = Pool(32)
    for pg_id in pgmiss:
        try:
            proc = pool.apply_async(upmap, (pg_id, up[pg_id]))
        except Exception as e:
            print 'Upmap failed, reason: \n' + str(e)
            sys.exit(0)

    pool.close()
    pool.join()
    try:
        check_call('ceph osd unset norebalance'.split(' '))
        check_call('ceph osd unset norecover'.split(' '))
        check_call('ceph osd unset nobackfill'.split(' '))
    except CalledProcessError as e:
        print 'There was an error unsetting the norebalance/norecover/nobackfill flags'
