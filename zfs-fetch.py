#!/usr/bin/env python
import argparse
import datetime
import json
import socket

from raid_utils import zfs, smart, ircu

parser = argparse.ArgumentParser(description='This fetches information about a local zfs pool.')
parser.add_argument('--pool', action='store', default=None, help='name of the pool [default: all]')
parser.add_argument('--create', action='store_true', help='create database tables')
parser.add_argument('--insert', action='store_true', help='insert into database')
args = parser.parse_args()

timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
host = socket.gethostname()

# get the pool and disk status
pools, disks = zfs.fetch_data(args.pool)

# parse the output of `smartctl`
for disk in disks:
    smart_data = smart.fetch_smart_data(disk['dev_by_id'])
    disk.update(smart_data)

# parse the output of `sas2ircu` or `sas3ircu`
ircu_bin = ircu.fetch_ircu_bin()
if ircu_bin:
    ircu_data = ircu.fetch_ircu_data(ircu_bin)
    for disk in disks:
        if 'sn' in disk and disk['sn'] in ircu_data:
            disk.update(ircu_data[disk['sn']])

if args.create or args.insert:
    import MySQLdb
    from secrets import DB_HOST, DB_USER, DB_PASSWD, DB_NAME

    conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    cur = conn.cursor()

    if args.create:
        cur.execute(zfs.pool_create_stmt)
        cur.execute(zfs.disk_create_stmt)

    if args.insert:
        ids = {}
        for pool in pools:
            pool.update({
                'timestamp': timestamp,
                'host': host
            })
            cur.execute(zfs.pool_insert_stmt, pool)
            ids[pool['pool_name']] = cur.lastrowid

        for disk in disks:
            disk.update({
                'zfs_pool_id': ids[disk['pool_name']],
                'timestamp': timestamp,
                'host': host
            })
            cur.execute(zfs.disk_insert_stmt, disk)
    conn.commit()
    conn.close()
else:
    print json.dumps({
        'pools': pools,
        'disks': disks
    }, sort_keys=True, indent=4)
