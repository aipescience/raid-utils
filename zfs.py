#!/usr/bin/env python
import argparse
import datetime
import json
import socket

import MySQLdb

from raid_utils import zfs, smart, ircu


from secrets import DB_HOST, DB_USER, DB_PASSWD, DB_NAME

parser = argparse.ArgumentParser(description='This fetches information about a local zfs pool.')
parser.add_argument('--pool', action='store', default='tank', help='name of the pool [default: tank]')
parser.add_argument('--ircu', action='store', default=None, help='name of the ircu bin [sas2ircu or sas3ircu, default: None]')
parser.add_argument('--create', action='store_true', help='create database tables')
parser.add_argument('--insert', action='store_true', help='insert into database')
args = parser.parse_args()

pool = {
    'timestamp': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    'host': socket.gethostname(),
    'pool_name': args.pool
}

# get the pool and disk status
pool, disks = zfs.fetch_pool_status(args.pool)
pool.update({
    'timestamp': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    'host': socket.gethostname(),
    'pool_name': args.pool
})

# parse the output of `smartctl`
for disk in disks:
    smart_data = smart.fetch_smart_data(disk['dev_by_id'])
    disk.update(smart_data)

# parse the output of `sas2ircu` or `sas3ircu`
if args.ircu:
    ircu_data = ircu.fetch_ircu_data(args.ircu)
    for disk in disks:
        disk.update(ircu_data[disk['sn']])
else:
    for disk in disks:
        disk.update({
            'controller': None,
            'enclosure': None,
            'slot': None
        })

if args.create or args.insert:
    # open connection and write to database
    conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    cur = conn.cursor()

    if args.create:
        cur.execute(zfs.pool_create_stmt)
        cur.execute(zfs.disk_create_stmt)

    if args.insert:
        cur.execute(zfs.pool_insert_stmt, pool)
        pool_id = cur.lastrowid

        for disk in disks:
            disk.update({
                'pool_id': pool_id,
                'timestamp': pool['timestamp'],
                'host': pool['host']
            })
            cur.execute(zfs.disk_insert_stmt, disk)
    conn.commit()
    conn.close()
else:
    print json.dumps({
        'pool': pool,
        'disks': disks
    }, sort_keys=True, indent=4)
