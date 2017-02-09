#!/usr/bin/env python
import argparse
import datetime
import json
import socket

from raid_utils import lsi

parser = argparse.ArgumentParser(description='This fetches information about a local lsi controller pool.')
parser.add_argument('--controller', action='store', default='0', help='# of the controller [default: all]')
parser.add_argument('--create', action='store_true', help='create database tables')
parser.add_argument('--insert', action='store_true', help='insert into database')
args = parser.parse_args()

timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
host = socket.gethostname()

controllers, disks = lsi.fetch_data(args.controller)

if args.create or args.insert:
    import MySQLdb
    from secrets import DB_HOST, DB_USER, DB_PASSWD, DB_NAME

    conn = MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASSWD, db=DB_NAME)
    cur = conn.cursor()

    if args.create:
        cur.execute(lsi.controller_create_stmt)
        cur.execute(lsi.disk_create_stmt)

    if args.insert:
        ids = {}
        for controller in controllers:
            controller.update({
                'timestamp': timestamp,
                'host': host
            })
            cur.execute(lsi.controller_insert_stmt, controller)
            ids[controller['controller']] = cur.lastrowid

            for disk in disks:
                disk.update({
                    'timestamp': timestamp,
                    'host': host,
                    'lsi_controller_id': ids[disk['controller']]
                })
                cur.execute(lsi.disk_insert_stmt, disk)
    conn.commit()
    conn.close()
else:
    print json.dumps({
        'controllers': controllers,
        'disks': disks
    }, sort_keys=True, indent=4)
