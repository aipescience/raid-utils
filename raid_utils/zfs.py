import os
import subprocess


pool_create_stmt = '''
    CREATE TABLE zfs_pool (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `timestamp` DATETIME,
        `host` VARCHAR(64),
        `pool_name` VARCHAR(16),
        `size` VARCHAR(16),
        `alloc` VARCHAR(16),
        `free` VARCHAR(16),
        `health` VARCHAR(16),
        `scan` VARCHAR(256),
        `state` VARCHAR(16),
        `write_errors` INT,
        `read_errors` INT,
        `cksum_errors` INT
    );
'''

disk_create_stmt = '''
    CREATE TABLE zfs_disk (
        `id`INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `zfs_pool_id` INT NOT NULL,
        `timestamp` DATETIME,
        `host` VARCHAR(64),
        `pool_name` VARCHAR(16),
        `dev` VARCHAR(128),
        `dev_by_id` VARCHAR(128),
        `model` VARCHAR(128),
        `sn` VARCHAR(128),
        `size` VARCHAR(16),
        `controller` INT,
        `enclosure` INT,
        `slot` INT,
        `state` VARCHAR(16),
        `write_errors` INT,
        `read_errors` INT,
        `cksum_errors` INT,
        `smart_health` VARCHAR(16)
    );
'''

pool_insert_stmt = '''
    INSERT INTO zfs_pool VALUES (
        NULL,
        %(timestamp)s,
        %(host)s,
        %(pool_name)s,
        %(size)s,
        %(alloc)s,
        %(free)s,
        %(health)s,
        %(scan)s,
        %(state)s,
        %(write_errors)s,
        %(read_errors)s,
        %(cksum_errors)s
    )
'''

disk_insert_stmt = '''
    INSERT INTO zfs_disk VALUES (
        NULL,
        %(zfs_pool_id)s,
        %(timestamp)s,
        %(host)s,
        %(pool_name)s,
        %(dev)s,
        %(dev_by_id)s,
        %(model)s,
        %(sn)s,
        %(size)s,
        %(controller)s,
        %(enclosure)s,
        %(slot)s,
        %(state)s,
        %(write_errors)s,
        %(read_errors)s,
        %(cksum_errors)s,
        %(smart_health)s
    )
'''


def fetch_data(pool_name=None):
    pools = []
    disks = []

    if pool_name:
        zpool_list_output = subprocess.check_output('zpool list %s' % pool_name, shell=True)
    else:
        zpool_list_output = subprocess.check_output('zpool list', shell=True)

    for line in zpool_list_output.split('\n'):
        line_split = line.split()
        if line and line_split[0] != 'NAME':
            pools.append({
                'pool_name': line_split[0],
                'size': line_split[1],
                'alloc': line_split[2],
                'free': line_split[3],
                'health': line_split[8]
            })

    for pool in pools:
        zpool_status_output = subprocess.check_output('zpool status -P %s' % pool['pool_name'], shell=True)
        for line in zpool_status_output.split('\n'):
            if line.startswith('  scan:'):
                pool['scan'] = line.split(':')[1].strip()

            elif line.startswith('\t'):
                line_split = line.split()

                if line_split[0] == pool['pool_name']:
                    pool['state'] = line_split[1]
                    pool['read_errors'] = int(line_split[2])
                    pool['write_errors'] = int(line_split[3])
                    pool['cksum_errors'] = int(line_split[4])

                if line_split[0].strip().startswith('/dev/disk/by-id/'):
                    dev_by_id = line_split[0].strip().replace('-part1', '')
                    disks.append({
                        'pool_name': pool['pool_name'],
                        'dev': os.path.realpath(dev_by_id),
                        'dev_by_id': dev_by_id,
                        'state': line_split[1],
                        'read_errors': int(line_split[2]),
                        'write_errors': int(line_split[3]),
                        'cksum_errors': int(line_split[4])
                    })

    return pools, disks
