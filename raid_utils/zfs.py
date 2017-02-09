import os
import subprocess


pool_create_stmt = '''
    CREATE TABLE zfs_pools (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `timestamp` DATETIME,
        `host` VARCHAR(64),
        `pool_name` VARCHAR(16),
        `state` VARCHAR(16),
        `scan` VARCHAR(256),
        `write_errors` INT,
        `read_errors` INT,
        `cksum_errors` INT
    );
'''

disk_create_stmt = '''
    CREATE TABLE zfs_disks (
        `id`INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `pool_id` INT NOT NULL,
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
    INSERT INTO zfs_pools (
        `timestamp`,
        `host`,
        `pool_name`,
        `state`,
        `scan`,
        `write_errors`,
        `read_errors`,
        `cksum_errors`
    ) VALUES (
        %(timestamp)s,
        %(host)s,
        %(pool_name)s,
        %(state)s,
        %(scan)s,
        %(write_errors)s,
        %(read_errors)s,
        %(cksum_errors)s
    )
'''

disk_insert_stmt = '''
    INSERT INTO zfs_disks (
        `pool_id`,
        `timestamp`,
        `host`,
        `pool_name`,
        `dev`,
        `dev_by_id`,
        `model`,
        `sn`,
        `size`,
        `controller`,
        `enclosure`,
        `slot`,
        `state`,
        `write_errors`,
        `read_errors`,
        `cksum_errors`,
        `smart_health`
    ) VALUES (
        %(pool_id)s,
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


def fetch_pool_status(pool_name):
    pool = {
        'pool_name': pool_name
    }
    disks = []

    zpool_status_output = subprocess.check_output('zpool status %s' % pool_name, shell=True)
    for line in zpool_status_output.split('\n'):
        if line.startswith('  scan:'):
            pool['scan'] = line.split(':')[1].strip()

        elif line.startswith('\t' + pool_name):
            pool_config = line.split()
            pool['state'] = pool_config[1]
            pool['read_errors'] = int(pool_config[2])
            pool['write_errors'] = int(pool_config[3])
            pool['cksum_errors'] = int(pool_config[4])

        elif line.startswith('\t    '):
            disk_config = line.split()
            dev_by_id = '/dev/disk/by-id/' + disk_config[0]

            disks.append({
                'pool_name': pool_name,
                'dev': os.path.realpath(dev_by_id),
                'dev_by_id': dev_by_id,
                'state': disk_config[1],
                'read_errors': int(pool_config[2]),
                'write_errors': int(pool_config[3]),
                'cksum_errors': int(pool_config[4])
            })

    return pool, disks
