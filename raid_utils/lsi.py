import json
import subprocess


controller_create_stmt = '''
    CREATE TABLE lsi_controller (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `timestamp` DATETIME,
        `host` VARCHAR(64),
        `controller` INT,
        `model` VARCHAR(128),
        `sn` VARCHAR(128),
        `firmware` VARCHAR(128),
        `status` VARCHAR(128),
        `memory_correctable_errors` INT,
        `memory_uncorrectable_errors` INT,
        `virtual_drives` INT,
        `physical_drives` INT
    );
'''

disk_create_stmt = '''
    CREATE TABLE lsi_disk (
        `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        `lsi_controller_id` INT,
        `timestamp` DATETIME,
        `host` VARCHAR(64),
        `controller` INT,
        `enclosure` INT,
        `slot` INT,
        `did` INT,
        `model` VARCHAR(128),
        `sn` VARCHAR(128),
        `size` VARCHAR(16),
        `medium` VARCHAR(128),
        `interface` VARCHAR(128),
        `state` VARCHAR(16),
        `device_speed` VARCHAR(16),
        `link_speed` VARCHAR(16),
        `media_error_count` INT,
        `other_error_count` INT,
        `predictive_failure_count` INT,
        `smart_alert_flag` BOOLEAN,
        `temperature` VARCHAR(16)
    );
'''

controller_insert_stmt = '''
    INSERT INTO lsi_controller VALUES (
        NULL,
        %(timestamp)s,
        %(host)s,
        %(controller)s,
        %(model)s,
        %(sn)s,
        %(firmware)s,
        %(status)s,
        %(memory_correctable_errors)s,
        %(memory_uncorrectable_errors)s,
        %(virtual_drives)s,
        %(physical_drives)s
    )
'''

disk_insert_stmt = '''
    INSERT INTO lsi_disk VALUES (
        NULL,
        %(lsi_controller_id)s,
        %(timestamp)s,
        %(host)s,
        %(controller)s,
        %(enclosure)s,
        %(slot)s,
        %(did)s,
        %(model)s,
        %(sn)s,
        %(size)s,
        %(medium)s,
        %(interface)s,
        %(state)s,
        %(device_speed)s,
        %(link_speed)s,
        %(media_error_count)s,
        %(other_error_count)s,
        %(predictive_failure_count)s,
        %(smart_alert_flag)s,
        %(temperature)s
    )
'''


def fetch_data(controller_id=None):
    controllers = []
    drives = []

    if controller_id:
        controller_json = subprocess.check_output('storcli64 /c%s show all J' % controller_id, shell=True)
    else:
        controller_json = subprocess.check_output('storcli64 /call show all J', shell=True)

    for controllers_data in json.loads(controller_json)['Controllers']:
        controller_data = controllers_data['Response Data']
        controller = controller_data['Basics']['Controller']

        controllers.append({
            'controller': int(controller),
            'model': controller_data['Basics']['Model'],
            'sn': controller_data['Basics']['Serial Number'],
            'firmware': controller_data['Version']['Firmware Package Build'],
            'status': controller_data['Status']['Controller Status'],
            'memory_correctable_errors': int(controller_data['Status']['Memory Correctable Errors']),
            'memory_uncorrectable_errors': int(controller_data['Status']['Memory Uncorrectable Errors']),
            'virtual_drives': int(controller_data['Virtual Drives']),
            'physical_drives': int(controller_data['Physical Drives'])
        })

        physical_drives = [pd['EID:Slt'].split(':') for pd in controller_data['PD LIST']]

        for enclosure, slot in physical_drives:
            drive_path = '/c%s/e%s/s%s' % (controller, enclosure, slot)
            drive_json = subprocess.check_output('storcli64 %s show all J' % drive_path, shell=True)
            drive_data = json.loads(drive_json)['Controllers'][0]['Response Data']
            drive_information = drive_data['Drive %s' % drive_path][0]
            drive_detailed_information = drive_data['Drive %s - Detailed Information' % drive_path]
            drive_attributes = drive_detailed_information['Drive %s Device attributes' % drive_path]
            drive_state = drive_detailed_information['Drive %s State' % drive_path]
            drives.append({
                'controller': int(controller),
                'enclosure': int(enclosure),
                'slot': int(slot),
                'did': int(drive_information['DID']),
                'model': drive_information['Model'],
                'sn': drive_attributes['SN'].strip(),
                'size': drive_information['Size'],
                'medium': drive_information['Med'],
                'interface': drive_information['Intf'],
                'state': drive_information['State'],
                'device_speed': drive_attributes['Device Speed'],
                'link_speed': drive_attributes['Link Speed'],
                'media_error_count': int(drive_state['Media Error Count']),
                'other_error_count': int(drive_state['Other Error Count']),
                'predictive_failure_count': int(drive_state['Predictive Failure Count']),
                'smart_alert_flag': drive_state['S.M.A.R.T alert flagged by drive'] != 'No',
                'temperature': drive_state['Drive Temperature']
            })

    return controllers, drives
