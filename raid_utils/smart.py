import subprocess
import re


def fetch_smart_data(device):
    smart_data = {}
    smartctl_output = subprocess.check_output('smartctl -a %s' % device, shell=True)
    for line in smartctl_output.split('\n'):
        line_split = line.split(':')

        if line_split[0] == 'Device Model':
            smart_data['model'] = line_split[1].strip()

        elif line_split[0] == 'Serial Number':
            smart_data['sn'] = line_split[1].strip()

        elif line_split[0] == 'User Capacity':
            m = re.search('\[(.*)\]', line_split[1])
            smart_data['size'] = m.group(1) if m else ''

        elif line_split[0] == 'SMART overall-health self-assessment test result':
            smart_data['smart_health'] = line_split[1].strip()

    return smart_data
