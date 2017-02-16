#!/usr/bin/env python
import argparse
import json

from raid_utils import smart, ircu

parser = argparse.ArgumentParser(description='Locates a given device using its LED.')
parser.add_argument('dev', action='store', help='device path')
parser.add_argument('operation', action='store', help='on or off')
args = parser.parse_args()

# get the smart_data for the device
smart_data = smart.fetch_smart_data(args.dev)
print json.dumps(smart_data, indent=4)

# get the ircu data
ircu_bin = ircu.fetch_ircu_bin()
ircu_data = ircu.fetch_ircu_data(ircu_bin)

print json.dumps(ircu_data[smart_data['sn']], indent=4)

ircu.locate_disk(
    ircu_bin,
    ircu_data[smart_data['sn']]['controller'],
    ircu_data[smart_data['sn']]['enclosure'],
    ircu_data[smart_data['sn']]['slot'],
    args.operation
)
