import subprocess


def fetch_ircu_bin():
    try:
        lspci_output = subprocess.check_output('lspci | grep \'Serial Attached SCSI controller\'', shell=True)
    except subprocess.CalledProcessError:
        return None
    if 'SAS-2' in lspci_output:
        return 'sas2ircu'
    elif 'SAS-3' in lspci_output:
        return 'sas3ircu'
    else:
        return None


def fetch_ircu_data(ircu_bin):
    drives = {}
    controller_ids = []

    sas3ircu_list_output = subprocess.check_output('%s LIST' % ircu_bin, shell=True)
    for line in sas3ircu_list_output.split('\n'):
        try:
            controller_id = int(line.split()[0])
            controller_ids.append(controller_id)
        except:
            pass

    for controller_id in controller_ids:
        sas3ircu_display_output = subprocess.check_output('%s %s DISPLAY' % (ircu_bin, controller_id), shell=True)
        for line in sas3ircu_display_output.split('\n'):
            line_split = line.split(':')
            if len(line_split) == 2:
                label, value = line_split

                if label.strip() == 'Enclosure #':
                    enclosure_id = int(value)

                elif label.strip() == 'Slot #':
                    slot_id = int(value)

                elif label.strip() == 'Serial No':
                    drives[value.strip()] = {
                        'controller': controller_id,
                        'enclosure': enclosure_id,
                        'slot': slot_id
                    }

    return drives


def locate_disk(ircu_bin, controller, enclosure, slot, operation):
    subprocess.check_output('%(ircu_bin)s %(controller)s LOCATE %(enclosure)s:%(slot)s %(operation)s' % {
        'ircu_bin': ircu_bin,
        'controller': controller,
        'enclosure': enclosure,
        'slot': slot,
        'operation': operation
    }, shell=True)
