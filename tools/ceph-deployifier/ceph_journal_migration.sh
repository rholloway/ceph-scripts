#!/bin/bash
# script to automatically create partitions on a new journal disk and move referenced
# osds to use that journal.
# WARNING: this assumes that journal_disk is a new empty disk. It will blindly try to create
#          partitions 1-<num_osds>. Be sure you know what you are doing before you run this...
#          there is basically 0 error handling currently as it steps through all the commands
#          for the specified OSDs
usage="$0 <journal_device> <comma separated list of osd ids>"
if [ "$#" -ne 2 ]; then
    echo $usage;
    echo "example: ceph_journal_migration.sh /dev/sdk 1,2,3,4"
    exit 1;
fi


journal_device="$1"
IFS=', ' read -r -a osds <<< "$2"

echo "Using Journal device: $journal_device"
echo "Number of OSDS: ${#osds[@]}"


num_journal_part=${#osds[@]}
i=1
while [ $i -le $num_journal_part ]
do
    disk_id=`uuidgen`
    echo "creating partition ${i} on ${journal_device} for osd ${osds[$i-1]} using disk id ${disk_id}"
    sgdisk --new=${i}:0:+5G --change-name="${i}:ceph journal" --partition-guid=${i}:${disk_id} --typecode=${i}:45b0969e-9b03-4f30-b4c6-b4b80ceff106 --mbrtogpt ${journal_device}
    # partx below from original script is failing me sometimes, doesn't seem consistent, partprobe worked
    #partx -a $journal_device
    partprobe
    # old service ceph stop osd.${osds[$i-1]}
    echo "setting ceph osd noout"
    ceph osd set noout
    #systemctl stop ceph-osd@${osds[$i-1]}
    # upstart version
    echo "stopping ceph osd.${osds[$i-1]}"
    stop ceph-osd id=${osds[$i-1]}

    echo "flushing journal"
    ceph-osd -i ${osds[$i-1]} --flush-journal

    echo "remapping journal to new disk/partition"
    ln -sf /dev/disk/by-partuuid/${disk_id} /var/lib/ceph/osd/ceph-${osds[$i-1]}/journal
    #chown -h ceph:ceph /var/lib/ceph/osd/ceph-${osds[$i-1]}/journal
    echo $disk_id > /var/lib/ceph/osd/ceph-${osds[$i-1]}/journal_uuid

    echo "initializing journal"
    ceph-osd -i ${osds[$i-1]} --mkjournal
    # old service ceph start osd.${osds[$i-1]}
    #systemctl start ceph-osd@${osds[$i-1]}

    echo "starting osd.${osds[$i-1]}"
    start ceph-osd id=${osds[$i-1]}

    echo "resuming ceph rebalancing"
    ceph osd unset noout
    i=$[$i+1]
done