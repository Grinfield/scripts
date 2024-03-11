#!/bin/bash

MARIA_HOSTS=""
ANSIBLE=""

declare -A rsts

function callback()
{
    node=`echo $* | sed -r "s/^[0-9]\s(.+)(\s\|\s){1}(\w+)\s.*$/\1/g"`
    rsts[$node]=$*
}

function cleanup()
{
    rm -f /tmp/maria || true
}
# when exception: kill self & subprocess
function exitup()
{
    cleanup
    kill -9 0
}

# print info message
function info()
{
    echo -e "$(date +'%F %T') <INFO> $@"
}

# print warning message
function warn()
{
    echo -e "$(date +'%F %T') \033[31m<WARN>\033[0m $@"
}

function handle_output()
{
    mapfile myarr < /tmp/maria
    for i in ${!myarr[@]}; do
        node=$(echo -n "${myarr[$i]}"|awk -F\| '{print $1}'|sed 's/[ \t]*$//g')
        # echo "node: $node"
        rsts[$node]=${myarr[$i]}
    done
}

function check_active()
{
    $ANSIBLE mariadb -m shell -a "systemctl is-active mariadb" -o >/tmp/maria 2>/dev/null
    if [ $? -eq 0 ]; then
        info "All MariaDB servers seem OK, do nothing."
        return 0
    else
        return 1
    fi
}

function check_active_and_boot()
{
    check_active && exit 0
    handle_output

    any_active=false
    for n in ${!rsts[*]}; do
        # echo $n: ${rsts[$n]}
        if echo -n "${rsts[$n]}"|grep -Ev 'FAILED'|grep -wq ' active' >/dev/null 2>&1; then
            any_active=true
            break
        fi
    done

    if $any_active; then
        for node in ${!rsts[*]}; do
            if echo -n "${rsts[$node]}"|grep -Eq 'FAILED' > /dev/null 2>&1; then
                info "Ready to start MariaDB on $node."
                $ANSIBLE $node -m shell -a "systemctl restart mariadb" 2>/dev/null
                check_active
                return $?
            fi
        done
    fi

    return 2
}

function boot_last_stopped_gracefully()
{
    $ANSIBLE mariadb -m shell -a "cat /var/lib/mysql/grastate.dat" -o > /tmp/maria 2>/dev/null
    handle_output

    declare -A node_seqno_mapping
    maxseqno=0
    for i in ${!rsts[*]}; do
        # echo $i: ${rsts[$i]}
        # Find out if any node is safe to boot MariaDB. If so, then boot from that node first.
        safe_to_boot=$(echo -n "${rsts[$i]}"|sed -r 's/^.*safe_to_bootstrap:\s([0-9])+.*$/\1/g')
        if [[ $safe_to_boot -eq 1 ]]; then
            info "Boot MariaDB from $i."
            $ANSIBLE $i -m shell -a "galera_new_cluster warn=false" -o 2>/dev/null
            info "Boot MariaDB from other nodes."
            $ANSIBLE "mariadb:!$i" -m shell -a "systemctl start mariadb warn=false" 2>/dev/null

            check_active
            return $?
        fi

        # If none of nodes is safe to boot, then filter out the seqno, and boot from the node
        # with the most advanced seqno.
        sn=$(echo -n ${rsts[$i]}|sed -r 's/^.*seqno:\s+(\-?[0-9]+).*$/\1/g')
        node_seqno_mapping[$i]=$sn
    done

    for n in ${!node_seqno_mapping[*]}; do
        if [ ${node_seqno_mapping[$n]} -gt $maxseqno ]; then
            maxseqno=${node_seqno_mapping[$n]}
        fi
    done

    if [ $maxseqno -eq 0 ]; then
        warn "All nodes may be last shutdown improperly."
        return 2
    fi

    for n in ${!node_seqno_mapping[*]}; do
        if [ ${node_seqno_mapping[$n]} -eq $maxseqno ]; then
            info "Boot MariaDB from $n."
            $ANSIBLE $n -m shell -a "sed -i '/safe_to_bootstrap/c safe_to_bootstrap: 1' /var/lib/mysql/grastate.dat" 2>/dev/null
            $ANSIBLE $n -m shell -a "galera_new_cluster" 2>/dev/null
            info "Boot Mariadb from other nodes."
            $ANSIBLE "mariadb:!$n" -m shell -a "systemctl start mariadb" 2>/dev/null
            break
        fi
    done

    check_active
    return $?
}

function boot_last_shutdown_improperly()
{
    $ANSIBLE mariadb -m shell -a "mysqld --wsrep-recover --user=mysql" 2>/dev/null
    $ANSIBLE mariadb -m shell -a "grep 'WSREP: Recovered position:' /var/log/mariadb/mariadb.log|tail -n 1" -o > /tmp/maria 2>/dev/null

    handle_output

    declare -A node_seqno_mapping
    maxseqno=0
    for i in ${!rsts[*]}; do
        # echo $i: ${rsts[$i]}
        if echo -n ${rsts[$i]} | grep -E 'FAILED' > /dev/null 2>&1; then
            continue
        fi
        sn=$(echo -n "${rsts[$i]}" | awk -F: '{print $NF}')
        # echo $sn
        node_seqno_mapping[$i]=$sn
    done

    for i in ${!node_seqno_mapping[*]}; do
        if [ ${node_seqno_mapping[$i]} -gt $maxseqno ]; then
            maxseqno=${node_seqno_mapping[$i]}
        fi
    done

    if [ $maxseqno -eq 0 ]; then
        warn "After a recovery boot, we still can't position the destined node."
        return 2
    fi

    for n in ${!node_seqno_mapping[*]}; do
        if [ ${node_seqno_mapping[$n]} -eq $maxseqno ]; then
            info "Boot MariaDB from $n."
            $ANSIBLE $n -m shell -a "sed -i '/safe_to_bootstrap/c safe_to_bootstrap: 1' /var/lib/mysql/grastate.dat" 2>/dev/null
            $ANSIBLE $n -m shell -a "galera_new_cluster" 2>/dev/null
            info "Boot MariaDB from other nodes."
            $ANSIBLE "mariadb:!$n" -m shell -a "systemctl start mariadb" 2>/dev/null
            break
        fi
    done

    check_active
    return $?
}

function check_ansible_installed()
{
    if ! which ansible > /dev/null 2>&1; then
        warn "Ansible is not installed, please firstly install via yum or pip."
        exit 1
    else
        ANSIBLE=$(which ansible)
    fi
}

function check_mariadb_hosts()
{
    MARIA_HOSTS=$($ANSIBLE mariadb --list-hosts|xargs|awk -F: '{print $2}')
    if [[ "$MARIA_HOSTS" = "" ]]; then
        warn "No MariaDB hosts were found, please config with the section \
            name of [mariadb] at the file '/etc/ansible/hosts'"
        exit 1
    fi
}

function main()
{
    trap "exitup" INT TERM
    trap "cleanup" EXIT

    check_ansible_installed

    # Check all the mariadb hosts information
    check_mariadb_hosts

    # Check the activeness of all the mariadbs
    if check_active_and_boot; then
        exit 0
    elif [ $? -eq 1 ]; then
        warn "After a trying to restart, some MariaDB servers are still inactive."
        exit 1
    fi

    # Bootstrap if any mariadb is gracefully stopped at the last time
    if boot_last_stopped_gracefully; then
        exit 0
    elif [ $? -eq 1 ]; then
        exit 1
    fi

    # Bootstrap if all mariadbs are shutdown improperly at the last time
    boot_last_shutdown_improperly

    cleanup

    exit 0
}

main "$@"
