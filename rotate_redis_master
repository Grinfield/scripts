#!/bin/bash

declare -A redis_hosts_ports
declare -A host_ip_mapping

readonly MYPID=$$

AUTH=""
ANSIBLE=""
DEBUG=false

function usage()
{
cat <<- EOF

Usage: nohup $(basename $0) [options] &
    Monitor the status of your Redis cluster. And rotate the redis cluster
    master residing in a physical node where the number of redis master
    exceeds the half of this cluster quorum.
OPTIONS:
    -h              Show this usage summary.
    -a <password>   Password to use when connecting to redis.
                    You can also use the REDISCLI_AUTH environment
                    variable to pass this password more safely.
    -d              Enable debug mode.
EOF
}

function cleanup()
{
    rm -f /tmp/redis || true
}
# when exception: kill self & subprocess
function exitup()
{
    cleanup
    kill -9 0
}

function debug()
{
     $DEBUG && echo -e "$(date +'%F %T') <DEBUG> $@"
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

function check_ansible_installed()
{
    if ! which ansible > /dev/null 2>&1; then
        warn "ansible is not installed, please firstly install via yum or pip."
        exit 1
    else
        ANSIBLE=$(which ansible)
    fi
}

function check_hosts_by_ping()
{
    ANS_REDIS_HOSTS=$($ANSIBLE redis --list-hosts|xargs|awk -F: '{print $2}' 2> /dev/null)
    if [[ "$ANS_REDIS_HOSTS" = "" ]]; then
        warn "No Redis hosts were found, please config with the section \
              name of [redis] in the file '/etc/ansible/hosts'"
        exit 1
    fi

    if ! $ANSIBLE redis -m ping > /dev/null 2>&1; then
        warn "Some redis hosts may be disconnected, exit..."
        exit 1
    fi
}

function check_redis_host_port()
{
    for i in ${ANS_REDIS_HOSTS[*]}; do
        local ip=$(echo "$i"|grep -Po '((25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))'\
            ||awk '$2=="'$i'" {print $1}' /etc/hosts)
        local res=$($ANSIBLE $i -m shell -a \
                    "ss -tunlp|grep redis|awk '{print \$5}'|awk -F: '\$2<10000 {print \$2}'|xargs"\
                    2> /dev/null)
        local ports=$(echo "$res"|sed -n '/>>/ {n;p}')
        redis_hosts_ports[$i]=$ports
        host_ip_mapping[$i]=$ip
    done
}

function get_cluster_nodes() 
{
    local nodes_res=""

    if [[ ${#CLUSTER_NODES_ALL} -eq 0 ]]; then
        # nodes_res=$(redis-cli -h ${REDIS_HOST} -p ${REDIS_PORT} -a ${AUTH} cluster nodes 2>&1)
        nodes_res=$(sed '0,/>>/d' /tmp/redis)
        if [[ "${nodes_res:0:8}" != "LOADING " && "${nodes_res:0:7}" != "NOAUTH " && "${nodes_res:0:4}" != "ERR " ]]; then
            # 1.masterID 2.role 3.ID 4.ip:port 5.flags 6.masterID 7.pingtime 8.pongtime 9.configepoch 10.connect 11.slots
            CLUSTER_NODES_ALL=$(echo "$nodes_res"|awk '{if($4=="-"){print $1" master "$0}else{print $4" slave "$0}}'|sort)
        elif [[ "$nodes_res" =~ "cluster support disabled" ]]; then
            # redis isn't in cluster mode
            warn "Redis isn't in cluster mode. Exit!"
            exit 1
        fi

        if [[ ${#CLUSTER_NODES_ALL} -eq 0 ]]; then
            warn "Access redis($1:$2) failed: \033[41;37m${nodes_res}\033[0m"
            if [ "${nodes_res:0:7}" == "NOAUTH " ]; then
                warn "Please input redis password by option '-a <password>'!"
            elif [ "${nodes_res:0:8}" == "LOADING " ]; then
                warn "Please retry the command after LOADING finished!"
            else
                warn "Please input active redis by option '-h <ip> -p <port>'!"
            fi
            exit 1
        fi
    fi

    # cut cluster port from node addr
    CLUSTER_NODES_ALL=$(echo "$CLUSTER_NODES_ALL"|sed -re 's/@[0-9]*//g'|sed -r "s/127.0.0.1/$1/g")
    debug "CLUSTER_NODES_ALL: \n$CLUSTER_NODES_ALL"

    # masters addr list
    MASTER_LIST=$(echo "${CLUSTER_NODES_ALL}"|grep -w master|grep -v noaddr|awk '{print $4}')
    debug "MASTER_LIST: \n$MASTER_LIST"

    # all nodes addr list
    NODES_LIST=$(echo "${CLUSTER_NODES_ALL}"|grep -v noaddr|awk '{print $4}')
    debug "NODES_LIST: \n$NODES_LIST"

    return 0
}

function check_cluster_status()
{
    for h in ${!redis_hosts_ports[*]}; do
        # echo $h: ${redis_hosts_ports[$h]}
        local ip="Nil"
        for k in ${!host_ip_mapping[*]}; do
            if [ "$k" = "$h" ]; then
                ip=${host_ip_mapping[$k]}
                break
            fi
        done

        if [[ $ip = "Nil" || $ip = "" ]]; then
            continue
        fi

        for port in ${redis_hosts_ports[$h]}; do
            info "Execute redis commands on node $h:$port"
            if $ANSIBLE $h -m shell -a "redis-cli -h $ip -p $port -a ${AUTH} \
                --no-auth-warning cluster nodes" > /tmp/redis 2> /dev/null
            then
                get_cluster_nodes $ip $port
                return 0
            fi
        done
    done
}

function rotate()
{
    local master_count=0
    local nodes_count=0
    for node in $(echo "$MASTER_LIST"|awk -F: '{print $1}'|sort|uniq); do
        ((master_count++))
    done
    nodes_count=$(echo "$NODES_LIST"|awk -F: '{print $1}'|sort|uniq|wc -l)
    if [[ $master_count -eq $nodes_count ]]; then
        info "We needn't rotate redis master."
        return 0
    fi

    # If one node has more than one master, whereas another node has only slaves,
    # then we need rotate one of the redis masters on the former node to the latter 
    # node with only slaves.
    local stripped_port_nodes_all=$(echo "$CLUSTER_NODES_ALL"|sed -re 's/:[0-9]*//g')
    # echo "stripped_port_nodes_all:\n$stripped_port_nodes_all"
    local role_host_nodes_all=$(echo "$stripped_port_nodes_all"|sort -k 4|awk '{print $2" "$4}'|uniq -c)
    # echo "role_host_nodes_all:\n$role_host_nodes_all"
    local redundant_master_node=$(echo "$role_host_nodes_all"|awk '{if ($1>1 && $2=="master") {print $3}}')
    debug "redundant_master_node:\n$redundant_master_node"

    local slave_nodes=$(echo "$role_host_nodes_all"|awk '{if ($2=="slave") {print $3}}'|xargs)
    local node_without_master="Nil"
    for node in $slave_nodes; do
        if ! [[ $MASTER_LIST =~ $node ]]; then
            node_without_master=$node
        fi
    done
    # echo node_without_master:\n$node_without_master

    local master_id_list=$(echo "$stripped_port_nodes_all"|grep $redundant_master_node|awk '{print $1}')
    # echo "master_id_list:\n$master_id_list"
    for id in $master_id_list; do
        local slave_id=$(echo "$stripped_port_nodes_all"|awk '$1=="'$id'" && $4=="'$node_without_master'" {print $3}')
        if [[ $slave_id != "" ]]; then
            local pending_rotate_master=$id
            break
        fi
    done
    if [[ $slave_id = "" ]]; then
        warn "Not found valid slave node, exit"
        return 0
    fi

    local failover_node=$(echo "$CLUSTER_NODES_ALL"|grep $slave_id|awk '{print $4}')
    debug "failover_node: $failover_node"
    # local failover_host=$(echo "$failover_node"|awk -F: '{print $1}'| \
    #     xargs -I {} awk '$1=="'{}'" {print $2}' /etc/hosts)
    local failover_host=$(echo "$failover_node"|awk -F: '{print $1}')
    local failover_port=$(echo "$failover_node"|awk -F: '{print $2}')

    for h in ${!host_ip_mapping[*]}; do
        if [[ $failover_host = ${host_ip_mapping[$h]} ]]; then
            ans_failover_host=$h
            break
        fi
    done
    if [[ $ans_failover_host = "" ]]; then
        warn "Exception occurred in finding ansible host."
        ans_failover_host=$failover_host
    fi
    info "Rotate redis master from node $pending_rotate_master to $slave_id."
    $ANSIBLE $ans_failover_host -m shell -a "redis-cli -h $failover_host -p $failover_port \
        -a ${AUTH} --no-auth-warning cluster failover" 2> /dev/null
}


function main()
{
    trap "exitup" INT TERM
    trap "cleanup" EXIT

    while getopts "a:hd" arg
    do
        case $arg in
        d)
            DEBUG=true
            ;;
        a)
            AUTH=${OPTARG}
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Option unknown: ${OPTARG}"
            usage
            exit 1
            ;;
        esac
    done

    check_ansible_installed

    check_hosts_by_ping

    check_redis_host_port

    check_cluster_status

    rotate

    cleanup

    exit 0
}

main "$@"
