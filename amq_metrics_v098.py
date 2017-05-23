#!/usr/bin/env python

import glob
import commands
import json
import requests
import csv
import re
import argparse
import sys
import socket
import os
import logging
import time
import subprocess
from sys import argv
from requests.auth import HTTPBasicAuth


def send_msg_tcp(message,graphite_server):
    TCP_PORT = 2003
    BUFFER_SIZE = len(message)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((graphite_server, TCP_PORT))
    s.sendall(message)
    s.shutdown(socket.SHUT_RDWR)
    s.close()

def get_my_ip():
    """Parses ifconfig to find this computers IP-address."""
    return commands.getoutput("/sbin/ifconfig | sed 's/addr\:/ /p'")\
                   .split("\n")[1].split()[1][0:].split(',')[0]

def arguments():
    """Parses the arguments from sys.argv, returns some argparse-object.

    If atleast one not specified (except ip), the help is printed and the program
    stops.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", help="pretend my ip is this (optional)")
    parser.add_argument("-m", help="enter metric prefix")
    parser.add_argument("-mem", help="enter heap metric prefix")
    parser.add_argument("-stat", help="enter broker statistics prefix")
    parser.add_argument("-os", help="enter broker os prefix")
    parser.add_argument("-u", help="enter URL to graphite server")
    parser.add_argument("-e", default=None, help="specify filename where error messages are saved, prints to stderr if omitted (optional)")
    args = parser.parse_args()
    if args.ip == None:
        args.ip = get_my_ip()
    args_values = [args.m, args.mem, args.stat, args.os, args.u, args.ip]

    if None in args_values:
        parser.print_help()
        raise SystemExit

    args_values.append(args.e)
    return args_values


def fetch_metric(ip, read_string):
    """Queries Jolokia with HTTP and returns the response JSON parsed.

    On any exceptions it prints the error and exits. The read string
    is infact the part after hawtio/jolokia/read. That part is the
    same for everyone so I hardcoded it here

    """

    url = 'http://{0}:8181/hawtio/jolokia/read/'
    url += read_string

    auth = HTTPBasicAuth('USERNAME-TO-JOLOKIA/HAWTIO','PASSWORD-TO-JOLOKIA/HAWTIO')
    query = url.format(ip)
    try:
        return requests.get(query, auth=auth, timeout=10.0).json()
    except requests.exceptions.RequestException as e:
        logging.error("Tried to call Jolokia but failed: %s URL=%s", e, query)
        return None

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def header_column_sorting(a, b):
    """Special sorting function for the csv header

    It does standard sorting unless one is RouteId which always sorts first"""
    if   a == "Name": return -1
    elif b == "Name": return 1
    else: return cmp(a,b)


def write_metrics_to_file(metrics, ip):
    """Writes the metrics to a temporary file

    The file is /tmp/<ip-number>.csv. Metrics is supposed to be a list
    of maps. Each map containing the metric name and its value as key,
    value pair. I assume also that each element in the list is a map
    with the same keys

    e.g.
    metrics = [{'RouterId': 'myRouter', 'ExchangesCompleted': 0},
    {'RouterId': 'myRouter', 'ExchangesCompleted': 0}]
    """
    filename = "/tmp/%s_amq.csv" % (ip)
    file = open(filename, 'w')

    header_list = metrics[0].keys()
    header_list.sort(header_column_sorting)
    join_metrics_in_order = (lambda metric: ",".join([str(metric[name]).replace('.', '_')
                                                      for name in header_list]) + "\n")

    # Write header
    header = ",".join(header_list) + "\n"
    file.write(header)
    file.writelines(map(join_metrics_in_order, metrics))
    file.close()

def queue_metric_lines(metrics_prefix, response, timestamp):
    """Format all metrics to complete graphite format

    This is specific for the Jolokia json parsed response for queues.

    The format is (one metric per line):
    Prefix.MetricName value timestamp"""
    metrics_for_graphite = []
    queues = filter(no_advisory_filter, response["value"].values())
    for queue in queues:
        name = queue.pop("Name")
        metrics_for_graphite += ["%s.%s.%s %s %s" % (metrics_prefix, name.replace('.','_'), key, value, timestamp)
            for (key, value) in queue.items()]

    return "\n".join(metrics_for_graphite)

def other_metric_lines(metrics_prefix, response, timestamp):
    """Format all metrics to complete graphite format

    response is the jolokia json parsed body.

    The format is (one metric per line):
    Prefix.MetricName value timestamp"""
    metrics_for_graphite = []
    for metric in response["value"].values():
        metrics_for_graphite += ["%s.%s %s %s" % (metrics_prefix, key, value, timestamp)
            for (key, value) in metric.items()]

    return "\n".join(metrics_for_graphite)

def key_value_metric_lines(metrics_prefix, metrics, timestamp):
    """Format all metrics to complete graphite format

    metrics is a dictionary with all metrics, {name: value}

    The format is (one metric per line):
    Prefix.MetricName value timestamp"""
    metrics_for_graphite = ["%s.%s %s %s" % (metrics_prefix, key, value, timestamp)
                            for (key, value) in metrics.items()]

    return "\n".join(metrics_for_graphite)

def im_on_aws():
    """Returns True if this is run on an AWS machine"""
    if os.path.exists("/sys/hypervisor/uuid"):
        return 'ec2' in open('/sys/hypervisor/uuid').read()
    return False

def gather_connection_metrics():
    on_aws = im_on_aws()
    if on_aws:
        connections = commands.getoutput("""/usr/sbin/lsof -np $(pgrep -f [k]araf-jmx-boot) -i | grep 61616 | grep -v LISTEN | awk '{print $9}' | sort | cut -d '>' -f2 | cut -d ':' -f1 | sort | uniq -c |  sed s/-/./g | sort | uniq -c | awk '{print $3 " " $2}' | sed 's/\./_/g' | sed 's/_eu_west_1_compute_internal//g' | sed 's/ip_//'""").split("\n")

    else:
        connections = commands.getoutput("""/usr/sbin/lsof -np $(pgrep -f [k]araf-jmx-boot) -i | grep 61616 | grep -v LISTEN | awk '{print $9}' | sort | cut -d '>' -f2 | cut -d ':' -f1 | sort | uniq -c | awk '{print $2 " " $1}' | sed 's/\./_/g'""").split("\n")

    metrics = {}
    for line in connections:
        try:
            metric, value = line.split(' ')
            metrics[metric] = value
        except Exception as e:
            logging.error("Failed to gather connection metrics due to %s, command output was %s", e, connections)
            return None
    return metrics

def gather_disc_metrics():
    try:
        diskInventory= commands.getoutput("""df -P | egrep -v "Mounted on" | egrep "\<var/opt\>" | sort -k 6 | awk '{print $2,$3,$4,$5}' | sed -e 's/^ //' | sed -e 's/%//'""").split(" ")
        return {
            "DiskVarOpt_1024-Blocks":diskInventory[0],
            "DiskVarOpt_Used":diskInventory[1],
            "DiskVarOpt_Available":diskInventory[2],
            "DiskVarOpt_Capacity":diskInventory[3]
        }
    except Exception as e:
        logging.error("Failed to gather disc metrics due to %s, command output was %s", e, diskInventory)
        return None

def gather_ram_metrics():
    #OS related checks (free), added 2016-12-07
    getRamUsage = commands.getoutput('free | grep -v "total" | awk \'{print $2,$3,$4,$5,$6,$7}\'').split("\n")

    concatedRamInfo = getRamUsage[0] + " " + getRamUsage[1]
    concatedRamInfo = concatedRamInfo.split(" ")


    return {
        "RamTotal":concatedRamInfo[0],
        "RamUsed":concatedRamInfo[1],
        "RamFree":concatedRamInfo[2],
        "RamShared":concatedRamInfo[3],
        "RamBuffandCache":concatedRamInfo[4],
        "RamAvailable":concatedRamInfo[5],
        "RamSwapTotal":concatedRamInfo[6],
        "RamSwapUsed":concatedRamInfo[7],
        "RamSwapFree":concatedRamInfo[8]
    }

def gather_cpu_metrics():
    getCpuLoad = commands.getoutput('cat /proc/stat | egrep "\<cpu\>" | sed -e \'s/cpu  //\'').split(" ")

    metrics = {
        "CpuUser": getCpuLoad[0],
        "CpuNice": getCpuLoad[1],
        "CpuSystem": getCpuLoad[2],
        "CpuIdle": getCpuLoad[3]
    }

    if len(getCpuLoad) > 4:
        metrics["CpuIoWait"] = getCpuLoad[4]
        metrics["CpuIrq"] = getCpuLoad[5]
        metrics["CpuSoftIrq"] = getCpuLoad[6]
    if len(getCpuLoad) > 7:
        metrics["CpuSteal"] = getCpuLoad[7]
    if len(getCpuLoad) > 8:
        metrics["CpuGuest"] = getCpuLoad[8]
    if len(getCpuLoad) > 9:
        metrics["CpuGuestNice"] = getCpuLoad[9]

    metrics["CpuProcsRunning"] = commands.getoutput('cat /proc/stat | egrep "\<procs_running\>" | awk \'{print $2}\'')
    return metrics


def gather_network_metrics():
    try:
        getProcNetDev = commands.getoutput('cat /proc/net/dev | grep eth0 | awk \'{$1="";print $0}\' | sed "s/^[ \t]*//"').split(" ")

        return {
            "eth0RecieveBytes": getProcNetDev[0],
            "eth0RecievePackets": getProcNetDev[1],
            "eth0TransmitBytes": getProcNetDev[8],
            "eth0TransmitPackets": getProcNetDev[9]
        }
    except Exception as e:
        logging.error("Failed to gather network metrics due to %s, command output was %s", e, getProcNetDev)
        return None

def no_advisory_filter(queue):
    return 'Advisory' not in queue["Name"]

def fetch_gc_metrics(ip):
    p1 = subprocess.Popen(['ps','-fwwC','java'], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['grep','UseConcMarkSweepGC'], stdin=p1.stdout, stdout=subprocess.PIPE)
    p2.communicate()

    gc_metrics, gc_metrics_sweep = None, None

    if (p2.returncode == 0):
        gc_metrics = fetch_metric(ip, 'java.lang:type=GarbageCollector,name=ParNew/CollectionTime')
        gc_metrics_sweep = fetch_metric(ip, 'java.lang:type=GarbageCollector,name=ConcurrentMarkSweep/CollectionTime')
    else:
        gc_metrics = fetch_metric(ip, 'java.lang:type=GarbageCollector,name=G1 Young Generation/CollectionTime')

    return (gc_metrics, gc_metrics_sweep)

if __name__ == '__main__':

    result = []
    my_dict = {}

    metric_prefix, heap_prefix, stats_prefix, os_prefix, carbon_server, ip, error_filename = arguments()
    non_heap_prefix = str.replace(heap_prefix, 'HeapMemoryUsage', 'NonHeapMemoryUsage')
    logging.basicConfig(stream=sys.stderr, datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s - %(levelname)s at line %(lineno)d - %(message)s', filename=error_filename)

    # Name is added later
    queue_metrics=("QueueSize", "EnqueueCount", "DequeueCount", "ProducerCount", "ConsumerCount",
                   "MemoryPercentUsage", "InFlightCount", "ForwardCount", "BlockedSends")
    broker_metrics=("TotalConnectionsCount", "TotalConsumerCount", "TotalDequeueCount", "TotalEnqueueCount",
                    "TotalMessageCount", "TotalProducerCount", "CurrentConnectionsCount")
    os_metrics=("FreePhysicalMemorySize","FreeSwapSpaceSize","OpenFileDescriptorCount","ProcessCpuLoad",
                "ProcessCpuTime","SystemCpuLoad","SystemLoadAverage","TotalPhysicalMemorySize","TotalSwapSpaceSize")

    url_part = 'org.apache.activemq:brokerName=*,destinationName=*,destinationType=*,type=Broker/'
    heap_metrics = fetch_metric(ip, 'java.lang:type=Memory/*')
    (gc_metrics, gc_metrics_sweep) = fetch_gc_metrics(ip)

    os_metrics = fetch_metric(ip, 'java.lang:type=OperatingSystem/' + ','.join(os_metrics))
    stats_metrics = fetch_metric(ip, 'java.lang:type=Threading/ThreadCount')
    # All does not have timestamp, just pick the one from queue_metrics and use it everywhere
    timestamp = os_metrics["timestamp"] if os_metrics != None else int(time.time())

    non_heap_metric = {'used': heap_metrics["value"]["NonHeapMemoryUsage"]["used"]} if heap_metrics != None else None
    connection_metrics = gather_connection_metrics()
    disc_metrics = gather_disc_metrics()
    ram_metrics = gather_ram_metrics()
    cpu_metrics = gather_cpu_metrics()
    network_metrics = gather_network_metrics()


    # Create metrics to graphite
    metrics_to_graphite = []

    for some_metrics in chunks(queue_metrics, 3):
        these_metrics = fetch_metric(ip, url_part + ','.join(list(some_metrics)+['Name']))
        if these_metrics == None:
            break

        # Find all queues and remove Advisory Queues, if none is left, break!
        if (len(filter(no_advisory_filter, [v for (k,v) in these_metrics['value'].items()]))) == 0:
            break
        metrics_to_graphite.append(queue_metric_lines(metric_prefix, these_metrics, timestamp))

    for some_metrics in chunks(broker_metrics, 3):
        these_metrics = fetch_metric(ip, 'org.apache.activemq:type=Broker,brokerName=*/' + ','.join(some_metrics))
        if these_metrics == None:
            break

        metrics_to_graphite.append(other_metric_lines(stats_prefix, these_metrics, timestamp))

    if os_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(stats_prefix, os_metrics["value"], timestamp))

    if stats_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(stats_prefix, {"ThreadCount": stats_metrics["value"]}, timestamp))

    if non_heap_metric != None:
        metrics_to_graphite.append(key_value_metric_lines(non_heap_prefix, non_heap_metric, timestamp))

    if heap_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(heap_prefix,
                                                          heap_metrics['value']["HeapMemoryUsage"], timestamp))
    if connection_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(os_prefix + '.Connected_Server', connection_metrics, timestamp))

    if disc_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(os_prefix, disc_metrics, timestamp))

    if ram_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(os_prefix, ram_metrics, timestamp))

    if cpu_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(os_prefix, cpu_metrics, timestamp))

    if gc_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(os_prefix, {"GCCollectionTime": gc_metrics['value']}, timestamp))

    if gc_metrics_sweep != None:
        metrics_to_graphite.append(key_value_metric_lines(os_prefix, {"GCSweepCollectionTime": gc_metrics_sweep['value']}, timestamp))

    if network_metrics != None:
        metrics_to_graphite.append(key_value_metric_lines(os_prefix, network_metrics, timestamp))

    metrics_to_graphite.append(key_value_metric_lines(stats_prefix, {"GraphiteLoggingActive": 1}, timestamp))
    metric_lines = '\n'.join(metrics_to_graphite)
    print metric_lines

    try:
        send_msg_tcp(metric_lines,carbon_server)
    except Exception as e:
        logging.error("Failed to send metrics to graphite due to %s", e)
