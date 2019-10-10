import os
import shutil
import sys
import socket
from xml.dom import minidom


def get_free_tcp_address():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    host, port = tcp.getsockname()
    tcp.close()
    return "tcp://127.0.0.1:{}".format(port)


def get_resources_from_platform(fn):
    platform = minidom.parse(fn)
    prop = platform.getElementsByTagName("prop")[0]
    assert prop.getAttribute("id") == 'cores_per_node'
    cores_per_node = int(prop.getAttribute("value"))
    hosts = platform.getElementsByTagName('host')
    hosts.sort(key=lambda x: x.attributes['id'].value)
    resources, id = [], 0
    for r in hosts:
        if r.getAttribute('id') != 'master_host':
            properties = {
                p.getAttribute('id'): p.getAttribute('value') for p in r.getElementsByTagName('prop')
            }
            properties['role'] = properties.get('role', '')
            resource = {
                'id': id,
                'name': r.getAttribute('id'),
                'pstate': r.getAttribute('pstate'),
                'speed': r.getAttribute('speed'),
                'properties': properties,
                'zone_properties': {'cores_per_node': cores_per_node}
            }
            resources.append(resource)
            id += 1
    return resources


def overwrite_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def signal_wrapper(call):
    def cleanup(signum, frame):
        call()
        sys.exit(signum)
    assert callable(call)
    return cleanup
