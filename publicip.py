import socket
import os


def get_ip_address():
    hostname = socket. gethostname()
    host_ip = socket. gethostbyname(hostname)
    print('gethost: ' + host_ip)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
    except socket.error:
        pass
    sock_ip = s.getsockname()[0]
    print('socketname: ' + sock_ip)
    stream = os.popen("ifconfig | grep 'inet ' | grep -v 127.0.0.1 | awk '$1 == \"inet\" {print $2}'")
    os_ip = stream.read()
    print('ifconfig: ' + os_ip)
    return sock_ip
                
