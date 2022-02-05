import socket
import os


def get_ip_address():
    hostname = socket. gethostname()
    local_ip = socket. gethostbyname(hostname)
    if local_ip != '127.0.0.1':
        return local_ip
    else:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
        except socket.error:
            pass
        local_ip = s.getsockname()[0]
        if local_ip != '127.0.0.1':
            return local_ip
        else:
            stream = os.popen("ifconfig | grep 'inet ' | grep -v 127.0.0.1 | awk '$1 == \"inet\" {print $2}'")
            output = stream.read()
            if output == '':
                print("Warning, using loopback!")
                output = '127.0.0.1'
            return output
                
