import socket


def get_free_tcp_address() -> str:
    """ Get a free tcp address. """
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    host, port = tcp.getsockname()
    tcp.close()
    return "tcp://127.0.0.1:{}".format(port)
