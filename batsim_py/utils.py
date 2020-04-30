import socket


class Identifier:
    def __init__(self, id):
        assert isinstance(id, int) or isinstance(id, str)
        self.__id = id

    @property
    def id(self):
        return self.__id

    def __hash__(self):
        return hash(self.id)

    def __ne__(self, other):
        return not (self == other)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id
        elif isinstance(other, int) or isinstance(other, str):
            return self.id == other
        return False

def get_free_tcp_address():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    host, port = tcp.getsockname()
    tcp.close()
    return "tcp://127.0.0.1:{}".format(port)
