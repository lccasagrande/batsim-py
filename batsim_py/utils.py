import socket


class Identifier:
    """ Hashable object class. 

    This class allows objects to be compared against its id. 

    Args:
        id: The identifier of the object.
    """

    def __init__(self, id: str) -> None:
        self.__id = str(id)

    @property
    def id(self) -> str:
        return self.__id

    def __hash__(self) -> int:
        return hash(self.id)

    def __ne__(self, other) -> bool:
        return not (self == other)

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return self.id == other.id
        return super().__eq__(other)


def get_free_tcp_address() -> str:
    """ Get a free tcp address. """
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    host, port = tcp.getsockname()
    tcp.close()
    return "tcp://127.0.0.1:{}".format(port)
