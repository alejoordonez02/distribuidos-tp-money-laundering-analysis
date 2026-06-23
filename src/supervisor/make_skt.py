from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket


def _make_skt(addr: tuple[str, int]):
    skt = socket(AF_INET, SOCK_STREAM)
    skt.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    skt.bind(addr)
    return skt
