from __future__ import absolute_import, unicode_literals, print_function

import os.path
import requests

import thriftrw
from thriftrw.wire import CallMessage
from thriftrw.protocol.binary import BinaryProtocol

ping = thriftrw.load(
    os.path.join(os.path.dirname(__file__), 'ping.thrift'),
)

protocol = BinaryProtocol()


def main():
    req = ping.Ping.ping.request('world')
    call = CallMessage('ping', 1, ping.dumps(req))

    response = requests.post(
        'http://127.0.0.1:8888/thrift',
        data=protocol.serialize_message(call),
    )
    reply = protocol.deserialize_message(response.content)
    assert reply.name == 'ping'
    assert reply.seqid == 1

    resp = ping.loads(ping.Ping.ping.response, reply.payload)
    print(resp)

if __name__ == "__main__":
    main()
