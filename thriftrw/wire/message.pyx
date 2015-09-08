# Copyright (c) 2015 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import absolute_import, unicode_literals, print_function

from libc.stdint cimport int32_t

from . cimport mtype

from thriftrw._cython cimport richcompare

__all__ = [
    'Message',
    'CallMessage',
    'ReplyMessage',
    'ExceptionMessage',
    'OneWayMessage',
]


cdef class Message(object):
    """A Message envelope for Thrift requests and responses.

    This should not be instantiated directly. Instead, one of the concrete
    definitions must be used.

    .. py:attribute:: name

        Name of the method being called.

    .. py:attribute:: seqid

        ID of the message used by the client to match responses to requests.
        The server's contract is to return the same ``seqid`` in the response
        that it received in the request.

    .. py:attribute:: payload

        Bytes representing the **serialized** payload of the message.

    The following additional attributes are available on a message.

    .. py:attribute:: message_type

        Message type of the message.

    .. versionadded:: 0.6
    """

    def __cinit__(self, unicode name, int32_t seqid, bytes payload):
        self.name = name
        self.seqid = seqid
        self.payload = payload

    def __richcmp__(Message self, Message other not None, int op):
        return richcompare(op, [
            (self.message_type, other.message_type),
            (self.name, other.name),
            (self.seqid, other.seqid),
            (self.payload, other.payload),
        ])

    def __str__(self):
        return '%s(%r, %r, %r)' % (
            self.__class__.__name__, self.name, self.seqid, self.payload
        )

    def __repr__(self):
        return str(self)


cdef class CallMessage(Message):
    """An outgoing call to a specific method.

    The payload is the serialized request arguments ``struct``.

    Has the same attributes as :py:class:`Message`.

    .. versionadded:: 0.6
    """
    message_type = mtype.CALL


cdef class ReplyMessage(Message):
    """A response to a :py:class:`CallMessage`.

    The payload is the serialized response ``union``.

    Has the same attributes as :py:class:`Message`.

    .. versionadded:: 0.6
    """
    message_type = mtype.REPLY


cdef class ExceptionMessage(Message):
    """An unexpected exception in response to a :py:class:`CallMessage`.

    Note that exceptions that are defined in the IDL are returned as part of
    the :py:class:`ReplyMessage` payload in the response ``union``. This
    message type is used for unexpected exceptions that were not defined in
    the IDL.

    Has the same attributes as :py:class:`Message`.

    .. versionadded:: 0.6
    """
    message_type = mtype.EXCEPTION


cdef class OneWayMessage(Message):
    """An outgoing request to a specific ``oneway`` method.

    The payload is similar to a :py:class:`CallMessage` but no response is
    expected.

    Has the same attributes as :py:class:`Message`.

    .. versionadded:: 0.6
    """
    message_type = mtype.ONEWAY


def message_cls(int typ):
    """Returns the Message class for the given message type code.

    Returns None if no such message type is known."""
    if typ == mtype.CALL:
        return CallMessage
    elif typ == mtype.REPLY:
        return ReplyMessage
    elif typ == mtype.EXCEPTION:
        return ExceptionMessage
    elif typ == mtype.ONEWAY:
        return OneWayMessage
    else:
        return None
