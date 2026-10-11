"""Immutable snapshot validation only, not an atomic mapped-memory provider."""
from dataclasses import dataclass
import struct
from envelope import HEADER_SIZE, MAX_PAYLOAD, Invalid, parse

CONTROL_SIZE = 64


class Busy(Invalid):
    pass


class MappingMismatch(Invalid):
    pass


@dataclass(frozen=True)
class Mapping:
    namespace: bytes
    generation: int
    capacity: int


@dataclass(frozen=True)
class Control:
    begin: int
    capacity: int
    used: int
    generation: int
    namespace: bytes
    end: int


def _mapping_valid(m):
    return (type(m) is Mapping and type(m.namespace) is bytes
            and len(m.namespace) == 16 and any(m.namespace)
            and type(m.generation) is int and 0 < m.generation < 2**64
            and type(m.capacity) is int
            and HEADER_SIZE <= m.capacity <= HEADER_SIZE + MAX_PAYLOAD)


def _validate(c):
    if type(c) is not Control:
        raise Invalid("control type")
    if not _mapping_valid(Mapping(c.namespace, c.generation, c.capacity)):
        raise Invalid("control allocation")
    for value in (c.begin, c.end, c.used):
        if type(value) is not int or not 0 < value < 2**64:
            raise Invalid("control unsigned range")
    if not HEADER_SIZE <= c.used <= c.capacity:
        raise Invalid("control used area")
    if c.begin & 1 or c.begin != c.end:
        raise Busy("publication in progress")


def marshal_control(c):
    _validate(c)
    return struct.pack(">4sHH4Q16sQ", b"VBSC", 1, CONTROL_SIZE,
                       c.begin, c.capacity, c.used, c.generation, c.namespace, c.end)


def parse_control(b):
    if type(b) is not bytes or len(b) != CONTROL_SIZE:
        raise Invalid("control length/type")
    magic, version, size, *fields = struct.unpack(">4sHH4Q16sQ", b)
    if magic != b"VBSC" or version != 1 or size != CONTROL_SIZE:
        raise Invalid("control format")
    c = Control(*fields)
    _validate(c)
    return c


def validate_snapshot(before, packet, after, expected):
    """No retry, mapped reads or authentication. Inputs must be immutable copies.

    Provider owns atomic loads, copy ordering and safe mapping lifetime;
    middleware owns authenticated expected allocation and outstanding budgets.
    Repeated snapshots are not new captures. Peer/source/graph authorization
    and source/publication fencing remain independent of byte integrity.
    """
    if not _mapping_valid(expected):
        raise MappingMismatch("expected allocation invalid")
    a, b = parse_control(before), parse_control(after)
    for c in (a, b):
        if Mapping(c.namespace, c.generation, c.capacity) != expected:
            raise MappingMismatch("allocation changed")
    if before != after:
        raise Busy("publication changed")
    if type(packet) is not bytes or len(packet) != a.used:
        raise Invalid("packet area")
    h, p = parse(packet)
    if h.namespace != expected.namespace:
        raise MappingMismatch("envelope namespace")
    return h, p
