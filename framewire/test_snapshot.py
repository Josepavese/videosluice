import struct
import unittest
from dataclasses import replace
from crosscheck import vectors
from envelope import Invalid, parse
from snapshot import Control, Mapping, Busy, MappingMismatch, marshal_control, parse_control, validate_snapshot


class Contract(unittest.TestCase):
    def fixture(self):
        p = vectors()[0]
        h, _ = parse(p)
        c = Control(2**64-2, 1024, len(p), 2**64-1, h.namespace, 2**64-2)
        return c, marshal_control(c), p, Mapping(c.namespace,c.generation,c.capacity)

    def test_exact_unsigned_offsets(self):
        c,b,p,m = self.fixture()
        self.assertEqual(struct.unpack_from(">Q", b, 32)[0],2**64-1)
        self.assertEqual(parse_control(b),c)
        self.assertEqual(validate_snapshot(b,p,b,m),parse(p))

    def test_reject_bad_types_sizes_and_overflow(self):
        c,b,p,m = self.fixture()
        for changes in ({"begin":True},{"end":2**64},{"used":447},{"used":1025},
                        {"generation":0},{"namespace":bytearray(16)},{"capacity":2**64}):
            with self.assertRaises(Invalid): marshal_control(replace(c,**changes))
        for invalid in (b[:-1],b+b"x",bytearray(b),b"NMSH"+b[4:]):
            with self.assertRaises(Invalid):parse_control(invalid)
        with self.assertRaises(Invalid):validate_snapshot(b,bytearray(p),b,m)

    def test_generation_namespace_capacity_and_busy(self):
        c,b,p,m = self.fixture()
        for changes in ({"generation":0},{"generation":1},{"namespace":bytes(16)},{"capacity":1023}):
            with self.assertRaises(MappingMismatch):validate_snapshot(b,p,b,replace(m,**changes))
        odd=bytearray(b);struct.pack_into(">Q",odd,8,3)
        with self.assertRaises(Busy):validate_snapshot(bytes(odd),p,b,m)
        changed=marshal_control(replace(c,begin=2,end=2))
        with self.assertRaises(Busy):validate_snapshot(b,p,changed,m)


if __name__=="__main__":unittest.main()
