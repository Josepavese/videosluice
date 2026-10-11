"""Independent Python vector and acceptance oracle for Go contract tests."""
import json
import sys
from crosscheck import vectors
from envelope import Invalid, parse
from snapshot import Control, Mapping, marshal_control, validate_snapshot


def controls():
    out = []
    for p in vectors():
        h, _ = parse(p)
        c = Control(2**64-2, 1024, len(p), 2**64-1, h.namespace, 2**64-2)
        out.append(marshal_control(c).hex())
    return out


if __name__ == "__main__":
    answers = []
    for record in json.load(sys.stdin):
        b, p, a = [bytes.fromhex(v) for v in record]
        expected = Mapping(b"\1" + bytes(15), 2**64-1, 1024)
        try:
            validate_snapshot(b, p, a, expected)
            answers.append(True)
        except Invalid:
            answers.append(False)
    json.dump({"controls": controls(), "accepted": answers}, sys.stdout)
