import os
import subprocess
import unittest
from crosscheck import vectors
from snapshot_crosscheck import controls
from snapshot import Mapping, validate_snapshot
from envelope import Invalid


class Contract(unittest.TestCase):
    def test_native_snapshot_parity(self):
        exe=os.environ['VBSC_TEST_ORACLE']
        produced=subprocess.run([exe,'--controls'],capture_output=True,text=True,check=True,timeout=15)
        self.assertEqual(produced.stdout.splitlines(),controls())
        cases=[]
        for b,p in zip([bytes.fromhex(v) for v in controls()],vectors()):
            cases.append((b,p,b))
            for i in range(len(b)):
                x=bytearray(b);x[i]^=128;x=bytes(x)
                cases.extend(((x,p,x),(b,p,x)))
            for i in range(len(b)):cases.extend(((b[:i],p,b),(b,p,b[:i])))
            for i in range(len(p)):
                x=bytearray(p);x[i]^=128;cases.append((b,bytes(x),b))
            cases.extend(((b+b'\0',p,b),(b,p[:-1],b),(b,p+b'\0',b)))
        m=Mapping(b'\1'+bytes(15),2**64-1,1024)
        wanted=[]
        for b,p,a in cases:
            try:validate_snapshot(b,p,a,m);wanted.append('1')
            except Invalid:wanted.append('0')
        wire=''.join(':'.join(v.hex() for v in row)+'\n' for row in cases)
        result=subprocess.run([exe],input=wire,capture_output=True,text=True,check=True,timeout=30)
        self.assertEqual(result.stderr,'')
        self.assertEqual(result.stdout.splitlines(),wanted)
        print(f'NATIVE_SNAPSHOT_PARITY_PASS cases={len(cases)} independent_controls=5')


if __name__=='__main__':unittest.main()
