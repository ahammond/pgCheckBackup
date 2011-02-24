__author__ = 'ahammond'

import unittest

from pgCheckBackup import *

class TestPgWriteAheadLog(unittest.TestCase):
    def testInit(self):
        a = PgWriteAheadLog('000000010000000200000003')
        self.assertEquals(a.timeLineId, 1)
        self.assertEquals(a.logId, 2)
        self.assertEquals(a.logSeg, 3)
    def testNextLogName(self):
        a = PgWriteAheadLog('000000010000000200000003')
        self.assertEquals(a.nextLogName(), '000000010000000200000004')
    def testNextLogNameRollover(self):
        a = PgWriteAheadLog('0000000100000002000000FF')
        self.assertEquals(a.nextLogName(), '000000010000000300000000')

class TestPgBackupHistory(unittest.TestCase):
    def testInit(self):
        name = '/some/path/000000010000000200000003.00000004.backup'
        a = PgBackupHistory(name)
        self.assertEquals(a.fileName, name)
    def testFirstWalName(self):
        name = '/some/path/000000010000000200000003.00000004.backup'
        a = PgBackupHistory(name)
        (head, tail) = os.path.split(name)
        self.assertEquals(a.firstWalName(), tail[0:24])
