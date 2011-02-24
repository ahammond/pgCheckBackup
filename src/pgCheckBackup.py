#!/usr/bin/env python
# -*- coding: ascii -*-

"""
pgBackupHistory
"""

__author__ = 'Andrew Hammond (andrew.george.hammond@gmail.com)'
__copyright__ = 'Copyright (c) 2010-2011 PostgreSQL Global Development Group'
__license__ = 'PostgreSQL License, see http://www.postgresql.org/about/licence'
__vcs_id__ = '$Id: $'

import sys
import os.path
import re
import optparse

# 255 comes from default postgresql config parameter of wal_segsize = 16.
# If you specified a different wal_segsize at postgresql compile time, you need to modify this to be
# 0xffffffff / (wal_segsize * 1024 * 1024) = 255

__XLogSegsPerFile__ = 255

class PgWriteAheadLog :
    def __init__ ( self, fileName ) :
        self.fileName = fileName
        self.timeLineId = int( fileName[0:8], 16 )
        self.logId = int( fileName[8:16], 16 )
        self.logSeg = int( fileName[16:24], 16 )

    def nextLogName ( self ):
        nextLogId = self.logId
        nextLogSeg = self.logSeg + 1
        if nextLogSeg > __XLogSegsPerFile__:
            nextLogSeg = 0
            nextLogId += 1
        return "%08X%08X%08X" % (self.timeLineId, nextLogId, nextLogSeg)

def pgWriteAheadLogList ( firstLogName, lastLogName ) :
    """given a first and last WAL file name, provide a generator which lists all the names between (inclusive)"""
    currentLogName = firstLogName
    while currentLogName <= lastLogName:
        yield currentLogName
        currentLogName = PgWriteAheadLog( currentLogName ).nextLogName()

def logFilesInDirectory ( xLogDirectory ) :
    assert os.path.isdir( xLogDirectory ), "%s is not a directory" % (xLogDirectory)
    logNamePattern = re.compile( r'^[0-9A-F]{24}$' )
    for fileName in os.listdir( xLogDirectory ) :
        if logNamePattern.match( fileName ) :
            yield fileName

class PgBackupHistory :
    def __init__ ( self, backupHistoryFileName ) :
        """backupHistoryFileName should be the full path to the file."""
        self.fileName = backupHistoryFileName
    def firstWalName ( self ) :
        """The first 24 characters in the backup file's name should be the name of the first required WAL file."""
        ( head, tail ) = os.path.split( self.fileName )
        return tail[:24]
    def lastWalName ( self, xLogDirectory ) :
        """Open the .backup file, and parse it to find the name of the last WAL file."""
        raise NotImplemented( 'Need to get backup file format, first.' )
        return ''
    def missingWalFiles ( self, xLogDirectory ) :
        """Read the contents of the backup history file and test to see if the WAL files expected exist on disk"""
        existingLogFiles = frozenset( logFilesInDirectory( xLogDirectory ) )
        for expectedLog in pgWriteAheadLogList( self.firstWalName(), self.lastWalName ):
            if expectedLog not in existingLogFiles:
                yield expectedLog

def getBackups(backupDir):
    assert os.path.isdir(backupDir), "%s is not a directory" % backupDir
    backupNamePattern = re.compile(r'^[0-9A-F]{24}\.[0-9A-F]{8}\.backup$')
    return filter( lambda x: backupNamePattern.match(x), os.listdir(backupDir))

def getNewestBackup(backupDir):
    backups = getBackups(backupDir)
    assert len(backups) > 0, "No backups found in directory %s" % backupDir
    backups.sort()      # a standard alpha sort should be fine here given the strict format of the file names.
    return backups[-1]

def getBackupFile (options) :
    if options.backup:
        return options.backup
    elif options.xlogdir:
        return getNewestBackup(options.xlogdir)
    else:
        return getNewestBackup('.')     # default to the current working directory

def backupCheck (options) :
    backupFile = getBackupFile (options)

def main():
    usage="""usage: %prog [options]
Exits with 0 when all files are present. Otherwise return 1 to indicate missing files.
By default, look in the current working directory for .backup and WAL files.
By default, examine the most recent .backup file in the directory.
By default, print the names of any WAL files which are required by this backup file, but not found.
"""
    parser=optparse.OptionParser(usage=usage, version=__vcs_id__)
    parser.add_option("-q", "--quiet",
        dest="quiet", action="store_true", default=False,
        help="Don't print anything to the stdout.")
    parser.add_option("-v", "--verbose",
        dest = "verbose", action = "store_true", default = False,
        help="Print both missing and present log files (present files are green)")
    parser.add_option("-d", "--backupdir",
        dest="xlogdir", action = "store",
        help="The directory to check for .backup and wal files. Defaults to current directory.")
    parser.add_option("-b", "--backup",
        dest="backup", action = "store",
        help="The name of the backup file to check. Defaults to most recent backup in directory.")
    (options, args) = parser.parse_args()
    return backupCheck(options)

if __name__=='__main__':
    sys.exit(main())
