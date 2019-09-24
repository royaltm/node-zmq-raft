ACID Storage
============

* Atomic - The last sector written commits the appending or truncating operation.
* Consistent - The data is consistent as long as the file system is. No additional checksums are being introduced.
* Isolated - Only one thread is modifying files.
* Durable - The commit is reported after `fsync`. When a new file is being created (on systems that support this), a parent directory is being fsynced. As long as the system upholds the fsync guarantees the commits are durable.


FileLog
-------

The File Log consists of:

- A Snapshot File.
- An ordered collection of Index Files.

The file name of each index file is a 56-bit hexadecimal number of the first LOG INDEX value that the file may begin with. ("may" not is, because the file can actually contain no entries yet).
The file is being placed in a directory structure matching the highest 36 bits of the 56-bit INDEX LOG.

For example the log file containing the first log entry with `INDEX=1` is on the following path (from the root of the RAFT log directory):

```
log/00000/00/00/00000000000001.rlog
```

The path of the log file containing the first log entry with `INDEX=137 438 953 218` is:

```
log/00001/ff/ff/00001fffffff02.rlog
```

The snapshot's file name is by default: "snap" and is placed in the root of the RAFT log directory.

See: [FileLog](lib/server/filelog.js).


Index File
----------

The Index File format is an extension to the token file format (see below).

The Index File consists of chunks (segments). The 1st segment's token is "RLOG" optionally followed by the "META" or more segments followed by the "ITMZ" empty segment marker followed by log entries data.

The `RLOG` segment contains information about the first entry index, file capacity and offsets of all stored log entries:

```
offs. content
  0 | "RLOG"
  4 | 4-byte LSB header size: 16 + capacity * 4
  8 | 01 00 00 00 - version
 12 | 4-byte LSB capacity: how many entries this file can contain, must be 0 < capacity <= MAX_CAPACITY
 16 | 8-byte LSB LOG INDEX of the first entry
 24 | 4-byte LSB offset of the end of the 1st entry relative to the beginning of the log entries data
                 or 0 if there are no stored entries
 28 | 4-byte LSB offset of the end of the 2nd entry relative to the beginning of the log entries data
                 or 0 if there is only 1 stored entry
 32 | 4-byte LSB offset of the end of the 3rd entry relative to the beginning of the log entries data
                 or 0 if there are only 2 stored entries
...
    | 4-byte LSB offset of the end of the nth entry relative to the beginning of the log entries data
      or 0 if the nth and later entries are not stored
    | 4-byte LSB offset of the end of the last entry relative to the beginning of the log entries data
      or 0 if the last entry is not stored
```

The `META` segment contains context information of the creation of this file:

```
offs. content
  0 | "META"
  4 | 4-byte LSB size of msgpacked data
  8 | msgpacked map: {"created": "iso date string", "hostname": hostname, "software": some software info....}
    | 0 to 3 padding bytes
```

Any other segment:

```
offs. content
  0 | 4-byte segment Token
  4 | 4-byte LSB size of the data
  8 | data
    | 0 to 3 padding bytes
```

The marker:

```
offs. content
  0 | "ITMZ"
  4 | 0x00000000
  8 | the beginning of log entries data
```

There are limits imposed on the entries:

* Each entry's size MUST be > 0 bytes.
* Total number of bytes of all entries must be less than MAX_DATASIZE

The number of entries written so far is determined by the number of non-zero offsets in the index table. The first offset == 0 indicates the next available index.

The log entry offsets are always written to 4 byte aligned index table, so no single offset will reside on the boundary of the file sector. This way as long as the disk file system doesn't allow partially written sectors to be part of the updated file there is no way the log file will be corrupted other than by disk/file system error.

On appending, the entry data is being written first, then the offset pointing after the entry is written to the index table, thus committing the write. On rollback only offset table is being zeroed - starting from the index being rolled back to.

There's an additional safety step before committing offset of the new entry to make sure the offset of the next entry (if available) is 0. If it's not 0 the offset table will be cleared first starting from the first "dirty" entry until clear entry is found or end of the table.

See: [IndexFile](lib/common/indexfile.js).


Snapshot File
-------------

The Snapshot File format is an extension to the Token File format (see below).

The Snapshot File consists of chunks (segments). The 1st segment's Token is "SNAP" optionally followed by the "META" or more segments followed by the "DATA" empty segment marker followed by actual snapshot data.

The `SNAP` segment contains information about the log entry index the snapshot is created from, the log term
and the actual snapshot data size.

The `SNAP` segment:

```
offs. content
  0 | "SNAP"
  4 | 4-byte LSB header size: 36
  8 | 01 00 00 00 - version
 12 | 8-byte LSB the log entry index in the snapshot
 20 | 8-byte LSB the log entry term in the snapshot
 28 | 8-byte LSB data size: the size of the snapshot data
```

The `META` segment:

```
offs. content
  0 | "META"
  4 | 4-byte LSB size of the segment
  8 | msgpacked map: {"created": "iso date string", "hostname": hostname, "software": some software info....}
    | 0 to 3 padding bytes
```

Any other segment:

```
offs. content
  0 | 4-byte segment Token
  4 | 4-byte LSB size of the segment
  8 | data
    | 0 to 3 padding bytes
```

The marker:

```
offs. content
  0 | "DATA"
  4 | 0x00000000
  8 | the beginning of the snapshot data
```

See: [SnapshotFile](lib/common/snapshotfile.js).


Token File
----------

1. A Token File consists of chunks (segments).
2. Each chunk has an 8 byte header followed by random data follewed by a padding.
3. The chunk header consists of a 4 byte identifier (a Token) followed by a 4 byte (least significant byte first) length of the data.
4. The padding is 0 to 3 bytes of zeroes to ensure that each chunk always starts at the byte offset
which is a multiplication of 4.
5. The Token is usually encoded and presented as 4 printable ASCII characters, e.g. "FORM".

See: [TokenFile](lib/utils/tokenfile.js).


State File
----------

There are also other information being stored such as the RAFT State and the current peer configuration or the Broadcast State Machine is recording its last applied index.

For this purpose the [FilePersistence](lib/common/file_persistence.js) class provides a way to safely store the updated state of the persistent objects in an append/rolling fashion.

The data of the persistent object is being stored as Message Packed flat JSON objects with the keys only containing modified properties of the persistent object. The data is being appended to the file producing a collection of Message Packed objects. To read back the current state the full file scan is required and the last updated property overrides its previous values.

When the file grows large enough (~500 kb) its being rolled by appending a suffix to the previous name and the new file is being created with the whole persistent object being saved to it.

Each data appended to the file or file creation is being fsynced.

The files with appended suffixes, if not needed for debugging purposes, may be safely deleted.
