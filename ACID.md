ACID Storage
============

* Atomic - The last sector being written commits the updating operation.
* Consistent - The data is consistent as long as the file system is. No additional checksums are being introduced.
* Isolated - Only one thread is modifying files.
* Durable - The commit is reported after `fsync`. When a new file is being created (on systems that support this), a parent directory is being fsynced. As long as the system upholds the fsync guarantees the commits are durable.


Log
---

The [FileLog](lib/server/filelog.js) consists of:

- A Snapshot File.
- An ordered collection of Index Files.

The file name of each index file is a 56-bit hexadecimal string of the first LOG INDEX value that the file may begin with. ("may", because the file can actually contain no entries yet).
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


Index File
----------

The [IndexFile](lib/common/indexfile.js) format is an extension to the Token File format (see below).

The Index File consists of tokenized segments. The 1st segment's token is "RLOG" optionally followed by the "META" or more segments followed by the "ITMZ" empty segment marker followed by log entries data.

The `RLOG` segment contains the index of the first log entry, file capacity and offsets of all stored log entries:

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

There's an additional safety step before committing offset of the new entry to make sure the offset of the next entry (if available) is 0. If it's not 0 the offset table will be cleared first starting from the first "dirty" entry until clear entry is found or the table ends.


Snapshot File
-------------

The [SnapshotFile](lib/common/snapshotfile.js) format is an extension to the Token File format (see below).

The Snapshot File consists of tokenized segments. The 1st segment's Token is "SNAP" optionally followed by the "META" or more segments followed by the "DATA" empty segment marker followed by actual snapshot data.

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


Token File
----------

1. A [TokenFile](lib/utils/tokenfile.js) consists of segments (chunks).
2. Each chunk has an 8 byte header followed by random data follewed by padding data.
3. The chunk header consists of a 4 byte identifier (a Token) followed by a 4 byte (least significant byte first) length of data.
4. The padding is 0 to 3 bytes of zeroes to ensure that each chunk always starts at the byte offset
which is a multiple of 4.
5. The Token is usually encoded and presented as 4 printable ASCII characters, e.g. "FORM".


State File
----------

Other RAFT related data also need to be stored in an ACID storage, such as:

* current term,
* voter for,
* cluster membership configuration and related [information](lib/server/raft_persistence.js).

or the last applied index of the [BroadcastStateMachine](lib/server/broadcast_state_machine.js).

For this purpose the [FilePersistence](lib/common/file_persistence.js) class provides a way to safely store the updated state of the persistent data in an append+rolling fashion.

The data of the persistent object is being stored as Message Packed JSON objects with the keys only containing modified properties of the persistent object. The data is being appended to the file producing a collection of Message Packed objects. To read back the current state, the full file scan is required and the last updated property overrides its previous values.

When the file grows large enough (~500 kb) its being rolled by appending a suffix to the previous name and the new file is being created with the whole persistent object being saved to it.

Each write to the file or file creation is being fsynced.

All files with timestamp suffixes appended to their names (e.g. "-2018-09-26-145936-8170"), if not needed for debugging purposes, may be safely deleted.


How to use  `zr-tool`
---------------------

A tool `bin/zr-tool.js` is provided for inspecting, listing and dumping:

- a log directory,
- index files,
- snapshot files and
- state files.

```
bin/zr-tool.js

  Usage: zr-tool [options] [command]

  inspect and modify various zmq-raft file formats

  Options:

    -V, --version            output the version number
    -o, --output <file>      an output file, by default the output goes to stdout
    -s, --snapshot <path>    an alternative path to a snapshot file (FileLog) (default: snap)
    -l, --log <path>         an alternative path to a log sub-directory (FileLog) (default: log)
    -h, --help               output usage information

  Commands:

    dump|d [options] <file>  dump the content of a snapshot, state or a log index file
    list|l [options] <path>  list the content of the log directory or a state or an index file
    inspect|* <path>         inspect a log directory, an indexfile or a state file
```

To inspect a log directory:

```
bin/zr-tool.js /path/to/raft
```

To inspect a RAFT state file:

```
bin/zr-tool.js /path/to/raft/raft.pers
```

To inspect a snapshot file:

```
bin/zr-tool.js /path/to/raft/snap
```

To inspect a log index file:

```
bin/zr-tool.js inspect /path/to/raft/log/00000/00/00/00000000000001.rlog
```

To list index files:

```
bin/zr-tool.js list /path/to/raft
```

To dump the state file:

```
bin/zr-tool.js dump /path/to/raft/raft.pers
```

To dump the snapshot file:

```
bin/zr-tool.js dump /path/to/raft/snap
```

The data of the log entries may be extracted from the index files. The dumping process will create files for each log entry with extensions `.data` and `.meta`. The `.data` file will be a raw entry's data with the log entry header being stripped. The `.meta` file contains the header of an entry:

```
type:request id:term
```

Where the `type` is:

- 0 a STATE entry,
- 1 a CONFIGURATION entry,
- 2 a CHECKPOINT entry.

The `-m` switch will force interpreting data as Message Pack JSONs and will save each entry's data as human readable JSONs instead.

```
bin/zr-tool.js dump /path/to/raft/log/00000/00/00/00000000000001.rlog -d dest/dir -i 100:110,1000:1050 -m
```

will attempt to dump entries with indexes: 100 to 110 and 1000 to 1050 from the index file. The `dest/dir` destination directory will be created if missing.
