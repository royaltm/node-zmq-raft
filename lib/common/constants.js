/* 
 *  Copyright (c) 2016-2019 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

module.exports = Object.freeze({
  /* raft finite state */

  FSM_CLIENT:    Symbol('Client')
, FSM_FOLLOWER:  Symbol('Follower')
, FSM_CANDIDATE: Symbol('Candidate')
, FSM_LEADER:    Symbol('Leader')

  /* rpc message types */

, APPEND_ENTRY:            '+'
, REQUEST_VOTE:            '?'
, INSTALL_SNAPSHOT:        '$'
, REQUEST_UPDATE:          '='
, CONFIG_UPDATE:           '&'
, REQUEST_ENTRIES:         '<'
, REQUEST_CONFIG:          '^'
, REQUEST_LOG_INFO:        '%'

  /* request entry response statuses */
, RE_STATUS_NOT_LEADER: 0
, RE_STATUS_LAST: 1
, RE_STATUS_MORE: 2
, RE_STATUS_SNAPSHOT: 3

  /* timeouts and intervals */

  /* a default value for the Raft option: electionTimeoutMin */
, ELECTION_TIMEOUT_MIN: 200
  /* a default value for the Raft option: electionTimeoutMax */
, ELECTION_TIMEOUT_MAX: 300
  /* this should be less than ELECTION_TIMEOUT_MIN divided by 2 */
, RPC_TIMEOUT: 50
  /* this should be less than ELECTION_TIMEOUT_MIN divided by 2 */
, APPEND_ENTRIES_HEARTBEAT_INTERVAL: 70
  /* while sending without entries */
, APPEND_ENTRIES_RPC_TIMEOUT_MIN: 70
  /* while sending with entries */
, APPEND_ENTRIES_RPC_TIMEOUT_MAX: 140

  /* hard size limit of a single log entry;
     a default value for the Raft option: maxLogEntryDataSize */
, MAX_LOG_ENTRY_DATA_SIZE: 10*1024*1024 /* 10MB */

  /* send this max bytes of snapshot data to followers at once,
     this also sets a high watermark for log entries sent to followers;
     a default value for the Raft option: peerMsgDataSize */
, PEER_MSG_DATA_SIZE: 64*1024

  /* how long before request update expires in milliseconds;
     this should be longer than expected cluster disaster outage time;
     a default value for the FileLog option: requestIdTtl; */
, DEFAULT_REQUEST_ID_TTL: 8*60*60*1000 /* 8 hours */
  /* a minimum value of the FileLog option: requestIdTtl; */
, MIN_REQUEST_ID_TTL: 60*1000 /* 60 seconds */
  /* a maximum value of the FileLog option: requestIdTtl; */
, MAX_REQUEST_ID_TTL: 25*365*24*60*60*1000 /* 25 years */
  /* when accepting a new log update, the timestamp part of an updating request id
     must be less than now + margin and must be greater than now + margin - requestIdTtl;
     a default value for the Raft option: requestIdTtlAcceptMargin */
, REQUEST_ID_TTL_ACCEPT_MARGIN: 15*60*1000 /* 15 minutes */

  /* limit parallel request entries streaming */
, REQUEST_ENTRIES_HIGH_WATERMARK: 8000
  /* 5 seconds, how log (+ up to 2 seconds) to wait between client requests before
     canceling stream to save resources */
, REQUEST_ENTRIES_TTL: 5*1000
  /* how many entries are send in advance before the confirmation/next request is received */
, REQUEST_ENTRIES_PIPELINES: 5
  /* limit pipeline if single entry byte size is big */
, REQUEST_ENTRIES_ENTRY_SIZE_LIMIT_PIPELINE: 64*1024
  /* how many snapshot chunks send in advance before the confirmation/next request is received */
, REQUEST_ENTRIES_SNAPSHOT_PIPELINES: 2


  /* clients expect response within this timeout, if no response clients should query the next server */
, SERVER_RESPONSE_TTL: 500

  /* clients should wait at least 300 ms before querying cluster again without leader yet */
, SERVER_ELECTION_GRACE_MS: 300

  /* how often to broadcast pings (broadcast state machine) */
, CLIENT_HEARTBEAT_INTERVAL: 500
});
