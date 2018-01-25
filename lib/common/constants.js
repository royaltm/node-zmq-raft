/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

module.exports = Object.freeze({

  /* timeouts and intervals */

  ELECTION_TIMEOUT_MIN: 200
, ELECTION_TIMEOUT_MAX: 300
  /* this should be less than ELECTION_TIMEOUT_MIN divided by 2 */
, APPEND_ENTRIES_HEARTBEAT_INTERVAL: 70
  /* this should be less than ELECTION_TIMEOUT_MIN divided by 2 */
, RPC_TIMEOUT: 50
  /* while sending without entries */
, APPEND_ENTRIES_RPC_TIMEOUT_MIN: 70
  /* while sending with entries */
, APPEND_ENTRIES_RPC_TIMEOUT_MAX: 140

  /* client expects response within this timeout, if not they should try to query the next server */
, SERVER_RESPONSE_TTL: 500

  /* how often broadcast pings (broadcast state machine) */
, CLIENT_HEARTBEAT_INTERVAL: 500

  /* hard size limit of a single log entry */
, MAX_LOG_ENTRY_DATA_SIZE: 10*1024*1024 /* 10MB */

  /* send this max bytes of snapshot data to followers at once,
     this also sets high watermark for log entries sent to followers */
, PEER_MSG_DATA_SIZE: 64*1024

  /* raft finite state */

, FSM_CLIENT:    Symbol('Client')
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

  /* how long before request update expires
     this should be longer than expected cluster disaster outage time */
, REQUEST_UPDATE_TTL: 8*60*60*1000 /* 8 hours */
  /* how far in the past (e.g. due to network/cluster outage)
     can request update id be set and we can accept it
     this should be slightly smaller than REQUEST_UPDATE_TTL */
, REQUEST_UPDATE_IN_PAST_DELTA_MAX: 7*60*60*1000 /* 7 hours */
  /* how far in the future (e.g. due to hosts' clocks de-synchronization)
     can request update id be set and we can accept it */
, REQUEST_UPDATE_IN_FUTURE_DELTA_MAX: 15*60*1000 /* 15 minutes */

  /* clients should wait at least 300 ms before querying cluster again without leader yet */
, SERVER_ELECTION_GRACE_MS: 300

  /* limit parallel request entries streaming */
, REQUEST_ENTRIES_HIGH_WATERMARK: 8000
  /* 5 seconds, how log (+ up to 2 seconds) to wait between client requests before
     canceling stream to save resources */
, REQUEST_ENTRIES_TTL: 5*1000
  /* how many entries send in advance before the confirmation/next request is received */
, REQUEST_ENTRIES_PIPELINES: 5
  /* limit pipeline if single entry byte size is big */
, REQUEST_ENTRIES_ENTRY_SIZE_LIMIT_PIPELINE: 64*1024
  /* how many snapshot chunks send in advance before the confirmation/next request is received */
, REQUEST_ENTRIES_SNAPSHOT_PIPELINES: 2

  /* request entry response statuses */
, RE_STATUS_NOT_LEADER: 0
, RE_STATUS_LAST: 1
, RE_STATUS_MORE: 2
, RE_STATUS_SNAPSHOT: 3
});
