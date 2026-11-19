// Kafka error codes: their names, and which are worth retrying (the
// same request may succeed once metadata is refreshed or a coordinator
// has loaded).

pub let ERR_NONE = 0;
pub let ERR_OFFSET_OUT_OF_RANGE = 1;
pub let ERR_UNKNOWN_TOPIC_OR_PARTITION = 3;
pub let ERR_LEADER_NOT_AVAILABLE = 5;
pub let ERR_NOT_LEADER_OR_FOLLOWER = 6;
pub let ERR_REQUEST_TIMED_OUT = 7;
pub let ERR_MESSAGE_TOO_LARGE = 10;
pub let ERR_COORDINATOR_LOAD_IN_PROGRESS = 14;
pub let ERR_COORDINATOR_NOT_AVAILABLE = 15;
pub let ERR_NOT_COORDINATOR = 16;
pub let ERR_ILLEGAL_GENERATION = 22;
pub let ERR_UNKNOWN_MEMBER_ID = 25;
pub let ERR_REBALANCE_IN_PROGRESS = 27;
pub let ERR_TOPIC_AUTHORIZATION_FAILED = 29;
pub let ERR_GROUP_AUTHORIZATION_FAILED = 30;
pub let ERR_UNSUPPORTED_SASL_MECHANISM = 33;
pub let ERR_UNSUPPORTED_VERSION = 35;
pub let ERR_TOPIC_ALREADY_EXISTS = 36;
pub let ERR_OUT_OF_ORDER_SEQUENCE_NUMBER = 45;
pub let ERR_DUPLICATE_SEQUENCE_NUMBER = 46;
pub let ERR_INVALID_PRODUCER_EPOCH = 47;
pub let ERR_INVALID_TXN_STATE = 48;
pub let ERR_CONCURRENT_TRANSACTIONS = 51;
pub let ERR_SASL_AUTHENTICATION_FAILED = 58;
pub let ERR_UNKNOWN_PRODUCER_ID = 59;
pub let ERR_FETCH_SESSION_ID_NOT_FOUND = 70;
pub let ERR_INVALID_FETCH_SESSION_EPOCH = 71;
pub let ERR_FENCED_LEADER_EPOCH = 74;
pub let ERR_UNKNOWN_LEADER_EPOCH = 75;
pub let ERR_MEMBER_ID_REQUIRED = 79;
pub let ERR_FENCED_INSTANCE_ID = 82;
pub let ERR_UNSTABLE_OFFSET_COMMIT = 88;
pub let ERR_THROTTLING_QUOTA_EXCEEDED = 89;
pub let ERR_PRODUCER_FENCED = 90;
pub let ERR_TRANSACTION_ABORTABLE = 120;

pub fn error_name(code: int) -> str {
    return switch code {
        case 0 { "NONE" }
        case -1 { "UNKNOWN_SERVER_ERROR" }
        case 1 { "OFFSET_OUT_OF_RANGE" }
        case 2 { "CORRUPT_MESSAGE" }
        case 3 { "UNKNOWN_TOPIC_OR_PARTITION" }
        case 4 { "INVALID_FETCH_SIZE" }
        case 5 { "LEADER_NOT_AVAILABLE" }
        case 6 { "NOT_LEADER_OR_FOLLOWER" }
        case 7 { "REQUEST_TIMED_OUT" }
        case 8 { "BROKER_NOT_AVAILABLE" }
        case 9 { "REPLICA_NOT_AVAILABLE" }
        case 10 { "MESSAGE_TOO_LARGE" }
        case 11 { "STALE_CONTROLLER_EPOCH" }
        case 12 { "OFFSET_METADATA_TOO_LARGE" }
        case 13 { "NETWORK_EXCEPTION" }
        case 14 { "COORDINATOR_LOAD_IN_PROGRESS" }
        case 15 { "COORDINATOR_NOT_AVAILABLE" }
        case 16 { "NOT_COORDINATOR" }
        case 17 { "INVALID_TOPIC_EXCEPTION" }
        case 18 { "RECORD_LIST_TOO_LARGE" }
        case 19 { "NOT_ENOUGH_REPLICAS" }
        case 20 { "NOT_ENOUGH_REPLICAS_AFTER_APPEND" }
        case 21 { "INVALID_REQUIRED_ACKS" }
        case 22 { "ILLEGAL_GENERATION" }
        case 23 { "INCONSISTENT_GROUP_PROTOCOL" }
        case 24 { "INVALID_GROUP_ID" }
        case 25 { "UNKNOWN_MEMBER_ID" }
        case 26 { "INVALID_SESSION_TIMEOUT" }
        case 27 { "REBALANCE_IN_PROGRESS" }
        case 28 { "INVALID_COMMIT_OFFSET_SIZE" }
        case 29 { "TOPIC_AUTHORIZATION_FAILED" }
        case 30 { "GROUP_AUTHORIZATION_FAILED" }
        case 31 { "CLUSTER_AUTHORIZATION_FAILED" }
        case 32 { "INVALID_TIMESTAMP" }
        case 33 { "UNSUPPORTED_SASL_MECHANISM" }
        case 34 { "ILLEGAL_SASL_STATE" }
        case 35 { "UNSUPPORTED_VERSION" }
        case 36 { "TOPIC_ALREADY_EXISTS" }
        case 37 { "INVALID_PARTITIONS" }
        case 38 { "INVALID_REPLICATION_FACTOR" }
        case 39 { "INVALID_REPLICA_ASSIGNMENT" }
        case 40 { "INVALID_CONFIG" }
        case 41 { "NOT_CONTROLLER" }
        case 42 { "INVALID_REQUEST" }
        case 43 { "UNSUPPORTED_FOR_MESSAGE_FORMAT" }
        case 44 { "POLICY_VIOLATION" }
        case 45 { "OUT_OF_ORDER_SEQUENCE_NUMBER" }
        case 46 { "DUPLICATE_SEQUENCE_NUMBER" }
        case 47 { "INVALID_PRODUCER_EPOCH" }
        case 48 { "INVALID_TXN_STATE" }
        case 49 { "INVALID_PRODUCER_ID_MAPPING" }
        case 50 { "INVALID_TRANSACTION_TIMEOUT" }
        case 51 { "CONCURRENT_TRANSACTIONS" }
        case 52 { "TRANSACTION_COORDINATOR_FENCED" }
        case 53 { "TRANSACTIONAL_ID_AUTHORIZATION_FAILED" }
        case 54 { "SECURITY_DISABLED" }
        case 55 { "OPERATION_NOT_ATTEMPTED" }
        case 56 { "KAFKA_STORAGE_ERROR" }
        case 57 { "LOG_DIR_NOT_FOUND" }
        case 58 { "SASL_AUTHENTICATION_FAILED" }
        case 59 { "UNKNOWN_PRODUCER_ID" }
        case 60 { "REASSIGNMENT_IN_PROGRESS" }
        case 61 { "DELEGATION_TOKEN_AUTH_DISABLED" }
        case 62 { "DELEGATION_TOKEN_NOT_FOUND" }
        case 63 { "DELEGATION_TOKEN_OWNER_MISMATCH" }
        case 64 { "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED" }
        case 65 { "DELEGATION_TOKEN_AUTHORIZATION_FAILED" }
        case 66 { "DELEGATION_TOKEN_EXPIRED" }
        case 67 { "INVALID_PRINCIPAL_TYPE" }
        case 68 { "NON_EMPTY_GROUP" }
        case 69 { "GROUP_ID_NOT_FOUND" }
        case 70 { "FETCH_SESSION_ID_NOT_FOUND" }
        case 71 { "INVALID_FETCH_SESSION_EPOCH" }
        case 72 { "LISTENER_NOT_FOUND" }
        case 73 { "TOPIC_DELETION_DISABLED" }
        case 74 { "FENCED_LEADER_EPOCH" }
        case 75 { "UNKNOWN_LEADER_EPOCH" }
        case 76 { "UNSUPPORTED_COMPRESSION_TYPE" }
        case 77 { "STALE_BROKER_EPOCH" }
        case 78 { "OFFSET_NOT_AVAILABLE" }
        case 79 { "MEMBER_ID_REQUIRED" }
        case 80 { "PREFERRED_LEADER_NOT_AVAILABLE" }
        case 81 { "GROUP_MAX_SIZE_REACHED" }
        case 82 { "FENCED_INSTANCE_ID" }
        case 83 { "ELIGIBLE_LEADERS_NOT_AVAILABLE" }
        case 84 { "ELECTION_NOT_NEEDED" }
        case 85 { "NO_REASSIGNMENT_IN_PROGRESS" }
        case 86 { "GROUP_SUBSCRIBED_TO_TOPIC" }
        case 87 { "INVALID_RECORD" }
        case 88 { "UNSTABLE_OFFSET_COMMIT" }
        case 89 { "THROTTLING_QUOTA_EXCEEDED" }
        case 90 { "PRODUCER_FENCED" }
        case 91 { "RESOURCE_NOT_FOUND" }
        case 92 { "DUPLICATE_RESOURCE" }
        case 93 { "UNACCEPTABLE_CREDENTIAL" }
        case 94 { "INCONSISTENT_VOTER_SET" }
        case 95 { "INVALID_UPDATE_VERSION" }
        case 96 { "FEATURE_UPDATE_FAILED" }
        case 97 { "PRINCIPAL_DESERIALIZATION_FAILURE" }
        case 98 { "SNAPSHOT_NOT_FOUND" }
        case 99 { "POSITION_OUT_OF_RANGE" }
        case 100 { "UNKNOWN_TOPIC_ID" }
        case 101 { "DUPLICATE_BROKER_REGISTRATION" }
        case 102 { "BROKER_ID_NOT_REGISTERED" }
        case 103 { "INCONSISTENT_TOPIC_ID" }
        case 104 { "INCONSISTENT_CLUSTER_ID" }
        case 105 { "TRANSACTIONAL_ID_NOT_FOUND" }
        case 106 { "FETCH_SESSION_TOPIC_ID_ERROR" }
        case 107 { "INELIGIBLE_REPLICA" }
        case 108 { "NEW_LEADER_ELECTED" }
        case 109 { "OFFSET_MOVED_TO_TIERED_STORAGE" }
        case 110 { "FENCED_MEMBER_EPOCH" }
        case 111 { "UNRELEASED_INSTANCE_ID" }
        case 112 { "UNSUPPORTED_ASSIGNOR" }
        case 113 { "STALE_MEMBER_EPOCH" }
        case 114 { "MISMATCHED_ENDPOINT_TYPE" }
        case 115 { "UNSUPPORTED_ENDPOINT_TYPE" }
        case 116 { "UNKNOWN_CONTROLLER_ID" }
        case 117 { "UNKNOWN_SUBSCRIPTION_ID" }
        case 118 { "TELEMETRY_TOO_LARGE" }
        case 119 { "INVALID_REGISTRATION" }
        case 120 { "TRANSACTION_ABORTABLE" }
        default { "UNKNOWN_ERROR_" + to_str(code) }
    };
}

// Whether a request that failed with code may succeed if retried (after
// a metadata refresh or a backoff), as the Java client classifies them.
pub fn is_retriable(code: int) -> bool {
    return switch code {
        case 2, 3, 5, 6, 7, 9, 13, 14, 15, 16, 19, 20, 41, 51, 56, 70, 71, 72, 74, 75, 78, 80, 83, 84, 88, 89, 100, 103, 106 { true }
        default { false }
    };
}

// An error code as text: "NOT_LEADER_OR_FOLLOWER (6)".
pub fn error_text(code: int) -> str {
    return error_name(code) + " (" + to_str(code) + ")";
}

// The error for code, with what failed and the broker's message if any.
fn kerr(what: str, code: int, msg: str) -> str {
    let s = "kafka: " + what + ": " + error_text(code);
    if msg != "" {
        s = s + ": " + msg;
    }
    return s;
}
