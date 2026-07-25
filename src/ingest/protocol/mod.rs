mod content;
mod decode;
mod duration;
mod event;
mod identifiers;
mod intent;
mod metadata;
mod state;
mod timestamp;
mod tokens;
mod wire;

#[cfg(test)]
pub(in crate::ingest) use content::is_transport_context_envelope;
pub(in crate::ingest) use content::{normalized_metadata_value, redact_and_bound};
#[cfg(test)]
pub(in crate::ingest) use decode::{
    AgentObservation, AgentStateTransition, ConversationStateTransition, LifecycleStateTransition,
    MetadataStateTransition, OrdinaryStateTransition, TerminalLifecycleKind, ThreadStateTransition,
    ToolStateTransition,
};
pub(in crate::ingest) use decode::{
    ConversationIntent, ConversationNoop, DecodedAgentRecord, DecodedConversationRecord,
    DecodedLifecycleRecord, DecodedMetadataRecord, DecodedOrdinaryRecord, DecodedThreadStateRecord,
    DecodedToolRecord, GoalUpdate, LifecycleIntent, MessageActivity, MessageIntent, MessageRole,
    MetadataIntent, MetadataUpdate, ObservedAgentActivity, OrdinaryIntent, OrdinaryNoop,
    TaskComplete, TaskStarted, TerminalLifecycle, ThreadStateIntent, ToolComplete, ToolCompletion,
    ToolEnrich, ToolIntent, ToolStart, ToolTerminal, decode_record, message_event,
};
#[cfg(test)]
pub(in crate::ingest) use decode::{
    DeferredMessageId, ToolTerminalStatus, decode_conversation_record, decode_usage_record,
};
#[cfg(test)]
pub(in crate::ingest) use duration::{MAX_STORED_DURATION_MS, duration_ms, raw_duration_ms};
pub(in crate::ingest) use event::{
    CompactionMetadata, MetadataScalar, PROJECTED_EVENT_BODY_CHARS, ProjectedCallId,
    ProjectedEvent, ProjectedEventMetadata, UnknownMetadata,
};
#[cfg(test)]
pub(in crate::ingest) use event::{PROJECTED_EVENT_LABEL_CHARS, SubagentMetadata};
#[cfg(test)]
pub(in crate::ingest) use identifiers::PROJECTED_IDENTIFIER_CHARS;
#[cfg(test)]
pub(in crate::ingest) use identifiers::is_owner_native_turn;
pub(in crate::ingest) use identifiers::{looks_like_uuid, normalized_relational_identifier};
#[cfg(test)]
pub(in crate::ingest) use intent::CursorOnlyReason;
pub(in crate::ingest) use intent::{DecodedRecord, UsageIntent};
#[cfg(test)]
pub(in crate::ingest) use metadata::SessionMetadata;
pub(in crate::ingest) use metadata::{OwnerMeta, decode_owner_record};
pub(in crate::ingest) use state::CursorState;
#[cfg(test)]
pub(in crate::ingest) use timestamp::canonical_source_timestamp;
pub(in crate::ingest) use tokens::checked_token_count;
#[cfg(test)]
pub(in crate::ingest) use tokens::{
    last_token_usage_is_total_only_hint, parse_token_usage, parse_total_token_usage,
};
