mod agents;
mod conversation;
mod lifecycle;
mod metadata;
mod ordinary;
mod record;
mod thread_state;
mod tools;
mod usage;

#[cfg(test)]
pub(in crate::ingest) use agents::{AgentObservation, AgentStateTransition};
pub(in crate::ingest) use agents::{
    DecodedAgentRecord, ObservedAgentActivity, decode_agent_record,
};
#[cfg(test)]
pub(in crate::ingest) use conversation::ConversationStateTransition;
#[cfg(test)]
pub(in crate::ingest) use conversation::DeferredMessageId;
pub(in crate::ingest) use conversation::{
    ConversationIntent, ConversationNoop, DecodedConversationRecord, MessageActivity,
    MessageIntent, MessageRole, decode_conversation_record, message_event,
};
pub(in crate::ingest) use lifecycle::{
    DecodedLifecycleRecord, LifecycleIntent, TaskComplete, TaskStarted, TerminalLifecycle,
    decode_lifecycle_record,
};
#[cfg(test)]
pub(in crate::ingest) use lifecycle::{LifecycleStateTransition, TerminalLifecycleKind};
#[cfg(test)]
pub(in crate::ingest) use metadata::MetadataStateTransition;
pub(in crate::ingest) use metadata::{
    DecodedMetadataRecord, MetadataIntent, MetadataUpdate, decode_session_metadata_record,
    decode_title_event_record,
};
#[cfg(test)]
pub(in crate::ingest) use ordinary::OrdinaryStateTransition;
pub(in crate::ingest) use ordinary::{
    DecodedOrdinaryRecord, OrdinaryIntent, OrdinaryNoop, decode_ordinary_record,
};
pub(in crate::ingest) use record::decode_record;
#[cfg(test)]
pub(in crate::ingest) use thread_state::ThreadStateTransition;
pub(in crate::ingest) use thread_state::{
    DecodedThreadStateRecord, GoalUpdate, ThreadStateIntent, decode_thread_state_record,
};
#[cfg(test)]
pub(in crate::ingest) use tools::ToolStateTransition;
#[cfg(test)]
pub(in crate::ingest) use tools::ToolTerminalStatus;
pub(in crate::ingest) use tools::{
    DecodedToolRecord, ToolComplete, ToolCompletion, ToolEnrich, ToolIntent, ToolStart,
    ToolTerminal, decode_event_tool_record, decode_response_tool_record,
};
pub(in crate::ingest) use usage::decode_usage_record;
