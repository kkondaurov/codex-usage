use super::{
    decode::{
        DecodedAgentRecord, DecodedConversationRecord, DecodedLifecycleRecord,
        DecodedMetadataRecord, DecodedOrdinaryRecord, DecodedThreadStateRecord, DecodedToolRecord,
    },
    state::CursorState,
    tokens::TokenUsage,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum DecodedRecord {
    Usage(DecodedUsageRecord),
    CursorOnly(DecodedCursorOnlyRecord),
    Metadata(DecodedMetadataRecord),
    Ordinary(DecodedOrdinaryRecord),
    ThreadState(DecodedThreadStateRecord),
    Conversation(DecodedConversationRecord),
    Tool(DecodedToolRecord),
    Lifecycle(DecodedLifecycleRecord),
    Agent(DecodedAgentRecord),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedUsageRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: CursorTransition,
    pub(in crate::ingest) intent: UsageIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct CursorTransition {
    pub(in crate::ingest) last_timestamp: String,
    pub(in crate::ingest) next_cumulative: TokenUsage,
}

impl CursorTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
        state.cumulative = self.next_cumulative;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct DecodedCursorOnlyRecord {
    pub(in crate::ingest) source_line: u64,
    pub(in crate::ingest) timestamp: String,
    pub(in crate::ingest) transition: CursorOnlyTransition,
    pub(in crate::ingest) reason: CursorOnlyReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct CursorOnlyTransition {
    pub(in crate::ingest) last_timestamp: String,
}

impl CursorOnlyTransition {
    pub(in crate::ingest) fn apply_to(&self, state: &mut CursorState) {
        state.last_timestamp = Some(self.last_timestamp.clone());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum CursorOnlyReason {
    InheritedForkReplay,
    AwaitingNativeStart,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) struct UsageIntent {
    /// `None` records a valid token-count transition which emits no usage
    /// fact, such as a reset or a pre-native inherited snapshot. A zero value
    /// remains `Some` so duplicate snapshots are distinguishable from resets.
    pub(in crate::ingest) usage: Option<TokenUsage>,
}
