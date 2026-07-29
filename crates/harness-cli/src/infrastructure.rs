use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, Utc};
use fs2::FileExt;
use rusqlite::{
    config::DbConfig,
    hooks::{AuthAction, AuthContext, Authorization},
    params,
    types::ValueRef,
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde_json::{json, Value};
use thiserror::Error;

use crate::application::{
    BacklogAddInput, BacklogCloseInput, BacklogOutcomeInput, BrownfieldImportResult,
    ChangesetApplyResult, ChangesetStatusResult, ContractDatabaseState, ContractDiscoveryResult,
    DbRebuildResult, DbSnapshotResult, DecisionAddInput, DecisionVerifyResult, HarnessContext,
    ImprovementHealthItem, ImprovementHealthResult, InitResult, IntakeInput, InterventionAddInput,
    InterventionFilter, LegacyReconcileRecord, LegacyReconcileResult, MigrateResult,
    OrchestrationStoryRecord, OutcomeObservationRecord, QueryTable, StoryAddInput,
    StoryBacklogLinkInput, StoryBacklogLinkRecord, StoryCasUpdateInput, StoryCasUpdateResult,
    StoryCompleteResult, StoryDependencyInput, StoryDependencyRecord, StoryHierarchyInput,
    StoryHierarchyRecord, StoryUpdateInput, StoryVerifyResult, ToolRegisterInput, TraceInput,
    WorkGraphResult,
};
use crate::domain::{
    compiled_tool_registry, normalize_token, proposal_key, score_context, score_trace,
    sha256_bytes, sha256_hex, stable_uid, validate_tool_description, AuditFinding, AuditResult,
    BacklogFilter, BacklogRecord, ContextScoreResult, ContextScoreSource, DecisionRecord,
    FrictionRecord, HarnessStats, ImprovementProposal, IntakeRecord, InterventionRecord,
    ProposalEvidence, RiskLane, StoryMatrixRecord, StoryVerifyAllItem, StoryVerifyAllResult,
    StoryVerifyStatus, ToolArgSpec, ToolEntry, TraceRecord, TraceScoreResult, TraceScoreSource,
};

pub type Result<T> = std::result::Result<T, HarnessInfraError>;

static AUTO_CHANGESET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum HarnessInfraError {
    #[error("database not found at {0}. Run: harness init")]
    MissingDatabase(String),
    #[error("schema file missing: {0}")]
    MissingSchema(String),
    #[error("brownfield import: missing {0}")]
    MissingBrownfieldPath(String),
    #[error("decision {0} has no verify_command. Configure one with: harness-cli decision add --id {0} --title <title> --verify \"<command>\"")]
    MissingDecisionVerifyCommand(String),
    #[error("story {0} has no verify_command. Configure one with: harness-cli story update --id {0} --verify \"<command>\"")]
    MissingStoryVerifyCommand(String),
    #[error("story complete: {0}")]
    StoryCompletion(String),
    #[error("story update: story '{0}' not found")]
    StoryNotFound(String),
    #[error("story dependency: a story cannot depend on itself ('{0}')")]
    StoryDependencySelf(String),
    #[error("story dependency: adding '{0}' -> '{1}' would create a cycle")]
    StoryDependencyCycle(String, String),
    #[error("story hierarchy: a story cannot be its own parent ('{0}')")]
    StoryHierarchySelf(String),
    #[error("story hierarchy: adding parent '{0}' -> child '{1}' would create a cycle")]
    StoryHierarchyCycle(String, String),
    #[error("story update conflict: story '{id}' expected status '{expected}' but stored status is '{actual}'")]
    StoryStatusConflict {
        id: String,
        expected: String,
        actual: String,
    },
    #[error("story update conflict: story '{0}' is not runnable")]
    StoryNotRunnable(String),
    #[error("story update: status 'implemented' is completion-only for story '{0}'. Move the story to 'in_progress' or 'changed', then run: harness-cli story complete {0}")]
    StoryImplementedRequiresCompletion(String),
    #[error("changeset identity conflict: run '{id}' was previously applied with SHA-256 '{applied_sha256}', not '{content_sha256}'")]
    ChangesetIdentityConflict {
        id: String,
        applied_sha256: String,
        content_sha256: String,
    },
    #[error("changeset revision conflict: run '{changeset_id}' expected {entity_kind} '{entity_id}' at revision {expected_revision}, but actual revision is {actual_revision:?}")]
    EntityRevisionConflict {
        changeset_id: String,
        entity_kind: String,
        entity_id: String,
        expected_revision: i64,
        actual_revision: Option<i64>,
    },
    #[error("database snapshot: output already exists at {0}")]
    SnapshotOutputExists(String),
    #[error("story backlog: backlog item '{0}' not found")]
    StoryBacklogNotFound(i64),
    #[error(
        "story backlog: backlog item '{0}' requires legacy reconciliation before it can be linked"
    )]
    StoryBacklogLegacy(i64),
    #[error("story backlog: relationship must be resolves or references")]
    StoryBacklogRelationship,
    #[error("story backlog: resolver links require accepted backlog item '{0}'")]
    StoryBacklogResolverRequiresAccepted(i64),
    #[error("story backlog: story '{0}' is terminal and cannot change a resolver link")]
    StoryBacklogTerminalStory(String),
    #[error("story backlog: backlog item '{0}' already has resolver story '{1}'")]
    StoryBacklogResolverExists(i64, String),
    #[error("story backlog: resolver link for backlog item '{0}' is immutable after closure")]
    StoryBacklogResolverImmutable(i64),
    #[error("tool register: tool '{0}' already exists with command '{1}'")]
    ToolAlreadyExists(String, String),
    #[error("tool remove: tool '{0}' not found")]
    ToolNotFound(String),
    #[error("tool register: command '{0}' was not found. Re-run with --force to register anyway.")]
    ToolCommandNotFound(String),
    #[error("{0}")]
    ToolValidation(#[from] crate::domain::ToolValidationError),
    #[error("backlog close: backlog item '{0}' not found")]
    BacklogNotFound(i64),
    #[error("backlog close: keyed lifecycle occurrence '{0}' must be completed through story complete or rejected through propose --reject")]
    KeyedBacklogClose(i64),
    #[error("backlog outcome record: backlog item '{0}' must be an implemented keyed occurrence")]
    BacklogOutcomeNotImplemented(i64),
    #[error("backlog outcome record: status must be confirmed, ineffective, or reverted")]
    BacklogOutcomeStatus,
    #[error("proposal decision: {0}")]
    ProposalDecision(String),
    #[error("legacy reconciliation: {0}")]
    LegacyReconciliation(String),
    #[error("trace '{0}' not found")]
    TraceNotFound(i64),
    #[error("no traces found")]
    NoTraces,
    #[error("story update: nothing to update")]
    EmptyStoryUpdate,
    #[error("query sql: failed to enforce SQLite read-only mode: {0}")]
    QuerySqlReadOnlyConfiguration(String),
    #[error("query sql is read-only; use typed Harness commands, schema migrations, or db changeset apply for mutations")]
    QuerySqlWriteDenied,
    #[error("changeset apply: {0}")]
    InvalidChangeset(String),
    #[error("changeset apply: unsupported operation '{0}'")]
    UnsupportedChangesetOp(String),
    #[error(
        "db rebuild: database already exists at {0}; remove it or choose an empty HARNESS_DB_PATH"
    )]
    RebuildDatabaseExists(String),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Outcome of one `tool check` scan. The CLI reports these facts; the agent
/// applies policy (skip / degrade / use) based on `status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCheckResult {
    pub name: String,
    pub kind: String,
    pub capability: Option<String>,
    pub status: String,
    pub detail: String,
}

pub trait HarnessRepository {
    fn init(&self) -> Result<InitResult>;
    fn migrate(&self) -> Result<MigrateResult>;
    fn import_brownfield(&self) -> Result<BrownfieldImportResult>;
    fn record_intake(&self, input: IntakeInput) -> Result<i64>;
    fn add_story(&self, input: StoryAddInput) -> Result<()>;
    fn update_story(&self, input: StoryUpdateInput) -> Result<()>;
    fn add_story_dependency(&self, input: StoryDependencyInput) -> Result<bool>;
    fn remove_story_dependency(&self, input: StoryDependencyInput) -> Result<bool>;
    fn link_story_backlog(&self, input: StoryBacklogLinkInput) -> Result<bool>;
    fn unlink_story_backlog(&self, story_id: &str, backlog_id: i64) -> Result<bool>;
    fn query_story_backlog_links(
        &self,
        story: Option<&str>,
        backlog_id: Option<i64>,
    ) -> Result<Vec<StoryBacklogLinkRecord>>;
    fn query_story_dependencies(&self, story: Option<&str>) -> Result<Vec<StoryDependencyRecord>>;
    fn add_story_hierarchy(&self, input: StoryHierarchyInput) -> Result<bool>;
    fn remove_story_hierarchy(&self, input: StoryHierarchyInput) -> Result<bool>;
    fn query_story_hierarchy(&self, story: Option<&str>) -> Result<Vec<StoryHierarchyRecord>>;
    fn query_orchestration_stories(&self) -> Result<Vec<OrchestrationStoryRecord>>;
    fn query_work_graph(&self) -> Result<WorkGraphResult>;
    fn update_story_cas(&self, input: StoryCasUpdateInput) -> Result<StoryCasUpdateResult>;
    fn discover_contract(&self) -> Result<ContractDiscoveryResult>;
    fn verify_story(&self, id: &str) -> Result<StoryVerifyResult>;
    fn complete_story(&self, id: &str) -> Result<StoryCompleteResult>;
    fn verify_all_stories(&self) -> Result<StoryVerifyAllResult>;
    fn add_decision(&self, input: DecisionAddInput) -> Result<()>;
    fn verify_decision(&self, id: &str) -> Result<DecisionVerifyResult>;
    fn add_backlog(&self, input: BacklogAddInput) -> Result<i64>;
    fn close_backlog(&self, input: BacklogCloseInput) -> Result<()>;
    fn record_backlog_outcome(
        &self,
        input: BacklogOutcomeInput,
    ) -> Result<OutcomeObservationRecord>;
    fn reconcile_legacy_improvements(&self, apply: bool) -> Result<LegacyReconcileResult>;
    fn register_tool(&self, input: ToolRegisterInput) -> Result<()>;
    fn remove_tool(&self, name: &str) -> Result<()>;
    fn check_tools(&self, name: Option<String>) -> Result<Vec<ToolCheckResult>>;
    fn add_intervention(&self, input: InterventionAddInput) -> Result<i64>;
    fn record_trace(&self, input: TraceInput) -> Result<i64>;
    fn score_trace(&self, id: Option<i64>) -> Result<TraceScoreResult>;
    fn score_context(&self, id: i64) -> Result<ContextScoreResult>;
    fn story_verify_status(&self, id: &str) -> Result<StoryVerifyStatus>;
    fn query_matrix(&self) -> Result<Vec<StoryMatrixRecord>>;
    fn query_backlog(&self, filter: BacklogFilter) -> Result<Vec<BacklogRecord>>;
    fn query_decisions(&self) -> Result<Vec<DecisionRecord>>;
    fn query_intakes(&self) -> Result<Vec<IntakeRecord>>;
    fn query_traces(&self) -> Result<Vec<TraceRecord>>;
    fn query_friction(&self) -> Result<Vec<FrictionRecord>>;
    fn query_tools(
        &self,
        responsibility: Option<String>,
        capability: Option<String>,
    ) -> Result<Vec<ToolEntry>>;
    fn query_interventions(&self, filter: InterventionFilter) -> Result<Vec<InterventionRecord>>;
    fn query_stats(&self) -> Result<HarnessStats>;
    fn query_improvement_health(&self) -> Result<ImprovementHealthResult>;
    fn audit(&self) -> Result<AuditResult>;
    fn audit_record_evidence(&self) -> Result<AuditResult>;
    fn propose(&self, decision: ProposalDecision) -> Result<ProposalResult>;
    fn query_sql(&self, sql: &str) -> Result<QueryTable>;
    fn apply_changeset(&self, path: &Path) -> Result<ChangesetApplyResult>;
    fn changeset_status(&self, path: &Path) -> Result<ChangesetStatusResult>;
    fn snapshot_db(&self, output: &Path) -> Result<DbSnapshotResult>;
    fn rebuild_db(&self, changeset_dir: &Path) -> Result<DbRebuildResult>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalDecision {
    Preview,
    PreviewSuppressed,
    Accept { key: String, schedule: String },
    Reject { key: String, reason: String },
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProposalResult {
    pub proposals: Vec<ImprovementProposal>,
    pub message: Option<String>,
}

#[derive(Debug)]
pub struct SqliteHarnessRepository {
    repo_root: PathBuf,
    db_path: PathBuf,
    schema_dir: PathBuf,
    run_id_override: Option<String>,
    #[cfg(test)]
    verification_env_override: Vec<(String, String)>,
}

#[derive(Debug)]
struct ChangesetAppend {
    path: PathBuf,
    original_len: u64,
    /// Held exclusively while the append is in flight and during rollback so a
    /// concurrent CLI invocation cannot `set_len` our pending bytes (which
    /// previously caused it to clobber a sibling process's committed
    /// operations).
    ///
    /// Invariant: `file` is always `Some` in practice — `ChangesetAppend` is
    /// only constructed in `append_changeset_operations` and that path always
    /// populates this field with the locked descriptor it just opened. The
    /// `None` arm exists only so `rollback_changeset_append` can recover a
    /// best-effort truncation if the field is ever unset, but does not
    /// reintroduce a race because in that branch the helper re-acquires the
    /// exclusive lock and holds it for the whole rollback before dropping.
    file: Option<std::fs::File>,
}

#[derive(Debug)]
struct ParsedChangeset {
    id: String,
    content_sha256: String,
    operations: Vec<Value>,
}

#[derive(Debug, Default)]
struct StoryCompletionContext {
    intake_uid: Option<String>,
    trace_uid: Option<String>,
}

#[derive(Debug)]
struct StoryCompletionWrite {
    already_completed: bool,
    context: StoryCompletionContext,
    closed_backlog_ids: Vec<i64>,
    already_closed_backlog_ids: Vec<i64>,
    referenced_backlog_ids: Vec<i64>,
}

#[derive(Debug, Clone)]
struct LegacyBacklogRow {
    id: i64,
    title: String,
    status: String,
    actual_outcome: Option<String>,
    legacy_payload: Value,
}

#[derive(Debug, Clone)]
struct LegacyEvidenceCapture {
    uid: String,
    source_kind: String,
    source_local_id: i64,
    fingerprint: String,
    canonical_payload: String,
}

#[derive(Debug, Clone)]
struct LegacyReconcileCandidate {
    row: LegacyBacklogRow,
    record: LegacyReconcileRecord,
    backlog_uid: Option<String>,
    evidence: Vec<(ProposalEvidence, Option<LegacyEvidenceCapture>)>,
}

impl SqliteHarnessRepository {
    pub fn new(repo_root: PathBuf, db_path: PathBuf, schema_dir: PathBuf) -> Self {
        let run_id_override = Self::run_id().or_else(|| {
            Self::is_source_tracked_state(&repo_root, &db_path)
                .then(Self::automatic_changeset_run_id)
        });
        Self {
            repo_root,
            db_path,
            schema_dir,
            run_id_override,
            #[cfg(test)]
            verification_env_override: Vec::new(),
        }
    }

    #[cfg(test)]
    fn with_run_id(mut self, run_id: &str) -> Self {
        self.run_id_override = Some(run_id.to_owned());
        self
    }

    fn verification_output(&self, verify_command: &str) -> std::io::Result<std::process::Output> {
        let (shell, flag) = verifier_shell();
        let mut command = Command::new(shell);
        command
            .arg(flag)
            .arg(verify_command)
            .current_dir(&self.repo_root);
        #[cfg(test)]
        for (key, value) in &self.verification_env_override {
            command.env(key, value);
        }
        command
            .env_remove("HARNESS_RUN_ID")
            .env_remove("HARNESS_RUN_MODE")
            .env_remove("HARNESS_DB_PATH")
            .output()
    }

    fn new_uid(prefix: &str, material: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        stable_uid(prefix, &format!("{material}\0{nanos}"))
    }

    fn unix_time_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or_default()
    }

    #[allow(clippy::type_complexity)]
    fn decide_proposal(
        &self,
        connection: &mut Connection,
        proposals: &[ImprovementProposal],
        key: &str,
        schedule: Option<String>,
        rejection_reason: Option<String>,
    ) -> Result<String> {
        let proposal = proposals
            .iter()
            .find(|proposal| proposal.key == key)
            .ok_or_else(|| {
                HarnessInfraError::ProposalDecision(format!(
                    "unknown or stale proposal key '{key}'"
                ))
            })?;
        if let Some(reason) = &rejection_reason {
            if reason.trim().is_empty() {
                return Err(HarnessInfraError::ProposalDecision(
                    "rejection reason must be nonblank".to_owned(),
                ));
            }
        }
        if proposal
            .evidence_items
            .iter()
            .any(|item| item.source_kind == "legacy_snapshot" && item.uid.starts_with("audit."))
        {
            return Err(HarnessInfraError::ProposalDecision(
                "audit proposal evidence is not recorded; run harness-cli audit --record-evidence"
                    .to_owned(),
            ));
        }
        let parsed_schedule = schedule
            .as_deref()
            .map(parse_observation_schedule)
            .transpose()?;
        if let Some((kind, Some(due), _)) = &parsed_schedule {
            debug_assert_eq!(kind, "due_at");
            let future: i64 = connection.query_row(
                "SELECT CASE WHEN datetime(?1) > datetime('now') THEN 1 ELSE 0 END;",
                params![due],
                |row| row.get(0),
            )?;
            if future != 1 {
                return Err(HarnessInfraError::ProposalDecision(
                    "outcome due time must be later than acceptance".to_owned(),
                ));
            }
        }
        let legacy_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM backlog WHERE proposal_key IS NULL AND title=?1;",
            params![proposal.title],
            |row| row.get(0),
        )?;
        if legacy_count > 0 {
            return Err(HarnessInfraError::ProposalDecision(
                "requires legacy reconciliation".to_owned(),
            ));
        }

        self.with_logged_write(connection, |transaction| {
            let existing: Option<(i64, String, String, Option<String>, Option<String>, Option<i64>, Option<String>, Option<String>, i64, String)> = transaction.query_row(
                "SELECT id, uid, status, outcome_schedule_kind, outcome_due_at, outcome_after_traces, notes, rejection_reason, revision, created_at
                 FROM backlog WHERE proposal_key=?1 ORDER BY id DESC LIMIT 1;",
                params![key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?))
            ).optional()?;
            if let Some((id, uid, status, kind, due, traces, notes, stored_reason, revision, created_at)) = existing {
                if matches!(
                    proposal.lifecycle_state.as_str(),
                    "regression" | "reconsideration"
                ) {
                    debug_assert!(matches!(status.as_str(), "implemented" | "rejected"));
                    let occurrence_kind = proposal.lifecycle_state.as_str();
                    let new_uid = Self::new_uid(
                        "blg",
                        &format!("{key}\0{uid}\0{occurrence_kind}"),
                    );
                    let notes = format!(
                        "component: {}; confidence: {}; validation: {}",
                        proposal.component, proposal.confidence, proposal.validation_plan
                    );
                    if let Some((schedule_kind, schedule_due, schedule_traces)) =
                        &parsed_schedule
                    {
                        transaction.execute(
                            "INSERT INTO backlog (uid, proposal_key, predecessor_uid, occurrence_kind, title, discovered_while, current_pain, suggested_improvement, risk, status, predicted_impact, notes, accepted_at, outcome_schedule_kind, outcome_due_at, outcome_after_traces, outcome_baseline_trace_count)
                             VALUES (?1, ?2, ?3, ?4, ?5, 'harness-cli propose', ?6, ?7, ?8, 'accepted', ?9, ?10, datetime('now'), ?11, ?12, ?13, NULL);",
                            params![new_uid, key, uid, occurrence_kind, proposal.title, proposal.evidence, proposal.suggested_action, normalize_token(&proposal.risk), proposal.predicted_impact, notes, schedule_kind, schedule_due, schedule_traces],
                        )?;
                        let new_id = transaction.last_insert_rowid();
                        record_proposal_evidence(transaction, &new_uid, proposal)?;
                        let accepted_at: String = transaction.query_row(
                            "SELECT accepted_at FROM backlog WHERE id=?1",
                            params![new_id],
                            |row| row.get(0),
                        )?;
                        let created_at: String = transaction.query_row(
                            "SELECT created_at FROM backlog WHERE id=?1",
                            params![new_id],
                            |row| row.get(0),
                        )?;
                        return Ok((
                            format!("Accepted {occurrence_kind} proposal {key} as backlog #{new_id}."),
                            vec![proposal_decision_operation(
                                &new_uid,
                                key,
                                "accepted",
                                proposal,
                                Some((schedule_kind, schedule_due.as_deref(), *schedule_traces)),
                                None,
                                Some(&accepted_at),
                                None,
                                Some(&notes),
                                Some(&created_at),
                                None,
                            )],
                        ));
                    }
                    let reason = rejection_reason.as_ref().expect("decision is reject");
                    let rejection_notes = format!(
                        "rejection_reason: {reason}\ncovered_evidence: {}",
                        proposal.evidence
                    );
                    transaction.execute(
                        "INSERT INTO backlog (uid, proposal_key, predecessor_uid, occurrence_kind, title, discovered_while, current_pain, suggested_improvement, risk, status, predicted_impact, notes, closed_at, rejection_reason)
                         VALUES (?1, ?2, ?3, ?4, ?5, 'harness-cli propose', ?6, ?7, ?8, 'rejected', ?9, ?10, datetime('now'), ?11);",
                        params![new_uid, key, uid, occurrence_kind, proposal.title, proposal.evidence, proposal.suggested_action, normalize_token(&proposal.risk), proposal.predicted_impact, rejection_notes, reason],
                    )?;
                    let new_id = transaction.last_insert_rowid();
                    record_proposal_evidence(transaction, &new_uid, proposal)?;
                    let closed_at: String = transaction.query_row(
                        "SELECT closed_at FROM backlog WHERE id=?1",
                        params![new_id],
                        |row| row.get(0),
                    )?;
                    let created_at: String = transaction.query_row(
                        "SELECT created_at FROM backlog WHERE id=?1",
                        params![new_id],
                        |row| row.get(0),
                    )?;
                    return Ok((
                        format!("Rejected {occurrence_kind} proposal {key} as backlog #{new_id}."),
                        vec![proposal_decision_operation(
                            &new_uid,
                            key,
                            "rejected",
                            proposal,
                            None,
                            Some(reason),
                            None,
                            Some(&closed_at),
                            Some(&rejection_notes),
                            Some(&created_at),
                            None,
                        )],
                    ));
                }
                if let Some((requested_kind, requested_due, requested_traces)) = &parsed_schedule {
                    if status == "accepted" {
                        if kind.as_deref() == Some(requested_kind.as_str()) && due == *requested_due && traces == *requested_traces {
                            return Ok((format!("Proposal {key} unchanged: accepted backlog #{id}."), Vec::new()));
                        }
                        return Err(HarnessInfraError::ProposalDecision("conflicting observation boundary for existing acceptance".to_owned()));
                    }
                    if status != "proposed" {
                        return Err(HarnessInfraError::ProposalDecision(format!("cannot accept a {status} occurrence")));
                    }
                    transaction.execute(
                        "UPDATE backlog SET status='accepted', accepted_at=datetime('now'), outcome_schedule_kind=?1, outcome_due_at=?2, outcome_after_traces=?3, outcome_baseline_trace_count=NULL WHERE id=?4;",
                        params![requested_kind, requested_due, requested_traces, id]
                    )?;
                    record_proposal_evidence(transaction, &uid, proposal)?;
                    let accepted_at: String = transaction.query_row("SELECT accepted_at FROM backlog WHERE id=?1", params![id], |row| row.get(0))?;
                    return Ok((format!("Accepted proposal {key} as backlog #{id}. Next: harness-cli intake --type harness_improvement --summary \"<implementation objective>\" --lane normal --story <US-NNN>"), vec![proposal_decision_operation(&uid, key, "accepted", proposal, Some((requested_kind, requested_due.as_deref(), *requested_traces)), None, Some(&accepted_at), None, notes.as_deref(), Some(&created_at), Some(revision))]));
                }
                let reason = rejection_reason.as_ref().expect("decision is reject");
                if status == "rejected" {
                    if stored_rejection_reason(stored_reason.as_deref(), notes.as_deref())
                        == Some(reason.as_str())
                    {
                        return Ok((format!("Proposal {key} unchanged: rejected backlog #{id}."), Vec::new()));
                    }
                    return Err(HarnessInfraError::ProposalDecision("a rejected occurrence cannot be rewritten with a different reason".to_owned()));
                }
                if status == "accepted" {
                    return Err(HarnessInfraError::ProposalDecision("an accepted occurrence cannot be rejected".to_owned()));
                }
                if status != "proposed" {
                    return Err(HarnessInfraError::ProposalDecision(format!("cannot reject a {status} occurrence")));
                }
                let rejection_notes = format!("rejection_reason: {reason}\ncovered_evidence: {}", proposal.evidence);
                transaction.execute("UPDATE backlog SET status='rejected', closed_at=datetime('now'), notes=?1, rejection_reason=?2 WHERE id=?3;", params![rejection_notes, reason, id])?;
                record_proposal_evidence(transaction, &uid, proposal)?;
                let closed_at: String = transaction.query_row("SELECT closed_at FROM backlog WHERE id=?1", params![id], |row| row.get(0))?;
                return Ok((format!("Rejected proposal {key} as backlog #{id}."), vec![proposal_decision_operation(&uid, key, "rejected", proposal, None, Some(reason), None, Some(&closed_at), Some(&rejection_notes), Some(&created_at), Some(revision))]));
            }

            let uid = stable_uid("blg", key);
            let notes = format!("component: {}; confidence: {}; validation: {}", proposal.component, proposal.confidence, proposal.validation_plan);
            if let Some((kind, due, traces)) = &parsed_schedule {
                transaction.execute(
                    "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, discovered_while, current_pain, suggested_improvement, risk, status, predicted_impact, notes, accepted_at, outcome_schedule_kind, outcome_due_at, outcome_after_traces, outcome_baseline_trace_count)
                     VALUES (?1, ?2, 'original', ?3, 'harness-cli propose', ?4, ?5, ?6, 'accepted', ?7, ?8, datetime('now'), ?9, ?10, ?11, NULL);",
                    params![uid, key, proposal.title, proposal.evidence, proposal.suggested_action, normalize_token(&proposal.risk), proposal.predicted_impact, notes, kind, due, traces]
                )?;
                let id = transaction.last_insert_rowid();
                record_proposal_evidence(transaction, &uid, proposal)?;
                let accepted_at: String = transaction.query_row("SELECT accepted_at FROM backlog WHERE id=?1", params![id], |row| row.get(0))?;
                let created_at: String = transaction.query_row("SELECT created_at FROM backlog WHERE id=?1", params![id], |row| row.get(0))?;
                return Ok((format!("Accepted proposal {key} as backlog #{id}. Next: harness-cli intake --type harness_improvement --summary \"<implementation objective>\" --lane normal --story <US-NNN>"), vec![proposal_decision_operation(&uid, key, "accepted", proposal, Some((kind, due.as_deref(), *traces)), None, Some(&accepted_at), None, Some(&notes), Some(&created_at), None)]));
            }
            let reason = rejection_reason.as_ref().expect("decision is reject");
            let rejection_notes = format!("rejection_reason: {reason}\ncovered_evidence: {}", proposal.evidence);
            transaction.execute(
                "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, discovered_while, current_pain, suggested_improvement, risk, status, predicted_impact, notes, closed_at, rejection_reason)
                 VALUES (?1, ?2, 'original', ?3, 'harness-cli propose', ?4, ?5, ?6, 'rejected', ?7, ?8, datetime('now'), ?9);",
                params![uid, key, proposal.title, proposal.evidence, proposal.suggested_action, normalize_token(&proposal.risk), proposal.predicted_impact, rejection_notes, reason]
            )?;
            let id = transaction.last_insert_rowid();
            record_proposal_evidence(transaction, &uid, proposal)?;
            let closed_at: String = transaction.query_row("SELECT closed_at FROM backlog WHERE id=?1", params![id], |row| row.get(0))?;
            let created_at: String = transaction.query_row("SELECT created_at FROM backlog WHERE id=?1", params![id], |row| row.get(0))?;
            Ok((format!("Rejected proposal {key} as backlog #{id}."), vec![proposal_decision_operation(&uid, key, "rejected", proposal, None, Some(reason), None, Some(&closed_at), Some(&rejection_notes), Some(&created_at), None)]))
        })
    }

    fn open_existing(&self) -> Result<Connection> {
        if !self.db_path.exists() {
            return Err(HarnessInfraError::MissingDatabase(
                self.db_path.display().to_string(),
            ));
        }

        let connection = Connection::open(&self.db_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        Ok(connection)
    }

    fn open_existing_read_only(&self) -> Result<Connection> {
        if !self.db_path.exists() {
            return Err(HarnessInfraError::MissingDatabase(
                self.db_path.display().to_string(),
            ));
        }

        let connection = Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        if !connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)? {
            return Err(HarnessInfraError::QuerySqlReadOnlyConfiguration(
                "checkpoint-on-close could not be disabled".to_owned(),
            ));
        }
        if !connection.is_readonly(rusqlite::MAIN_DB)? {
            return Err(HarnessInfraError::QuerySqlReadOnlyConfiguration(
                "main database did not open read-only".to_owned(),
            ));
        }
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "query_only", "ON")?;
        let query_only: i64 = connection.query_row("PRAGMA query_only;", [], |row| row.get(0))?;
        if query_only != 1 {
            return Err(HarnessInfraError::QuerySqlReadOnlyConfiguration(
                "PRAGMA query_only did not remain enabled".to_owned(),
            ));
        }
        connection.authorizer(Some(authorize_query_sql))?;
        Ok(connection)
    }

    fn open_or_create(&self) -> Result<Connection> {
        let connection = Connection::open(&self.db_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        Ok(connection)
    }

    fn schema_version(connection: &Connection) -> Result<i64> {
        let version = connection
            .query_row(
                "SELECT COALESCE(MAX(version),0) FROM schema_version;",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0);
        Ok(version)
    }

    fn query_orchestration_stories_from(
        connection: &Connection,
    ) -> Result<Vec<OrchestrationStoryRecord>> {
        let mut statement = connection.prepare(
            "SELECT s.id, s.title, s.risk_lane, s.contract_doc, s.status,
                    s.verify_command,
                    CASE WHEN s.status='planned'
                               AND length(trim(COALESCE(s.verify_command,''))) > 0
                               AND NOT EXISTS (
                                   SELECT 1 FROM story_dependency d
                                   JOIN story blocker ON blocker.id=d.story_id
                                   WHERE d.blocks_story_id=s.id
                                     AND blocker.status <> 'implemented'
                               )
                         THEN 1 ELSE 0 END AS runnable
             FROM story s ORDER BY s.id;",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(OrchestrationStoryRecord {
                id: row.get(0)?,
                title: row.get(1)?,
                risk_lane: row.get(2)?,
                contract_doc: row.get(3)?,
                status: row.get(4)?,
                verify_command: row.get(5)?,
                runnable: row.get::<_, i64>(6)? == 1,
            })
        })?;
        collect_rows(rows)
    }

    fn query_dependencies_from(connection: &Connection) -> Result<Vec<StoryDependencyRecord>> {
        let mut statement = connection.prepare(
            "SELECT story_id, blocks_story_id FROM story_dependency
             ORDER BY story_id, blocks_story_id;",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(StoryDependencyRecord {
                blocker: row.get(0)?,
                blocked: row.get(1)?,
            })
        })?;
        collect_rows(rows)
    }

    fn query_hierarchy_from(connection: &Connection) -> Result<Vec<StoryHierarchyRecord>> {
        let mut statement = connection.prepare(
            "SELECT parent_story_id, child_story_id FROM story_hierarchy
             ORDER BY parent_story_id, child_story_id;",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(StoryHierarchyRecord {
                parent: row.get(0)?,
                child: row.get(1)?,
            })
        })?;
        collect_rows(rows)
    }

    fn work_graph_from(connection: &Connection) -> Result<WorkGraphResult> {
        let stories = Self::query_orchestration_stories_from(connection)?;
        let dependencies = Self::query_dependencies_from(connection)?;
        let hierarchy = Self::query_hierarchy_from(connection)?;
        let canonical = serde_json::to_vec(&json!({
            "stories": &stories,
            "dependencies": &dependencies,
            "hierarchy": &hierarchy,
        }))?;
        Ok(WorkGraphResult {
            revision: sha256_bytes(&canonical),
            stories,
            dependencies,
            hierarchy,
        })
    }

    fn apply_schema_v1(&self, connection: &Connection) -> Result<()> {
        let schema_path = self.schema_dir.join("001-init.sql");
        if !schema_path.exists() {
            return Err(HarnessInfraError::MissingSchema(
                schema_path.display().to_string(),
            ));
        }

        let schema = fs::read_to_string(schema_path)?;
        connection.execute_batch(&schema)?;
        Ok(())
    }

    fn apply_pending_migrations(
        &self,
        connection: &mut Connection,
        current_version: i64,
    ) -> Result<Vec<i64>> {
        let mut applied = Vec::new();
        for (version, path) in self.migration_files()? {
            if version > current_version {
                let sql = fs::read_to_string(path)?;
                // Wrap each migration in a single transaction so a crash
                // mid-script cannot leave the schema half-applied (e.g.
                // `schema_version` recorded but tables not yet created).
                // Without this, `apply_schema_v1` could record `version = 1`
                // before the CREATE TABLE statements run and brick the DB.
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(&sql)?;
                transaction.commit()?;
                applied.push(version);
            }
        }
        Ok(applied)
    }

    fn migration_files(&self) -> Result<Vec<(i64, PathBuf)>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.schema_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("sql") {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            let Some(prefix) = file_name.split('-').next() else {
                continue;
            };
            let Ok(version) = prefix.trim_start_matches('0').parse::<i64>() else {
                continue;
            };
            files.push((version, path));
        }
        files.sort_by_key(|(version, _)| *version);
        Ok(files)
    }

    fn run_id() -> Option<String> {
        env::var("HARNESS_RUN_ID")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .and_then(|value| {
                if Self::is_safe_run_id(&value) {
                    Some(value)
                } else {
                    // Reject absolute paths, traversal segments, and any character
                    // outside the safe set so HARNESS_RUN_ID cannot escape
                    // `.harness/changesets` when joined into a path.
                    None
                }
            })
    }

    /// Validates that a run id can safely be joined under `.harness/changesets`
    /// without escaping it. Allows the character set used by both automatic
    /// run ids (`run_auto_*`) and explicit ids supplied via the programmatic
    /// test helper. Rejects absolute paths, `..` segments, separators, NULs,
    /// and any other character outside `[A-Za-z0-9._-]`.
    fn is_safe_run_id(run_id: &str) -> bool {
        !run_id.is_empty()
            && run_id.len() <= 128
            && !run_id.starts_with('.')
            && run_id
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-')
            && !run_id.contains("..")
    }

    fn is_source_tracked_state(repo_root: &Path, db_path: &Path) -> bool {
        db_path == repo_root.join("harness.db")
            && repo_root.join("Cargo.toml").is_file()
            && repo_root.join("crates/harness-cli/Cargo.toml").is_file()
    }

    fn automatic_changeset_run_id() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let sequence = AUTO_CHANGESET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        format!(
            "run_auto_{nanos:020}_{:010}_{sequence:06}",
            std::process::id()
        )
    }

    fn changeset_path(&self, run_id: &str) -> PathBuf {
        self.repo_root
            .join(".harness")
            .join("changesets")
            .join(format!("{run_id}.changeset.jsonl"))
    }

    fn with_logged_write<T>(
        &self,
        connection: &mut Connection,
        write: impl FnOnce(&Transaction<'_>) -> Result<(T, Vec<Value>)>,
    ) -> Result<T> {
        let run_id = self.run_id_override.clone().or_else(Self::run_id);
        self.with_logged_write_for_run(connection, run_id.as_deref(), write)
    }

    fn with_logged_write_for_run<T>(
        &self,
        connection: &mut Connection,
        run_id: Option<&str>,
        write: impl FnOnce(&Transaction<'_>) -> Result<(T, Vec<Value>)>,
    ) -> Result<T> {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (result, operations) = write(&transaction)?;
        let append = if let Some(run_id) = run_id {
            self.append_changeset_operations(&transaction, run_id, operations)?
        } else {
            None
        };

        match transaction.commit() {
            Ok(()) => Ok(result),
            Err(error) => {
                if let Some(mut append) = append {
                    rollback_changeset_append(&mut append)?;
                }
                Err(error.into())
            }
        }
    }

    fn append_changeset_operations(
        &self,
        connection: &Connection,
        run_id: &str,
        operations: Vec<Value>,
    ) -> Result<Option<ChangesetAppend>> {
        if operations.is_empty() {
            return Ok(None);
        }

        let path = self.changeset_path(run_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let original_len = fs::metadata(&path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        // Hold an exclusive advisory lock for the entire append+commit
        // window so a concurrent CLI invocation cannot `set_len` the file
        // (its own rollback path) and clobber bytes that another process
        // has already committed.
        file.lock_exclusive()?;

        if original_len == 0 {
            let header = json!({
                "op": "changeset.header",
                "version": 1,
                "run_id": run_id,
                "base_schema_version": Self::schema_version(connection)?,
            });
            writeln!(file, "{}", serde_json::to_string(&header)?)?;
        }

        for operation in operations {
            writeln!(file, "{}", serde_json::to_string(&operation)?)?;
        }
        file.flush()?;
        file.sync_all()?;
        // Keep the lock held; release only on success (ChangesetAppend is
        // dropped) or explicit rollback (which reuses the same descriptor).
        Ok(Some(ChangesetAppend {
            path,
            original_len,
            file: Some(file),
        }))
    }

    fn parse_changeset(&self, path: &Path) -> Result<ParsedChangeset> {
        let bytes = fs::read(path)?;
        let content_sha256 = sha256_bytes(&bytes);
        let content = std::str::from_utf8(&bytes).map_err(|error| {
            HarnessInfraError::InvalidChangeset(format!("{} is not UTF-8: {error}", path.display()))
        })?;
        let mut operations = Vec::new();
        for (index, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let value = serde_json::from_str::<Value>(line).map_err(|error| {
                HarnessInfraError::InvalidChangeset(format!(
                    "{} line {} is not valid JSON: {error}",
                    path.display(),
                    index + 1
                ))
            })?;
            operations.push(value);
        }
        let header = operations
            .first()
            .filter(|value| value.get("op").and_then(Value::as_str) == Some("changeset.header"))
            .ok_or_else(|| {
                HarnessInfraError::InvalidChangeset(
                    "first operation must be changeset.header".to_owned(),
                )
            })?;
        if header.get("version").and_then(Value::as_i64) != Some(1) {
            return Err(HarnessInfraError::InvalidChangeset(
                "changeset.header version must be 1".to_owned(),
            ));
        }
        let id = required_string(header, "run_id")?;
        if id.trim().is_empty() {
            return Err(HarnessInfraError::InvalidChangeset(
                "changeset.header run_id must be nonblank".to_owned(),
            ));
        }
        let base_schema_version = header
            .get("base_schema_version")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                HarnessInfraError::InvalidChangeset(
                    "changeset.header base_schema_version must be an integer".to_owned(),
                )
            })?;
        let schema_maximum = self
            .migration_files()?
            .last()
            .map(|(version, _)| *version)
            .unwrap_or(1);
        if !(1..=schema_maximum).contains(&base_schema_version) {
            return Err(HarnessInfraError::InvalidChangeset(format!(
                "changeset.header base_schema_version {base_schema_version} is outside supported range 1..={schema_maximum}"
            )));
        }
        Ok(ParsedChangeset {
            id,
            content_sha256,
            operations,
        })
    }

    fn import_matrix(&self, connection: &Connection) -> Result<usize> {
        let matrix_path = self.repo_root.join("docs/TEST_MATRIX.md");
        if !matrix_path.exists() {
            return Err(HarnessInfraError::MissingBrownfieldPath(
                matrix_path.display().to_string(),
            ));
        }

        let content = fs::read_to_string(matrix_path)?;
        let mut story_count = 0;
        let mut columns: Option<MatrixColumns> = None;

        for line in content.lines() {
            if !line.trim_start().starts_with('|') {
                continue;
            }

            let fields = markdown_table_fields(line);
            if fields.len() < 2 {
                continue;
            }

            if columns.is_none() {
                let candidate = MatrixColumns::from_header(&fields);
                if candidate.story.is_some() && candidate.status.is_some() {
                    columns = Some(candidate);
                }
                continue;
            }

            let columns = columns.as_ref().expect("matrix columns discovered");
            let id = field_at(&fields, columns.story).unwrap_or_default();
            let token = normalize_token(&id);
            if matches!(
                token.as_str(),
                "" | "story" | "tbd" | "todo" | "example" | "examples"
            ) || id.chars().all(|character| character == '-')
            {
                continue;
            }

            let mut title = field_at(&fields, columns.contract).unwrap_or_else(|| id.clone());
            if title.is_empty() {
                title = id.clone();
            }

            let status =
                normalize_story_status(&field_at(&fields, columns.status).unwrap_or_default());
            let unit = proof_from_cell(&field_at(&fields, columns.unit).unwrap_or_default());
            let integration =
                proof_from_cell(&field_at(&fields, columns.integration).unwrap_or_default());
            let e2e = proof_from_cell(&field_at(&fields, columns.e2e).unwrap_or_default());
            let platform =
                proof_from_cell(&field_at(&fields, columns.platform).unwrap_or_default());
            let evidence = columns
                .evidence
                .and_then(|index| evidence_from_fields(&fields, index));

            connection.execute(
                "INSERT INTO story (
                    id, title, risk_lane, contract_doc, status,
                    unit_proof, integration_proof, e2e_proof, platform_proof,
                    evidence, notes
                 ) VALUES (?1, ?2, 'high_risk', ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                    'Imported from docs/TEST_MATRIX.md by harness import brownfield.'
                 )
                 ON CONFLICT(id) DO UPDATE SET
                    title=excluded.title,
                    contract_doc=excluded.contract_doc,
                    status=excluded.status,
                    unit_proof=excluded.unit_proof,
                    integration_proof=excluded.integration_proof,
                    e2e_proof=excluded.e2e_proof,
                    platform_proof=excluded.platform_proof,
                    evidence=excluded.evidence,
                    notes=excluded.notes;",
                params![
                    id,
                    title,
                    field_at(&fields, columns.contract),
                    status,
                    unit,
                    integration,
                    e2e,
                    platform,
                    evidence,
                ],
            )?;
            story_count += 1;
        }

        Ok(story_count)
    }

    fn import_decisions(&self, connection: &Connection) -> Result<usize> {
        let decisions_dir = self.repo_root.join("docs/decisions");
        if !decisions_dir.is_dir() {
            return Err(HarnessInfraError::MissingBrownfieldPath(
                decisions_dir.display().to_string(),
            ));
        }

        let mut files = Vec::new();
        for entry in fs::read_dir(&decisions_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("md") {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if is_decision_file_name(file_name) {
                files.push(path);
            }
        }
        files.sort();

        let mut decision_count = 0;
        for path in files {
            let content = fs::read_to_string(&path)?;
            let stem = path
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_owned();
            let title = content
                .lines()
                .next()
                .and_then(|line| line.strip_prefix("# "))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(&stem)
                .to_owned();
            let status =
                normalize_decision_status(&markdown_section_first_value(&content, "Status"));
            let doc_path = format!(
                "docs/decisions/{}",
                path.file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default()
            );

            connection.execute(
                "INSERT INTO decision (id, title, status, doc_path, notes)
                 VALUES (?1, ?2, ?3, ?4,
                    'Imported from docs/decisions by harness import brownfield.'
                 )
                 ON CONFLICT(id) DO UPDATE SET
                    title=excluded.title,
                    status=excluded.status,
                    doc_path=excluded.doc_path,
                    notes=excluded.notes;",
                params![stem, title, status, doc_path],
            )?;
            decision_count += 1;
        }

        Ok(decision_count)
    }

    fn import_backlog(&self, connection: &Connection) -> Result<usize> {
        let backlog_path = self.repo_root.join("docs/HARNESS_BACKLOG.md");
        if !backlog_path.exists() {
            return Ok(0);
        }

        let content = fs::read_to_string(backlog_path)?;
        let items = backlog_items(&content);
        let mut imported = 0;
        for item in items {
            if item.title.is_empty() || item.title == "Short name." {
                continue;
            }

            let risk = if item.risk.is_empty() {
                None
            } else {
                RiskLane::from_str(&item.risk)
                    .ok()
                    .map(|value| value.as_db_value().to_owned())
            };
            let status = normalize_backlog_status(&item.status);
            let discovered = empty_to_none(item.discovered_while);
            let pain = empty_to_none(item.current_pain);
            let suggestion = empty_to_none(item.suggested_improvement);

            connection.execute(
                "INSERT INTO backlog (
                    title, discovered_while, current_pain, suggested_improvement,
                    risk, status, notes
                 )
                 SELECT ?1, ?2, ?3, ?4, ?5, ?6,
                    'Imported from docs/HARNESS_BACKLOG.md by harness import brownfield.'
                 WHERE NOT EXISTS (
                    SELECT 1 FROM backlog WHERE title=?1
                 );",
                params![item.title, discovered, pain, suggestion, risk, status],
            )?;
            imported += 1;
        }

        Ok(imported)
    }
}

impl HarnessRepository for SqliteHarnessRepository {
    fn init(&self) -> Result<InitResult> {
        if self.db_path.exists() {
            let connection = self.open_existing()?;
            let current = Self::schema_version(&connection).unwrap_or(0);
            if current == 0 {
                let mut connection = connection;
                self.apply_schema_v1(&connection)?;
                self.apply_pending_migrations(&mut connection, 1)?;
                return Ok(InitResult::MigratedExisting {
                    db_path: self.db_path.clone(),
                });
            }

            return Ok(InitResult::Existing {
                db_path: self.db_path.clone(),
                version: current,
            });
        }

        let mut connection = self.open_or_create()?;
        self.apply_schema_v1(&connection)?;
        self.apply_pending_migrations(&mut connection, 1)?;
        Ok(InitResult::Created {
            db_path: self.db_path.clone(),
        })
    }

    fn migrate(&self) -> Result<MigrateResult> {
        let mut connection = self.open_existing()?;
        let current_version = Self::schema_version(&connection).unwrap_or(0);
        let applied = self.apply_pending_migrations(&mut connection, current_version)?;

        Ok(MigrateResult {
            current_version,
            applied,
        })
    }

    fn import_brownfield(&self) -> Result<BrownfieldImportResult> {
        let connection = self.open_existing()?;
        let stories = self.import_matrix(&connection)?;
        let decisions = self.import_decisions(&connection)?;
        let backlog_items = self.import_backlog(&connection)?;

        Ok(BrownfieldImportResult {
            stories,
            decisions,
            backlog_items,
        })
    }

    fn record_intake(&self, input: IntakeInput) -> Result<i64> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let input_type = input.input_type.as_db_value().to_owned();
            let risk_lane = input.risk_lane.as_db_value().to_owned();
            let risk_flags = input.risk_flags.as_json_text();
            let affected_docs = input.affected_docs.as_json_text();
            let uid = Self::new_uid(
                "ink",
                &format!("{input_type}\0{}\0{risk_lane}", input.summary),
            );
            transaction.execute(
                "INSERT INTO intake (
                    uid, input_type, summary, risk_lane, risk_flags, affected_docs, story_id, notes
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
                params![
                    uid,
                    input_type,
                    input.summary,
                    risk_lane,
                    risk_flags,
                    affected_docs,
                    input.story_id,
                    input.notes,
                ],
            )?;

            let id = transaction.last_insert_rowid();
            let created_at: String = transaction.query_row(
                "SELECT created_at FROM intake WHERE id=?1",
                params![id],
                |row| row.get(0),
            )?;
            Ok((
                id,
                vec![json!({
                    "op": "intake.add",
                    "version": 2,
                    "uid": uid,
                    "payload": {
                        "created_at": created_at,
                        "input_type": input_type,
                        "summary": input.summary,
                        "risk_lane": risk_lane,
                        "risk_flags": risk_flags,
                        "affected_docs": affected_docs,
                        "story_id": input.story_id,
                        "notes": input.notes,
                    },
                })],
            ))
        })
    }

    fn reconcile_legacy_improvements(&self, apply: bool) -> Result<LegacyReconcileResult> {
        let proposals = self.propose(ProposalDecision::PreviewSuppressed)?.proposals;
        let mut connection = self.open_existing()?;
        let candidates = legacy_reconcile_candidates(&connection, &proposals)?;
        let changed = candidates
            .iter()
            .filter(|candidate| candidate.backlog_uid.is_some())
            .count();
        let records = candidates
            .iter()
            .map(|candidate| candidate.record.clone())
            .collect::<Vec<_>>();

        if !apply {
            return Ok(LegacyReconcileResult {
                applied: false,
                changed,
                trace_id: None,
                records,
            });
        }

        self.with_logged_write(&mut connection, |transaction| {
            let captured_at: String =
                transaction.query_row("SELECT datetime('now');", [], |row| row.get(0))?;
            let mut operations = Vec::new();
            let mut applied = 0usize;

            for candidate in &candidates {
                let Some(backlog_uid) = candidate.backlog_uid.as_deref() else {
                    continue;
                };
                let proposal_key = candidate
                    .record
                    .proposal_key
                    .as_deref()
                    .expect("derivable candidate has proposal key");
                let updated = transaction.execute(
                    "UPDATE backlog
                     SET uid=?1, proposal_key=?2, occurrence_kind='original'
                     WHERE id=?3 AND uid IS NULL AND proposal_key IS NULL
                       AND occurrence_kind IS NULL;",
                    params![backlog_uid, proposal_key, candidate.row.id],
                )?;
                if updated != 1 {
                    return Err(HarnessInfraError::LegacyReconciliation(format!(
                        "backlog #{} changed after classification; retry reconciliation",
                        candidate.row.id
                    )));
                }

                let mut evidence_operations = Vec::new();
                for (evidence, capture) in &candidate.evidence {
                    let (source_kind, evidence_uid, evidence_fingerprint, observed_at) =
                        if let Some(capture) = capture {
                            transaction.execute(
                                "INSERT INTO legacy_evidence_snapshot
                                    (uid, source_kind, source_local_id, evidence_fingerprint,
                                     canonical_payload, captured_at)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                                 ON CONFLICT(source_kind, evidence_fingerprint) DO NOTHING;",
                                params![
                                    capture.uid,
                                    capture.source_kind,
                                    capture.source_local_id,
                                    capture.fingerprint,
                                    capture.canonical_payload,
                                    captured_at,
                                ],
                            )?;
                            let snapshot: (String, String) = transaction.query_row(
                                "SELECT uid, captured_at FROM legacy_evidence_snapshot
                                 WHERE source_kind=?1 AND evidence_fingerprint=?2;",
                                params![capture.source_kind, capture.fingerprint],
                                |row| Ok((row.get(0)?, row.get(1)?)),
                            )?;
                            operations.push(json!({
                                "op": "legacy.evidence.capture",
                                "version": 1,
                                "uid": snapshot.0,
                                "payload": {
                                    "source_kind": capture.source_kind,
                                    "source_local_id": capture.source_local_id,
                                    "evidence_fingerprint": capture.fingerprint,
                                    "canonical_payload": capture.canonical_payload,
                                    "captured_at": snapshot.1,
                                }
                            }));
                            (
                                "legacy_snapshot".to_owned(),
                                snapshot.0,
                                capture.fingerprint.clone(),
                                evidence.observed_at.clone(),
                            )
                        } else {
                            (
                                evidence.source_kind.clone(),
                                evidence.uid.clone(),
                                evidence.fingerprint.clone(),
                                evidence.observed_at.clone(),
                            )
                        };
                    transaction.execute(
                        "INSERT OR IGNORE INTO proposal_evidence_link
                            (backlog_uid, source_kind, evidence_uid, evidence_fingerprint, observed_at)
                         VALUES (?1, ?2, ?3, ?4, ?5);",
                        params![
                            backlog_uid,
                            source_kind,
                            evidence_uid,
                            evidence_fingerprint,
                            observed_at
                        ],
                    )?;
                    evidence_operations.push(json!({
                        "source_kind": source_kind,
                        "evidence_uid": evidence_uid,
                        "evidence_fingerprint": evidence_fingerprint,
                        "observed_at": observed_at,
                    }));
                }

                operations.push(json!({
                    "op": "backlog.legacy.reconcile",
                    "version": 1,
                    "uid": backlog_uid,
                    "payload": {
                        "title": candidate.row.title,
                        "legacy_row": candidate.row.legacy_payload,
                        "proposal_key": proposal_key,
                        "occurrence_kind": "original",
                        "evidence": evidence_operations,
                    }
                }));

                if matches!(candidate.row.status.as_str(), "implemented" | "rejected") {
                    if let Some(outcome) = candidate
                        .row
                        .actual_outcome
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                    {
                        let observation_uid = stable_uid("obs", &format!("{backlog_uid}\0legacy"));
                        transaction.execute(
                            "INSERT OR IGNORE INTO backlog_outcome_observation
                                (uid, backlog_uid, ordinal, status, outcome, evidence, observed_at)
                             VALUES (?1, ?2, 1, 'legacy_recorded', ?3,
                                     'migrated from backlog.actual_outcome', ?4);",
                            params![observation_uid, backlog_uid, outcome, captured_at],
                        )?;
                        operations.push(json!({
                            "op": "backlog.outcome.observe",
                            "version": 1,
                            "uid": observation_uid,
                            "payload": {
                                "backlog_uid": backlog_uid,
                                "ordinal": 1,
                                "status": "legacy_recorded",
                                "outcome": outcome,
                                "evidence": "migrated from backlog.actual_outcome",
                                "observed_at": captured_at,
                            }
                        }));
                    }
                }
                applied += 1;
            }

            let trace_id = if applied == 0 {
                None
            } else {
                let trace_uid = Self::new_uid("trc", "legacy improvement reconciliation");
                transaction.execute(
                    "INSERT INTO trace
                        (uid, created_at, task_summary, agent, actions_taken, files_changed,
                         outcome, notes)
                     VALUES (?1, ?2, ?3, 'harness-cli', ?4, ?5, 'completed', ?6);",
                    params![
                        trace_uid,
                        captured_at,
                        format!("Reconciled {applied} legacy improvement row(s)"),
                        json!(["classified legacy improvements", "backfilled derivable lifecycle identity", "captured immutable legacy evidence"]).to_string(),
                        json!(["harness durable legacy lifecycle metadata"]).to_string(),
                        "Manual, ambiguous, and duplicate-candidate rows were left unchanged.",
                    ],
                )?;
                let id = transaction.last_insert_rowid();
                operations.push(json!({
                    "op": "trace.add",
                    "version": 2,
                    "uid": trace_uid,
                    "payload": {
                        "created_at": captured_at,
                        "task_summary": format!("Reconciled {applied} legacy improvement row(s)"),
                        "intake_uid": null,
                        "story_id": null,
                        "agent": "harness-cli",
                        "actions_taken": json!(["classified legacy improvements", "backfilled derivable lifecycle identity", "captured immutable legacy evidence"]).to_string(),
                        "files_read": null,
                        "files_changed": json!(["harness durable legacy lifecycle metadata"]).to_string(),
                        "decisions_made": null,
                        "errors": null,
                        "outcome": "completed",
                        "duration_seconds": null,
                        "token_estimate": null,
                        "harness_friction": null,
                        "notes": "Manual, ambiguous, and duplicate-candidate rows were left unchanged.",
                    }
                }));
                Some(id)
            };

            Ok((
                LegacyReconcileResult {
                    applied: true,
                    changed: applied,
                    trace_id,
                    records,
                },
                operations,
            ))
        })
    }

    fn add_story(&self, input: StoryAddInput) -> Result<()> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let risk_lane = input.risk_lane.as_db_value().to_owned();
            transaction.execute(
                "INSERT INTO story (id, title, risk_lane, contract_doc, verify_command, notes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
                params![
                    input.id,
                    input.title,
                    risk_lane,
                    input.contract_doc,
                    input.verify_command,
                    input.notes,
                ],
            )?;
            let created_at: String = transaction.query_row(
                "SELECT created_at FROM story WHERE id=?1;",
                params![input.id],
                |row| row.get(0),
            )?;
            Ok((
                (),
                vec![json!({
                    "op": "story.add",
                    "version": 2,
                    "id": input.id,
                    "payload": {
                        "created_at": created_at,
                        "title": input.title,
                        "risk_lane": risk_lane,
                        "contract_doc": input.contract_doc,
                        "verify_command": input.verify_command,
                        "notes": input.notes,
                    },
                })],
            ))
        })
    }

    fn update_story(&self, input: StoryUpdateInput) -> Result<()> {
        if input.contract_doc.is_none()
            && input.status.is_none()
            && input.evidence.is_none()
            && input.unit.is_none()
            && input.integration.is_none()
            && input.e2e.is_none()
            && input.platform.is_none()
            && input.verify_command.is_none()
        {
            return Err(HarnessInfraError::EmptyStoryUpdate);
        }

        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            ensure_story_exists(transaction, &input.id)?;
            let expected_revision: i64 = transaction.query_row(
                "SELECT revision FROM story WHERE id=?1;",
                params![input.id],
                |row| row.get(0),
            )?;
            reject_ordinary_story_implementation(&input.id, input.status.as_deref())?;
            let unit = input.unit.map(|value| value.0);
            let integration = input.integration.map(|value| value.0);
            let e2e = input.e2e.map(|value| value.0);
            let platform = input.platform.map(|value| value.0);
            transaction.execute(
                "UPDATE story SET
                    contract_doc=COALESCE(?1, contract_doc),
                    status=COALESCE(?2, status),
                    evidence=COALESCE(?3, evidence),
                    unit_proof=COALESCE(?4, unit_proof),
                    integration_proof=COALESCE(?5, integration_proof),
                    e2e_proof=COALESCE(?6, e2e_proof),
                    platform_proof=COALESCE(?7, platform_proof),
                    verify_command=COALESCE(?8, verify_command)
                 WHERE id=?9;",
                params![
                    input.contract_doc,
                    input.status,
                    input.evidence,
                    unit,
                    integration,
                    e2e,
                    platform,
                    input.verify_command,
                    input.id,
                ],
            )?;

            if transaction.changes() == 0 {
                return Err(HarnessInfraError::StoryNotFound(input.id));
            }
            Ok((
                (),
                vec![json!({
                    "op": "story.update",
                    "version": 3,
                    "id": input.id,
                    "expected_revision": expected_revision,
                    "payload": {
                        "contract_doc": input.contract_doc,
                        "status": input.status,
                        "evidence": input.evidence,
                        "unit_proof": unit,
                        "integration_proof": integration,
                        "e2e_proof": e2e,
                        "platform_proof": platform,
                        "verify_command": input.verify_command,
                    },
                })],
            ))
        })
    }

    fn add_story_dependency(&self, input: StoryDependencyInput) -> Result<bool> {
        if input.blocker == input.blocked {
            return Err(HarnessInfraError::StoryDependencySelf(input.blocker));
        }

        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            ensure_story_exists(transaction, &input.blocker)?;
            ensure_story_exists(transaction, &input.blocked)?;
            if dependency_path_exists(transaction, &input.blocked, &input.blocker)? {
                return Err(HarnessInfraError::StoryDependencyCycle(
                    input.blocker,
                    input.blocked,
                ));
            }
            let changed = transaction.execute(
                "INSERT INTO story_dependency (story_id, blocks_story_id) VALUES (?1, ?2)
                 ON CONFLICT(story_id, blocks_story_id) DO NOTHING;",
                params![input.blocker, input.blocked],
            )? > 0;
            let operations = if changed {
                let created_at: String = transaction.query_row(
                    "SELECT created_at FROM story_dependency WHERE story_id=?1 AND blocks_story_id=?2;",
                    params![input.blocker, input.blocked],
                    |row| row.get(0),
                )?;
                vec![json!({
                    "op": "story.dependency.add",
                    "version": 2,
                    "id": input.blocker,
                    "payload": { "blocked": input.blocked, "created_at": created_at },
                })]
            } else {
                Vec::new()
            };
            Ok((changed, operations))
        })
    }

    fn remove_story_dependency(&self, input: StoryDependencyInput) -> Result<bool> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let changed = transaction.execute(
                "DELETE FROM story_dependency WHERE story_id=?1 AND blocks_story_id=?2;",
                params![input.blocker, input.blocked],
            )? > 0;
            let operations = if changed {
                vec![json!({
                    "op": "story.dependency.remove",
                    "version": 1,
                    "id": input.blocker,
                    "payload": { "blocked": input.blocked },
                })]
            } else {
                Vec::new()
            };
            Ok((changed, operations))
        })
    }

    fn query_story_dependencies(&self, story: Option<&str>) -> Result<Vec<StoryDependencyRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT story_id, blocks_story_id FROM story_dependency
             WHERE (?1 IS NULL OR story_id=?1 OR blocks_story_id=?1)
             ORDER BY story_id, blocks_story_id;",
        )?;
        let rows = statement.query_map(params![story], |row| {
            Ok(StoryDependencyRecord {
                blocker: row.get(0)?,
                blocked: row.get(1)?,
            })
        })?;
        collect_rows(rows)
    }

    fn add_story_hierarchy(&self, input: StoryHierarchyInput) -> Result<bool> {
        if input.parent == input.child {
            return Err(HarnessInfraError::StoryHierarchySelf(input.parent));
        }
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            ensure_story_exists(transaction, &input.parent)?;
            ensure_story_exists(transaction, &input.child)?;
            if hierarchy_path_exists(transaction, &input.child, &input.parent)? {
                return Err(HarnessInfraError::StoryHierarchyCycle(
                    input.parent,
                    input.child,
                ));
            }
            let changed = transaction.execute(
                "INSERT INTO story_hierarchy (parent_story_id, child_story_id)
                 VALUES (?1, ?2) ON CONFLICT(parent_story_id, child_story_id) DO NOTHING;",
                params![input.parent, input.child],
            )? > 0;
            let operations = if changed {
                let created_at: String = transaction.query_row(
                    "SELECT created_at FROM story_hierarchy WHERE parent_story_id=?1 AND child_story_id=?2;",
                    params![input.parent, input.child],
                    |row| row.get(0),
                )?;
                vec![json!({
                    "op": "story.hierarchy.add",
                    "version": 2,
                    "id": input.parent,
                    "payload": { "child": input.child, "created_at": created_at },
                })]
            } else {
                Vec::new()
            };
            Ok((changed, operations))
        })
    }

    fn remove_story_hierarchy(&self, input: StoryHierarchyInput) -> Result<bool> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let changed = transaction.execute(
                "DELETE FROM story_hierarchy WHERE parent_story_id=?1 AND child_story_id=?2;",
                params![input.parent, input.child],
            )? > 0;
            let operations = if changed {
                vec![json!({
                    "op": "story.hierarchy.remove",
                    "version": 1,
                    "id": input.parent,
                    "payload": { "child": input.child },
                })]
            } else {
                Vec::new()
            };
            Ok((changed, operations))
        })
    }

    fn query_story_hierarchy(&self, story: Option<&str>) -> Result<Vec<StoryHierarchyRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT parent_story_id, child_story_id FROM story_hierarchy
             WHERE (?1 IS NULL OR parent_story_id=?1 OR child_story_id=?1)
             ORDER BY parent_story_id, child_story_id;",
        )?;
        let rows = statement.query_map(params![story], |row| {
            Ok(StoryHierarchyRecord {
                parent: row.get(0)?,
                child: row.get(1)?,
            })
        })?;
        collect_rows(rows)
    }

    fn query_orchestration_stories(&self) -> Result<Vec<OrchestrationStoryRecord>> {
        let connection = self.open_existing()?;
        Self::query_orchestration_stories_from(&connection)
    }

    fn query_work_graph(&self) -> Result<WorkGraphResult> {
        let mut connection = self.open_existing()?;
        let transaction = connection.transaction()?;
        let result = Self::work_graph_from(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    fn update_story_cas(&self, input: StoryCasUpdateInput) -> Result<StoryCasUpdateResult> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let (actual, runnable, expected_revision): (String, i64, i64) = transaction
                .query_row(
                    "SELECT s.status,
                            CASE WHEN s.status='planned'
                                      AND length(trim(COALESCE(s.verify_command,''))) > 0
                                      AND NOT EXISTS (
                                          SELECT 1 FROM story_dependency d
                                          JOIN story blocker ON blocker.id=d.story_id
                                          WHERE d.blocks_story_id=s.id
                                            AND blocker.status <> 'implemented'
                                      )
                                 THEN 1 ELSE 0 END,
                            s.revision
                     FROM story s WHERE s.id=?1;",
                    params![input.id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?
                .ok_or_else(|| HarnessInfraError::StoryNotFound(input.id.clone()))?;
            if actual != input.expected_status {
                return Err(HarnessInfraError::StoryStatusConflict {
                    id: input.id,
                    expected: input.expected_status,
                    actual,
                });
            }
            if input.require_runnable && runnable != 1 {
                return Err(HarnessInfraError::StoryNotRunnable(input.id));
            }
            reject_ordinary_story_implementation(&input.id, Some(&input.status))?;
            transaction.execute(
                "UPDATE story SET status=?1 WHERE id=?2;",
                params![input.status, input.id],
            )?;
            let result = StoryCasUpdateResult {
                id: input.id.clone(),
                before_status: actual,
                after_status: input.status.clone(),
                runnable_before: runnable == 1,
            };
            Ok((
                result,
                vec![json!({
                    "op": "story.update",
                    "version": 3,
                    "id": input.id,
                    "expected_revision": expected_revision,
                    "payload": {
                        "status": input.status,
                        "evidence": null,
                        "unit_proof": null,
                        "integration_proof": null,
                        "e2e_proof": null,
                        "platform_proof": null,
                        "verify_command": null,
                    }
                })],
            ))
        })
    }

    fn discover_contract(&self) -> Result<ContractDiscoveryResult> {
        const CAPABILITIES: &[&str] = &[
            "changesets.apply.v1",
            "changesets.status-sha.v1",
            "entity-revision-conflicts.v1",
            "isolated-db-snapshot.v1",
            "isolated-db.v1",
            "semantic-operation-log.v1",
            "stories.read.v1",
            "stories.write.v1",
            "story-dependencies.read-write.v1",
            "story-hierarchy.read-write.v1",
            "work-graph.read.v1",
        ];
        let maximum = self
            .migration_files()?
            .last()
            .map(|(version, _)| *version)
            .unwrap_or(1);
        let (database_state, database_schema_version) = if !self.db_path.exists() {
            (ContractDatabaseState::Missing, None)
        } else {
            use rusqlite::OpenFlags;
            let connection = Connection::open_with_flags(
                &self.db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            let version = Self::schema_version(&connection).unwrap_or(-1);
            let state = if version < 1 || version > maximum {
                ContractDatabaseState::Unsupported
            } else if version < maximum {
                ContractDatabaseState::NeedsMigration
            } else {
                ContractDatabaseState::Current
            };
            (state, Some(version))
        };
        Ok(ContractDiscoveryResult {
            protocol_version: 1,
            cli_version: env!("CARGO_PKG_VERSION").to_owned(),
            schema_minimum: 1,
            schema_maximum: maximum,
            database_state,
            database_schema_version,
            required_environment_variables: vec!["HARNESS_DB_PATH".to_owned()],
            capabilities: CAPABILITIES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        })
    }

    fn link_story_backlog(&self, input: StoryBacklogLinkInput) -> Result<bool> {
        if !matches!(input.relationship.as_str(), "resolves" | "references") {
            return Err(HarnessInfraError::StoryBacklogRelationship);
        }
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let (backlog_uid, backlog_status) = linked_backlog(transaction, input.backlog_id)?;
            let (story_status, story_revision): (String, i64) = transaction.query_row("SELECT status, revision FROM story WHERE id=?1;", params![input.story_id], |row| Ok((row.get(0)?, row.get(1)?))).optional()?.ok_or_else(|| HarnessInfraError::StoryNotFound(input.story_id.clone()))?;
            let previous: Option<String> = transaction.query_row("SELECT relationship FROM story_backlog_link WHERE story_id=?1 AND backlog_uid=?2;", params![input.story_id, backlog_uid], |row| row.get(0)).optional()?;
            if previous.as_deref() == Some(&input.relationship) { return Ok((false, Vec::new())); }
            let mutates_story = input.relationship == "resolves" || previous.as_deref() == Some("resolves");
            if mutates_story {
                validate_resolver_mutation(transaction, &input.story_id, input.backlog_id, &backlog_status, &story_status, &backlog_uid)?;
            }
            let linked_at: String = transaction.query_row("SELECT datetime('now')", [], |row| row.get(0))?;
            let linked_at_unix_ns = Self::unix_time_nanos();
            transaction.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at, linked_at_unix_ns) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(story_id, backlog_uid) DO UPDATE SET relationship=excluded.relationship, linked_at=excluded.linked_at, linked_at_unix_ns=excluded.linked_at_unix_ns;", params![input.story_id, backlog_uid, input.relationship, linked_at, linked_at_unix_ns])?;
            if mutates_story {
                transaction.execute("UPDATE story SET last_verified_at=NULL, last_verified_result=NULL WHERE id=?1;", params![input.story_id])?;
            }
            Ok((true, vec![json!({"op":"story.backlog.link","version":if mutates_story { 3 } else { 2 },"id":input.story_id,"expected_revision":mutates_story.then_some(story_revision),"payload":{"backlog_uid":backlog_uid,"relationship":input.relationship,"linked_at":linked_at,"linked_at_unix_ns":linked_at_unix_ns}})]))
        })
    }

    fn unlink_story_backlog(&self, story_id: &str, backlog_id: i64) -> Result<bool> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let (backlog_uid, backlog_status) = linked_backlog(transaction, backlog_id)?;
            let relationship: Option<String> = transaction.query_row("SELECT relationship FROM story_backlog_link WHERE story_id=?1 AND backlog_uid=?2;", params![story_id, backlog_uid], |row| row.get(0)).optional()?;
            let Some(relationship) = relationship else { return Ok((false, Vec::new())); };
            let mut story_revision = None;
            if relationship == "resolves" {
                let (story_status, revision): (String, i64) = transaction.query_row("SELECT status, revision FROM story WHERE id=?1;", params![story_id], |row| Ok((row.get(0)?, row.get(1)?))).optional()?.ok_or_else(|| HarnessInfraError::StoryNotFound(story_id.to_owned()))?;
                story_revision = Some(revision);
                validate_resolver_mutation(transaction, story_id, backlog_id, &backlog_status, &story_status, &backlog_uid)?;
            }
            transaction.execute("DELETE FROM story_backlog_link WHERE story_id=?1 AND backlog_uid=?2;", params![story_id, backlog_uid])?;
            if relationship == "resolves" { transaction.execute("UPDATE story SET last_verified_at=NULL, last_verified_result=NULL WHERE id=?1;", params![story_id])?; }
            Ok((true, vec![json!({"op":"story.backlog.unlink","version":if story_revision.is_some() { 3 } else { 1 },"id":story_id,"expected_revision":story_revision,"payload":{"backlog_uid":backlog_uid}})]))
        })
    }

    fn query_story_backlog_links(
        &self,
        story: Option<&str>,
        backlog_id: Option<i64>,
    ) -> Result<Vec<StoryBacklogLinkRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare("SELECT link.story_id, backlog.id, link.backlog_uid, link.relationship FROM story_backlog_link AS link JOIN backlog ON backlog.uid=link.backlog_uid WHERE (?1 IS NULL OR link.story_id=?1) AND (?2 IS NULL OR backlog.id=?2) ORDER BY backlog.id, link.relationship, link.story_id;")?;
        let rows = statement.query_map(params![story, backlog_id], |row| {
            Ok(StoryBacklogLinkRecord {
                story_id: row.get(0)?,
                backlog_id: row.get(1)?,
                backlog_uid: row.get(2)?,
                relationship: row.get(3)?,
            })
        })?;
        collect_rows(rows)
    }

    fn verify_story(&self, id: &str) -> Result<StoryVerifyResult> {
        let mut connection = self.open_existing()?;
        let verify_command = connection
            .query_row(
                "SELECT verify_command FROM story WHERE id=?1;",
                params![id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| HarnessInfraError::MissingStoryVerifyCommand(id.to_owned()))?;

        let output = self.verification_output(&verify_command)?;
        let result = if output.status.success() {
            "pass"
        } else {
            "fail"
        }
        .to_owned();
        self.with_logged_write(&mut connection, |transaction| {
            let expected_revision: i64 = transaction.query_row(
                "SELECT revision FROM story WHERE id=?1;",
                params![id],
                |row| row.get(0),
            )?;
            let verified_at: String =
                transaction.query_row("SELECT datetime('now')", [], |row| row.get(0))?;
            transaction.execute(
                "UPDATE story
                 SET last_verified_at=?1, last_verified_result=?2
                 WHERE id=?3;",
                params![verified_at, result, id],
            )?;
            Ok((
                (),
                vec![json!({
                    "op": "story.verify",
                    "version": 3,
                    "id": id,
                    "expected_revision": expected_revision,
                    "payload": {
                        "result": result,
                        "verified_at": verified_at,
                    },
                })],
            ))
        })?;

        Ok(StoryVerifyResult {
            command: verify_command,
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            result,
        })
    }

    fn complete_story(&self, id: &str) -> Result<StoryCompleteResult> {
        let mut connection = self.open_existing()?;
        let (status, verify_command): (String, Option<String>) = connection
            .query_row(
                "SELECT status, verify_command FROM story WHERE id=?1;",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| HarnessInfraError::StoryNotFound(id.to_owned()))?;
        if status == "implemented" {
            let (_, already_closed_backlog_ids, referenced_backlog_ids) =
                story_completion_links(&connection, id)?;
            return Ok(StoryCompleteResult {
                command: verify_command.unwrap_or_default(),
                stdout: String::new(),
                stderr: String::new(),
                result: "already-completed".to_owned(),
                intake_uid: None,
                implementation_trace_uid: None,
                closed_backlog_ids: vec![],
                already_closed_backlog_ids,
                referenced_backlog_ids,
            });
        }
        if !matches!(status.as_str(), "in_progress" | "changed") {
            return Err(HarnessInfraError::StoryCompletion(format!("story '{id}' has status '{status}'; move it to in_progress or changed before completion")));
        }
        let verify_command = verify_command
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| HarnessInfraError::MissingStoryVerifyCommand(id.to_owned()))?;
        let context = story_completion_context(&connection, id)?;
        let (_, already_closed_backlog_ids, referenced_backlog_ids) =
            story_completion_links(&connection, id)?;
        let output = self.verification_output(&verify_command)?;
        let result = if output.status.success() {
            "pass"
        } else {
            "fail"
        }
        .to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if result == "fail" {
            let already_completed = self.with_logged_write(&mut connection, |tx| {
                let (current_status, expected_revision): (String, i64) = tx.query_row(
                    "SELECT status, revision FROM story WHERE id=?1;",
                    params![id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                if current_status == "implemented" {
                    return Ok((true, Vec::new()));
                }
                let verified_at: String =
                    tx.query_row("SELECT datetime('now')", [], |row| row.get(0))?;
                tx.execute("UPDATE story SET last_verified_at=?1, last_verified_result='fail' WHERE id=?2", params![verified_at, id])?;
                Ok((false, vec![json!({"op":"story.verify","version":3,"id":id,"expected_revision":expected_revision,"payload":{"result":"fail","verified_at":verified_at}})]))
            })?;
            return Ok(StoryCompleteResult {
                command: verify_command,
                stdout,
                stderr,
                result: if already_completed {
                    "already-completed".to_owned()
                } else {
                    result
                },
                intake_uid: context.intake_uid,
                implementation_trace_uid: context.trace_uid,
                closed_backlog_ids: vec![],
                already_closed_backlog_ids,
                referenced_backlog_ids,
            });
        }
        let completion = self.with_logged_write(&mut connection, |tx| {
            let (current_status, story_expected_revision): (String, i64) = tx.query_row(
                "SELECT status, revision FROM story WHERE id=?1;",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if current_status == "implemented" {
                let (_, already_closed_backlog_ids, referenced_backlog_ids) =
                    story_completion_links(tx, id)?;
                return Ok((
                    StoryCompletionWrite {
                        already_completed: true,
                        context: StoryCompletionContext::default(),
                        closed_backlog_ids: Vec::new(),
                        already_closed_backlog_ids,
                        referenced_backlog_ids,
                    },
                    Vec::new(),
                ));
            }
            if !matches!(current_status.as_str(), "in_progress" | "changed") {
                return Err(HarnessInfraError::StoryCompletion(format!("story '{id}' changed to status '{current_status}' before completion commit")));
            }
            let context = story_completion_context(tx, id)?;
            let (rows, already_closed_backlog_ids, referenced_backlog_ids) =
                story_completion_links(tx, id)?;
            let trace_count: i64 = tx.query_row("SELECT COUNT(*) FROM trace WHERE uid IS NOT NULL", [], |row| row.get(0))?;
            let completed_at: String = tx.query_row("SELECT datetime('now');", [], |row| row.get(0))?;
            let completion_uid = Self::new_uid("cmp", &format!("{id}\0{completed_at}"));
            tx.execute("UPDATE story SET status='implemented', last_verified_at=?1, last_verified_result='pass' WHERE id=?2", params![completed_at, id])?;
            let mut operations = vec![json!({"op":"story.complete","version":3,"id":id,"expected_revision":story_expected_revision,"payload":{"result":"pass","completion_uid":completion_uid,"completed_at":completed_at}})];
            let mut closed_backlog_ids = Vec::new();
            for (backlog_id, uid, schedule) in &rows {
                let backlog_expected_revision: i64 = tx.query_row(
                    "SELECT revision FROM backlog WHERE uid=?1;",
                    params![uid],
                    |row| row.get(0),
                )?;
                let evidence = json!({"story_id":id,"verify_command":verify_command,"result":"pass","completion_uid":completion_uid,"completed_at":completed_at}).to_string();
                tx.execute("UPDATE backlog SET status='implemented', implemented_at=?1, closed_at=?1, resolution_evidence=?2, outcome_baseline_trace_count=CASE WHEN ?3='trace_count' THEN ?4 ELSE outcome_baseline_trace_count END WHERE uid=?5", params![completed_at, evidence, schedule, trace_count, uid])?;
                operations.push(json!({"op":"backlog.complete","version":3,"uid":uid,"expected_revision":backlog_expected_revision,"payload":{"story_id":id,"trace_baseline":trace_count,"resolution_evidence":evidence,"completed_at":completed_at}}));
                closed_backlog_ids.push(*backlog_id);
            }
            Ok((StoryCompletionWrite {
                already_completed: false,
                context,
                closed_backlog_ids,
                already_closed_backlog_ids,
                referenced_backlog_ids,
            }, operations))
        })?;
        Ok(StoryCompleteResult {
            command: verify_command,
            stdout,
            stderr,
            result: if completion.already_completed {
                "already-completed".to_owned()
            } else {
                result
            },
            intake_uid: completion.context.intake_uid,
            implementation_trace_uid: completion.context.trace_uid,
            closed_backlog_ids: completion.closed_backlog_ids,
            already_closed_backlog_ids: completion.already_closed_backlog_ids,
            referenced_backlog_ids: completion.referenced_backlog_ids,
        })
    }

    fn verify_all_stories(&self) -> Result<StoryVerifyAllResult> {
        let mut connection = self.open_existing()?;
        let mut statement =
            connection.prepare("SELECT id, title, verify_command FROM story ORDER BY id;")?;
        let story_rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        let stories = collect_rows(story_rows)?;
        drop(statement);
        let mut items = Vec::new();

        for (id, title, verify_command) in stories {
            let Some(command) = verify_command.filter(|value| !value.trim().is_empty()) else {
                items.push(StoryVerifyAllItem {
                    id,
                    title,
                    command: None,
                    result: "skipped".to_owned(),
                    stdout: String::new(),
                    stderr: String::new(),
                });
                continue;
            };

            let output = self.verification_output(&command)?;
            let result = if output.status.success() {
                "pass"
            } else {
                "fail"
            }
            .to_owned();
            self.with_logged_write(&mut connection, |transaction| {
                let expected_revision: i64 = transaction.query_row(
                    "SELECT revision FROM story WHERE id=?1;",
                    params![id],
                    |row| row.get(0),
                )?;
                let verified_at: String =
                    transaction.query_row("SELECT datetime('now')", [], |row| row.get(0))?;
                transaction.execute(
                    "UPDATE story
                     SET last_verified_at=?1, last_verified_result=?2
                     WHERE id=?3;",
                    params![verified_at, result, id],
                )?;
                Ok((
                    (),
                    vec![json!({
                        "op": "story.verify",
                        "version": 3,
                        "id": id,
                        "expected_revision": expected_revision,
                        "payload": {
                            "result": result,
                            "verified_at": verified_at,
                        },
                    })],
                ))
            })?;
            items.push(StoryVerifyAllItem {
                id,
                title,
                command: Some(command),
                result,
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }

        Ok(StoryVerifyAllResult { items })
    }

    fn add_decision(&self, input: DecisionAddInput) -> Result<()> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            transaction.execute(
                "INSERT INTO decision (id, title, status, doc_path, verify_command, predicted_impact, notes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
                params![
                    input.id,
                    input.title,
                    input.status,
                    input.doc_path,
                    input.verify_command,
                    input.predicted_impact,
                    input.notes,
                ],
            )?;
            let created_at: String = transaction.query_row(
                "SELECT created_at FROM decision WHERE id=?1;",
                params![input.id],
                |row| row.get(0),
            )?;
            Ok((
                (),
                vec![json!({
                    "op": "decision.add",
                    "version": 2,
                    "id": input.id,
                    "payload": {
                        "created_at": created_at,
                        "title": input.title,
                        "status": input.status,
                        "doc_path": input.doc_path,
                        "verify_command": input.verify_command,
                        "predicted_impact": input.predicted_impact,
                        "notes": input.notes,
                    },
                })],
            ))
        })
    }

    fn verify_decision(&self, id: &str) -> Result<DecisionVerifyResult> {
        let mut connection = self.open_existing()?;
        let verify_command = connection
            .query_row(
                "SELECT verify_command FROM decision WHERE id=?1;",
                params![id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| HarnessInfraError::MissingDecisionVerifyCommand(id.to_owned()))?;

        let (shell, flag) = verifier_shell();
        let status = Command::new(shell)
            .arg(flag)
            .arg(&verify_command)
            .current_dir(&self.repo_root)
            .status()?;
        let result = if status.success() { "pass" } else { "fail" }.to_owned();
        self.with_logged_write(&mut connection, |transaction| {
            let expected_revision: i64 = transaction.query_row(
                "SELECT revision FROM decision WHERE id=?1;",
                params![id],
                |row| row.get(0),
            )?;
            let verified_at: String =
                transaction.query_row("SELECT datetime('now');", [], |row| row.get(0))?;
            transaction.execute(
                "UPDATE decision
                 SET last_verified_at=?1, last_verified_result=?2
                 WHERE id=?3;",
                params![verified_at, result, id],
            )?;
            Ok((
                (),
                vec![json!({
                    "op": "decision.verify",
                    "version": 3,
                    "id": id,
                    "expected_revision": expected_revision,
                    "payload": {
                        "result": result,
                        "verified_at": verified_at,
                    },
                })],
            ))
        })?;

        Ok(DecisionVerifyResult {
            command: verify_command,
            result,
        })
    }

    fn add_backlog(&self, input: BacklogAddInput) -> Result<i64> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let risk = input.risk.map(|value| value.as_db_value().to_owned());
            let uid = Self::new_uid(
                "blg",
                &format!(
                    "{}\0{}",
                    input.title,
                    input.discovered_while.as_deref().unwrap_or_default()
                ),
            );
            transaction.execute(
                "INSERT INTO backlog (
                    uid, title, discovered_while, current_pain, suggested_improvement,
                    risk, predicted_impact, notes
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
                params![
                    uid,
                    input.title,
                    input.discovered_while,
                    input.current_pain,
                    input.suggestion,
                    risk,
                    input.predicted_impact,
                    input.notes,
                ],
            )?;
            let id = transaction.last_insert_rowid();
            let created_at: String = transaction.query_row(
                "SELECT created_at FROM backlog WHERE id=?1",
                params![id],
                |row| row.get(0),
            )?;
            Ok((
                id,
                vec![json!({
                    "op": "backlog.add",
                    "version": 2,
                    "uid": uid,
                    "payload": {
                        "created_at": created_at,
                        "title": input.title,
                        "discovered_while": input.discovered_while,
                        "current_pain": input.current_pain,
                        "suggested_improvement": input.suggestion,
                        "risk": risk,
                        "predicted_impact": input.predicted_impact,
                        "notes": input.notes,
                    },
                })],
            ))
        })
    }

    fn close_backlog(&self, input: BacklogCloseInput) -> Result<()> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let existing: Option<(Option<String>, Option<String>, i64)> = transaction
                .query_row(
                    "SELECT uid, proposal_key, revision FROM backlog WHERE id=?1;",
                    params![input.id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let (uid, proposal_key, expected_revision) =
                existing.ok_or(HarnessInfraError::BacklogNotFound(input.id))?;
            if proposal_key.is_some() && matches!(input.status.as_str(), "implemented" | "rejected")
            {
                return Err(HarnessInfraError::KeyedBacklogClose(input.id));
            }
            let implemented_at: String =
                transaction.query_row("SELECT datetime('now');", [], |row| row.get(0))?;
            transaction.execute(
                "UPDATE backlog
                 SET status=?1, actual_outcome=?2, implemented_at=?3
                 WHERE id=?4;",
                params![input.status, input.actual_outcome, implemented_at, input.id],
            )?;

            if transaction.changes() == 0 {
                return Err(HarnessInfraError::BacklogNotFound(input.id));
            }
            let guarded = uid.is_some();
            Ok((
                (),
                vec![json!({
                    "op": "backlog.close",
                    "version": if guarded { 3 } else { 1 },
                    "id": input.id,
                    "uid": uid,
                    "expected_revision": guarded.then_some(expected_revision),
                    "payload": {
                        "status": input.status,
                        "actual_outcome": input.actual_outcome,
                        "implemented_at": implemented_at,
                    },
                })],
            ))
        })
    }

    fn record_backlog_outcome(
        &self,
        input: BacklogOutcomeInput,
    ) -> Result<OutcomeObservationRecord> {
        if !matches!(
            input.status.as_str(),
            "confirmed" | "ineffective" | "reverted"
        ) {
            return Err(HarnessInfraError::BacklogOutcomeStatus);
        }
        if input.outcome.trim().is_empty() {
            return Err(HarnessInfraError::ProposalDecision(
                "outcome observation text must not be empty".to_owned(),
            ));
        }

        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let occurrence: Option<(Option<String>, Option<String>, String)> = transaction
                .query_row(
                    "SELECT uid, proposal_key, status FROM backlog WHERE id=?1;",
                    params![input.id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let Some((Some(backlog_uid), Some(_), status)) = occurrence else {
                return Err(HarnessInfraError::BacklogOutcomeNotImplemented(input.id));
            };
            if status != "implemented" {
                return Err(HarnessInfraError::BacklogOutcomeNotImplemented(input.id));
            }

            let ordinal: i64 = transaction.query_row(
                "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM backlog_outcome_observation WHERE backlog_uid=?1;",
                params![backlog_uid],
                |row| row.get(0),
            )?;
            let uid = Self::new_uid("obs", &format!("{}\0{}", backlog_uid, ordinal));
            transaction.execute(
                "INSERT INTO backlog_outcome_observation
                    (uid, backlog_uid, ordinal, status, outcome, evidence, observed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'));",
                params![uid, backlog_uid, ordinal, input.status, input.outcome, input.evidence],
            )?;
            let observed_at: String = transaction.query_row(
                "SELECT observed_at FROM backlog_outcome_observation WHERE uid=?1;",
                params![uid],
                |row| row.get(0),
            )?;
            let record = OutcomeObservationRecord {
                backlog_id: input.id,
                ordinal,
                status: input.status.clone(),
                outcome: input.outcome.clone(),
                evidence: input.evidence.clone(),
                observed_at: observed_at.clone(),
            };
            Ok((
                record,
                vec![json!({
                    "op": "backlog.outcome.observe",
                    "version": 1,
                    "uid": uid,
                    "payload": {
                        "backlog_uid": backlog_uid,
                        "ordinal": ordinal,
                        "status": input.status,
                        "outcome": input.outcome,
                        "evidence": input.evidence,
                        "observed_at": observed_at,
                    }
                })],
            ))
        })
    }

    fn register_tool(&self, input: ToolRegisterInput) -> Result<()> {
        validate_tool_description(&input.description)?;
        // Only exec-probed kinds are PATH-checked at register time. mcp/skill/http
        // are not on PATH by nature, so registering intent always succeeds; their
        // presence is resolved later by `tool check` via scan_target.
        let exec_probed = matches!(input.kind.as_str(), "cli" | "binary");
        if exec_probed && !input.force && !command_available(&self.repo_root, &input.command) {
            return Err(HarnessInfraError::ToolCommandNotFound(input.command));
        }

        let mut connection = self.open_existing()?;
        let existing = connection
            .query_row(
                "SELECT command FROM tool WHERE name=?1;",
                params![input.name],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(command) = existing {
            return Err(HarnessInfraError::ToolAlreadyExists(input.name, command));
        }

        self.with_logged_write(&mut connection, |transaction| {
            let args_json = tool_args_json(&input.args);
            transaction.execute(
                "INSERT INTO tool
                    (name, provider, command, description, args, responsibility, since,
                     kind, capability, scan_target, status)
                 VALUES (?1, 'custom', ?2, ?3, ?4, ?5, 'registered', ?6, ?7, ?8, 'unknown');",
                params![
                    input.name,
                    input.command,
                    input.description,
                    args_json,
                    input.responsibility,
                    input.kind,
                    input.capability,
                    input.scan_target,
                ],
            )?;
            let created_at: String = transaction.query_row(
                "SELECT created_at FROM tool WHERE name=?1;",
                params![input.name],
                |row| row.get(0),
            )?;
            Ok((
                (),
                vec![json!({
                    "op": "tool.register",
                    "version": 2,
                    "id": input.name,
                    "payload": {
                        "created_at": created_at,
                        "command": input.command,
                        "description": input.description,
                        "args": args_json,
                        "responsibility": input.responsibility,
                        "kind": input.kind,
                        "capability": input.capability,
                        "scan_target": input.scan_target,
                    },
                })],
            ))
        })
    }

    fn remove_tool(&self, name: &str) -> Result<()> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let expected_revision: i64 = transaction
                .query_row(
                    "SELECT revision FROM tool WHERE name=?1;",
                    params![name],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| HarnessInfraError::ToolNotFound(name.to_owned()))?;
            transaction.execute("DELETE FROM tool WHERE name=?1;", params![name])?;
            if transaction.changes() == 0 {
                return Err(HarnessInfraError::ToolNotFound(name.to_owned()));
            }
            Ok((
                (),
                vec![json!({
                    "op": "tool.remove",
                    "version": 3,
                    "id": name,
                    "expected_revision": expected_revision,
                    "payload": {},
                })],
            ))
        })
    }

    fn check_tools(&self, name: Option<String>) -> Result<Vec<ToolCheckResult>> {
        let mut connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT name, kind, command, scan_target, capability FROM tool
             WHERE (?1 IS NULL OR name = ?1)
             ORDER BY name;",
        )?;
        let rows = statement.query_map(params![name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;
        let tools = collect_rows(rows)?;
        drop(statement);

        let mut results = Vec::with_capacity(tools.len());
        for (name, kind, command, scan_target, capability) in tools {
            let (status, detail) =
                scan_tool_status(&self.repo_root, &kind, &command, scan_target.as_deref());
            self.with_logged_write(&mut connection, |transaction| {
                let expected_revision: i64 = transaction.query_row(
                    "SELECT revision FROM tool WHERE name=?1;",
                    params![name],
                    |row| row.get(0),
                )?;
                let checked_at: String =
                    transaction.query_row("SELECT datetime('now');", [], |row| row.get(0))?;
                transaction.execute(
                    "UPDATE tool SET status=?1, checked_at=?2 WHERE name=?3;",
                    params![status, checked_at, name],
                )?;
                Ok((
                    (),
                    vec![json!({
                        "op": "tool.check",
                        "version": 3,
                        "id": name,
                        "expected_revision": expected_revision,
                        "payload": {
                            "status": status,
                            "detail": detail,
                            "checked_at": checked_at,
                        },
                    })],
                ))
            })?;
            results.push(ToolCheckResult {
                name,
                kind,
                capability,
                status: status.to_owned(),
                detail,
            });
        }
        Ok(results)
    }

    fn add_intervention(&self, input: InterventionAddInput) -> Result<i64> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let uid = Self::new_uid("int", &format!("{}\0{}", input.intervention_type, input.description));
            let trace_uid: Option<String> = input.trace_id.and_then(|trace_id| transaction
                .query_row("SELECT uid FROM trace WHERE id=?1", params![trace_id], |row| row.get(0)).optional().ok().flatten());
            transaction.execute(
                "INSERT INTO intervention (uid, trace_id, story_id, type, description, source, impact)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
                params![
                    uid,
                    input.trace_id,
                    input.story_id,
                    input.intervention_type,
                    input.description,
                    input.source,
                    input.impact,
                ],
            )?;
            let id = transaction.last_insert_rowid();
            let created_at: String = transaction.query_row(
                "SELECT created_at FROM intervention WHERE id=?1", params![id], |row| row.get(0))?;
            Ok((
                id,
                vec![json!({
                    "op": "intervention.add",
                    "version": 2,
                    "uid": uid,
                    "payload": {
                        "created_at": created_at,
                        "trace_uid": trace_uid,
                        "story_id": input.story_id,
                        "type": input.intervention_type,
                        "description": input.description,
                        "source": input.source,
                        "impact": input.impact,
                    },
                })],
            ))
        })
    }

    fn record_trace(&self, input: TraceInput) -> Result<i64> {
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let actions = input.actions.as_json_text();
            let files_read = input.files_read.as_json_text();
            let files_changed = input.files_changed.as_json_text();
            let decisions = input.decisions.as_json_text();
            let errors = input.errors.as_json_text();
            let recorded_at_unix_ns = Self::unix_time_nanos();
            let uid = Self::new_uid(
                "trc",
                &format!(
                    "{}\0{}",
                    input.task_summary,
                    input.story_id.as_deref().unwrap_or_default()
                ),
            );
            let intake_uid: Option<String> = input.intake_id.and_then(|intake_id| {
                transaction
                    .query_row(
                        "SELECT uid FROM intake WHERE id=?1",
                        params![intake_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .ok()
                    .flatten()
            });
            transaction.execute(
                "INSERT INTO trace (
                    uid, intake_uid, recorded_at_unix_ns, task_summary, intake_id, story_id, agent,
                    actions_taken, files_read, files_changed, decisions_made, errors,
                    outcome, duration_seconds, token_estimate, harness_friction, notes
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17);",
                params![
                    uid,
                    intake_uid,
                    recorded_at_unix_ns,
                    input.task_summary,
                    input.intake_id,
                    input.story_id,
                    input.agent,
                    actions,
                    files_read,
                    files_changed,
                    decisions,
                    errors,
                    input.outcome,
                    input.duration_seconds,
                    input.token_estimate,
                    input.friction,
                    input.notes,
                ],
            )?;
            let id = transaction.last_insert_rowid();
            let created_at: String = transaction.query_row(
                "SELECT created_at FROM trace WHERE id=?1",
                params![id],
                |row| row.get(0),
            )?;
            Ok((
                id,
                vec![json!({
                    "op": "trace.add",
                    "version": 2,
                    "uid": uid,
                    "payload": {
                        "created_at": created_at,
                        "recorded_at_unix_ns": recorded_at_unix_ns,
                        "task_summary": input.task_summary,
                        "intake_uid": intake_uid,
                        "story_id": input.story_id,
                        "agent": input.agent,
                        "actions_taken": actions,
                        "files_read": files_read,
                        "files_changed": files_changed,
                        "decisions_made": decisions,
                        "errors": errors,
                        "outcome": input.outcome,
                        "duration_seconds": input.duration_seconds,
                        "token_estimate": input.token_estimate,
                        "harness_friction": input.friction,
                        "notes": input.notes,
                    },
                })],
            ))
        })
    }

    fn score_trace(&self, id: Option<i64>) -> Result<TraceScoreResult> {
        let connection = self.open_existing()?;
        let sql = match id {
            Some(_) => {
                "SELECT
                    trace.id,
                    trace.task_summary,
                    trace.intake_id,
                    intake.risk_lane,
                    trace.agent,
                    trace.actions_taken,
                    trace.files_read,
                    trace.files_changed,
                    trace.decisions_made,
                    trace.errors,
                    trace.outcome,
                    trace.duration_seconds,
                    trace.token_estimate,
                    trace.harness_friction,
                    trace.notes
                 FROM trace
                 LEFT JOIN intake ON intake.id = trace.intake_id
                 WHERE trace.id = ?1"
            }
            None => {
                "SELECT
                    trace.id,
                    trace.task_summary,
                    trace.intake_id,
                    intake.risk_lane,
                    trace.agent,
                    trace.actions_taken,
                    trace.files_read,
                    trace.files_changed,
                    trace.decisions_made,
                    trace.errors,
                    trace.outcome,
                    trace.duration_seconds,
                    trace.token_estimate,
                    trace.harness_friction,
                    trace.notes
                 FROM trace
                 LEFT JOIN intake ON intake.id = trace.intake_id
                 ORDER BY trace.id DESC
                 LIMIT 1"
            }
        };

        let source = if let Some(id) = id {
            connection
                .query_row(sql, params![id], trace_score_source_from_row)
                .optional()?
                .ok_or(HarnessInfraError::TraceNotFound(id))?
        } else {
            connection
                .query_row(sql, [], trace_score_source_from_row)
                .optional()?
                .ok_or(HarnessInfraError::NoTraces)?
        };

        Ok(score_trace(source))
    }

    fn score_context(&self, id: i64) -> Result<ContextScoreResult> {
        let connection = self.open_existing()?;
        let source = connection
            .query_row(
                "SELECT
                    trace.id,
                    intake.risk_lane,
                    trace.story_id,
                    trace.files_read,
                    trace.files_changed,
                    trace.outcome
                 FROM trace
                 LEFT JOIN intake ON intake.id = trace.intake_id
                 WHERE trace.id=?1;",
                params![id],
                |row| {
                    Ok(ContextScoreSource {
                        id: row.get(0)?,
                        risk_lane: row.get(1)?,
                        story_id: row.get(2)?,
                        files_read: row.get(3)?,
                        files_changed: row.get(4)?,
                        outcome: row.get(5)?,
                    })
                },
            )
            .optional()?
            .ok_or(HarnessInfraError::TraceNotFound(id))?;

        Ok(score_context(source))
    }

    fn story_verify_status(&self, id: &str) -> Result<StoryVerifyStatus> {
        let connection = self.open_existing()?;
        connection
            .query_row(
                "SELECT id, verify_command, last_verified_result FROM story WHERE id=?1;",
                params![id],
                |row| {
                    Ok(StoryVerifyStatus {
                        id: row.get(0)?,
                        verify_command: row.get(1)?,
                        last_verified_result: row.get(2)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| HarnessInfraError::StoryNotFound(id.to_owned()))
    }

    fn query_matrix(&self) -> Result<Vec<StoryMatrixRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT s.id, s.title, s.risk_lane, s.status, s.unit_proof, s.integration_proof,
                    s.e2e_proof, s.platform_proof, s.evidence,
                    CASE WHEN s.status='planned'
                               AND length(trim(COALESCE(s.verify_command,''))) > 0
                               AND NOT EXISTS (
                                   SELECT 1 FROM story_dependency d
                                   JOIN story blocker ON blocker.id=d.story_id
                                   WHERE d.blocks_story_id=s.id
                                     AND blocker.status <> 'implemented'
                               )
                         THEN 1 ELSE 0 END AS runnable
             FROM story s ORDER BY s.id;",
        )?;

        let rows = statement.query_map([], |row| {
            Ok(StoryMatrixRecord {
                id: row.get(0)?,
                title: row.get(1)?,
                risk_lane: row.get(2)?,
                status: row.get(3)?,
                unit: row.get(4)?,
                integration: row.get(5)?,
                e2e: row.get(6)?,
                platform: row.get(7)?,
                evidence: row.get(8)?,
                runnable: row.get::<_, i64>(9)? == 1,
            })
        })?;

        collect_rows(rows)
    }

    fn query_backlog(&self, filter: BacklogFilter) -> Result<Vec<BacklogRecord>> {
        let connection = self.open_existing()?;
        let where_clause = match filter {
            BacklogFilter::All => "",
            BacklogFilter::Open => "WHERE status IN ('proposed', 'accepted')",
            BacklogFilter::Closed => "WHERE status IN ('implemented', 'rejected')",
        };
        let sql = format!(
            "SELECT id, title, status, risk, predicted_impact, actual_outcome
             FROM backlog {where_clause} ORDER BY status, id;"
        );
        let mut statement = connection.prepare(&sql)?;

        let rows = statement.query_map([], |row| {
            Ok(BacklogRecord {
                id: row.get(0)?,
                title: row.get(1)?,
                status: row.get(2)?,
                risk: row.get(3)?,
                predicted_impact: row.get(4)?,
                actual_outcome: row.get(5)?,
            })
        })?;

        collect_rows(rows)
    }

    fn query_decisions(&self) -> Result<Vec<DecisionRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT id, title, status, last_verified_at, last_verified_result
             FROM decision ORDER BY id;",
        )?;

        let rows = statement.query_map([], |row| {
            Ok(DecisionRecord {
                id: row.get(0)?,
                title: row.get(1)?,
                status: row.get(2)?,
                last_verified_at: row.get(3)?,
                last_verified_result: row.get(4)?,
            })
        })?;

        collect_rows(rows)
    }

    fn query_intakes(&self) -> Result<Vec<IntakeRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT id, created_at, input_type, risk_lane, summary
             FROM intake ORDER BY id DESC LIMIT 20;",
        )?;

        let rows = statement.query_map([], |row| {
            Ok(IntakeRecord {
                id: row.get(0)?,
                created_at: row.get(1)?,
                input_type: row.get(2)?,
                risk_lane: row.get(3)?,
                summary: row.get(4)?,
            })
        })?;

        collect_rows(rows)
    }

    fn query_traces(&self) -> Result<Vec<TraceRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT id, created_at, outcome, task_summary, harness_friction
             FROM trace ORDER BY id DESC LIMIT 20;",
        )?;

        let rows = statement.query_map([], |row| {
            Ok(TraceRecord {
                id: row.get(0)?,
                created_at: row.get(1)?,
                outcome: row.get(2)?,
                task_summary: row.get(3)?,
                harness_friction: row.get(4)?,
            })
        })?;

        collect_rows(rows)
    }

    fn query_friction(&self) -> Result<Vec<FrictionRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT
                trace.id,
                trace.created_at,
                intake.risk_lane,
                intake.input_type,
                trace.task_summary,
                trace.harness_friction
             FROM trace
             LEFT JOIN intake ON intake.id = trace.intake_id
             WHERE trace.harness_friction IS NOT NULL
             ORDER BY trace.id DESC;",
        )?;

        let rows = statement.query_map([], |row| {
            Ok(FrictionRecord {
                id: row.get(0)?,
                created_at: row.get(1)?,
                risk_lane: row.get(2)?,
                input_type: row.get(3)?,
                task_summary: row.get(4)?,
                harness_friction: row.get(5)?,
            })
        })?;

        collect_rows(rows)
    }

    fn query_tools(
        &self,
        responsibility: Option<String>,
        capability: Option<String>,
    ) -> Result<Vec<ToolEntry>> {
        let connection = self.open_existing()?;
        let mut tools = compiled_tool_registry();
        let mut statement = connection.prepare(
            "SELECT provider, name, command, description, args, responsibility, since,
                    kind, capability, scan_target, status, checked_at
             FROM tool ORDER BY name;",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(ToolEntry {
                provider: row.get(0)?,
                name: row.get(1)?,
                command: row.get(2)?,
                description: row.get(3)?,
                args: parse_stored_tool_args(row.get::<_, Option<String>>(4)?.as_deref()),
                responsibility: row.get(5)?,
                source: "registered".to_owned(),
                since: row.get(6)?,
                kind: row.get(7)?,
                capability: row.get(8)?,
                scan_target: row.get(9)?,
                status: row.get(10)?,
                checked_at: row.get(11)?,
            })
        })?;
        tools.extend(collect_rows(rows)?);
        if let Some(responsibility) = responsibility {
            let normalized = normalize_token(&responsibility);
            tools.retain(|tool| normalize_token(&tool.responsibility) == normalized);
        }
        if let Some(capability) = capability {
            let normalized = normalize_token(&capability);
            tools.retain(|tool| {
                tool.capability
                    .as_deref()
                    .is_some_and(|value| normalize_token(value) == normalized)
            });
        }
        Ok(tools)
    }

    fn query_interventions(&self, filter: InterventionFilter) -> Result<Vec<InterventionRecord>> {
        let connection = self.open_existing()?;
        let mut statement = connection.prepare(
            "SELECT id, created_at, trace_id, story_id, type, description, source, impact
             FROM intervention
             WHERE (?1 IS NULL OR trace_id = ?1)
               AND (?2 IS NULL OR story_id = ?2)
               AND (?3 IS NULL OR type = ?3)
             ORDER BY id DESC;",
        )?;
        let rows = statement.query_map(
            params![filter.trace_id, filter.story_id, filter.intervention_type],
            |row| {
                Ok(InterventionRecord {
                    id: row.get(0)?,
                    created_at: row.get(1)?,
                    trace_id: row.get(2)?,
                    story_id: row.get(3)?,
                    intervention_type: row.get(4)?,
                    description: row.get(5)?,
                    source: row.get(6)?,
                    impact: row.get(7)?,
                })
            },
        )?;
        collect_rows(rows)
    }

    fn query_stats(&self) -> Result<HarnessStats> {
        let connection = self.open_existing()?;
        connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM intake) AS intakes,
                    (SELECT COUNT(*) FROM story) AS stories,
                    (SELECT COUNT(*) FROM decision) AS decisions,
                    (SELECT COUNT(*) FROM backlog) AS backlog_items,
                    (SELECT COUNT(*) FROM trace) AS traces;",
                [],
                |row| {
                    Ok(HarnessStats {
                        intakes: row.get(0)?,
                        stories: row.get(1)?,
                        decisions: row.get(2)?,
                        backlog_items: row.get(3)?,
                        traces: row.get(4)?,
                    })
                },
            )
            .map_err(HarnessInfraError::from)
    }

    fn query_improvement_health(&self) -> Result<ImprovementHealthResult> {
        let audit = self.audit()?;
        let proposals = self.propose(ProposalDecision::PreviewSuppressed)?.proposals;
        let connection = self.open_existing()?;
        let actionable_drift = audit.orphaned_stories.len()
            + audit.unverified_stories.len()
            + audit.unverified_decisions.len()
            + audit.backlog_without_outcomes.len()
            + audit.stale_stories.len()
            + audit.broken_tools.len();
        let mut items = Vec::new();

        if actionable_drift > 0 {
            items.push(ImprovementHealthItem {
                category: "audit".to_owned(),
                id: "entropy".to_owned(),
                title: format!("{actionable_drift} actionable audit finding(s)"),
                state: "drift".to_owned(),
                schedule: "now".to_owned(),
                outcome: String::new(),
                evidence: String::new(),
                next_action: "harness-cli audit".to_owned(),
            });
        }

        for proposal in proposals {
            let category = match proposal.lifecycle_state.as_str() {
                "new" | "pending" | "legacy-unclassified" => "proposal_decision",
                "regression" | "reconsideration" => "recurrence",
                _ => continue,
            };
            let next_action = match proposal.lifecycle_state.as_str() {
                "legacy-unclassified" => {
                    "reconcile legacy identity before accepting or rejecting".to_owned()
                }
                _ => format!(
                    "harness-cli propose --accept {} <schedule> OR --reject {} --reason <reason>",
                    proposal.key, proposal.key
                ),
            };
            items.push(ImprovementHealthItem {
                category: category.to_owned(),
                id: proposal.key,
                title: proposal.title,
                state: proposal.lifecycle_state,
                schedule: "decision_pending".to_owned(),
                outcome: String::new(),
                evidence: proposal.evidence,
                next_action,
            });
        }

        let persisted_pending: Vec<(i64, String, String)> = {
            let mut statement = connection.prepare(
                "SELECT id, proposal_key, title FROM backlog
                 WHERE proposal_key IS NOT NULL AND status='proposed'
                 ORDER BY id;",
            )?;
            let rows =
                statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            collect_rows(rows)?
        };
        for (id, key, title) in persisted_pending {
            if items.iter().any(|item| item.id == key) {
                continue;
            }
            items.push(ImprovementHealthItem {
                category: "proposal_decision".to_owned(),
                id: key.clone(),
                title,
                state: "pending".to_owned(),
                schedule: "decision_pending".to_owned(),
                outcome: String::new(),
                evidence: format!("persisted backlog #{id}"),
                next_action: format!(
                    "harness-cli propose --accept {key} <schedule> OR --reject {key} --reason <reason>"
                ),
            });
        }

        #[allow(clippy::type_complexity)]
        let lifecycle_rows: Vec<(
            i64,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<i64>,
            Option<String>,
            Option<String>,
        )> = {
            let mut statement = connection.prepare(
                "SELECT backlog.id, backlog.title, backlog.status, backlog.uid,
                        backlog.outcome_schedule_kind, backlog.outcome_due_at,
                        backlog.outcome_after_traces, backlog.outcome_baseline_trace_count,
                        observation.status, observation.ordinal,
                        observation.outcome, observation.evidence
                 FROM backlog
                 LEFT JOIN backlog_outcome_observation AS observation
                   ON observation.backlog_uid=backlog.uid
                  AND observation.ordinal=(SELECT MAX(latest.ordinal)
                                           FROM backlog_outcome_observation AS latest
                                           WHERE latest.backlog_uid=backlog.uid)
                 WHERE backlog.proposal_key IS NOT NULL
                   AND backlog.status IN ('accepted','implemented')
                 ORDER BY backlog.id;",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                ))
            })?;
            collect_rows(rows)?
        };
        let current_trace_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM trace WHERE uid IS NOT NULL;",
            [],
            |row| row.get(0),
        )?;

        for (
            id,
            title,
            backlog_status,
            _uid,
            schedule_kind,
            due_at,
            after_traces,
            baseline,
            observation_status,
            ordinal,
            observation_outcome,
            observation_evidence,
        ) in lifecycle_rows
        {
            if backlog_status == "accepted" {
                items.push(ImprovementHealthItem {
                    category: "accepted_work".to_owned(),
                    id: id.to_string(),
                    title,
                    state: "in_progress".to_owned(),
                    schedule: schedule_kind
                        .unwrap_or_else(|| "awaiting_observation_plan".to_owned()),
                    outcome: String::new(),
                    evidence: String::new(),
                    next_action: "complete the designated resolver story with fresh proof"
                        .to_owned(),
                });
                continue;
            }

            let (state, schedule, next_action) = if let Some(status) = observation_status {
                let action = match status.as_str() {
                    "confirmed" => {
                        "continue monitoring; append a later observation if impact changes"
                    }
                    "ineffective" => {
                        "review the ineffective change and decide the next proposal action"
                    }
                    "reverted" => "inspect recurrence evidence before accepting replacement work",
                    "legacy_recorded" => {
                        "preserved legacy evidence; append a modern observation when measured"
                    }
                    _ => "inspect outcome history",
                };
                (
                    status,
                    format!("observed_ordinal_{}", ordinal.unwrap_or_default()),
                    action.to_owned(),
                )
            } else {
                let outcome_command = format!(
                    "harness-cli backlog outcome record --id {id} --status <confirmed|ineffective|reverted> --outcome <text>"
                );
                match schedule_kind.as_deref() {
                    Some("manual") => (
                        "pending_manual".to_owned(),
                        "manual".to_owned(),
                        outcome_command,
                    ),
                    Some("due_at") => {
                        let due = due_at.unwrap_or_default();
                        let reached: i64 = connection.query_row(
                            "SELECT CASE WHEN datetime(?1) <= datetime('now') THEN 1 ELSE 0 END;",
                            params![due],
                            |row| row.get(0),
                        )?;
                        if reached == 1 {
                            ("due".to_owned(), format!("due_at:{due}"), outcome_command)
                        } else {
                            (
                                "scheduled_not_due".to_owned(),
                                format!("due_at:{due}"),
                                "wait until due, or record early if evidence is available"
                                    .to_owned(),
                            )
                        }
                    }
                    Some("trace_count") => {
                        let baseline = baseline.unwrap_or(0);
                        let target = after_traces.unwrap_or(0);
                        if current_trace_count < baseline {
                            ("schedule_error".to_owned(), format!("trace_count:baseline={baseline},current={current_trace_count}"), "rebuild or repair the persisted trace baseline before judging due state".to_owned())
                        } else {
                            let observed = current_trace_count - baseline;
                            if observed >= target {
                                (
                                    "due".to_owned(),
                                    format!("trace_count:{observed}/{target}"),
                                    outcome_command,
                                )
                            } else {
                                ("scheduled_not_due".to_owned(), format!("trace_count:{observed}/{target};remaining={}", target - observed), "wait for the remaining stable traces, or record early if evidence is available".to_owned())
                            }
                        }
                    }
                    _ => (
                        "awaiting_observation_plan".to_owned(),
                        "legacy_null_schedule".to_owned(),
                        "reconcile the missing observation plan; do not guess an overdue date"
                            .to_owned(),
                    ),
                }
            };
            items.push(ImprovementHealthItem {
                category: "outcome_review".to_owned(),
                id: id.to_string(),
                title,
                state,
                schedule,
                outcome: observation_outcome.unwrap_or_default(),
                evidence: observation_evidence.unwrap_or_default(),
                next_action,
            });
        }

        items.sort_by(|left, right| {
            health_category_rank(&left.category)
                .cmp(&health_category_rank(&right.category))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(ImprovementHealthResult {
            entropy_score: audit.entropy_score(),
            actionable_drift,
            items,
        })
    }

    fn audit(&self) -> Result<AuditResult> {
        let connection = self.open_existing()?;
        let mut result = AuditResult {
            orphaned_stories: audit_findings(
                &connection,
                "SELECT story.id, story.title
                 FROM story
                 LEFT JOIN trace ON trace.story_id = story.id
                 WHERE story.status IN ('planned','in_progress') AND trace.id IS NULL
                 ORDER BY story.id;",
            )?,
            unverified_stories: audit_findings(
                &connection,
                "SELECT id, title FROM story
                 WHERE verify_command IS NOT NULL
                   AND TRIM(verify_command) <> ''
                   AND last_verified_result IS NULL
                   AND status <> 'retired'
                 ORDER BY id;",
            )?,
            unverified_decisions: audit_findings(
                &connection,
                "SELECT id, title FROM decision
                 WHERE verify_command IS NOT NULL
                   AND TRIM(verify_command) <> ''
                   AND last_verified_result IS NULL
                 ORDER BY id;",
            )?,
            backlog_without_outcomes: audit_findings(
                &connection,
                "SELECT CAST(id AS TEXT), title FROM backlog
                 WHERE predicted_impact IS NOT NULL
                   AND status='implemented'
                   AND (
                     (proposal_key IS NOT NULL AND NOT EXISTS (
                       SELECT 1 FROM backlog_outcome_observation
                       WHERE backlog_uid=backlog.uid
                     ))
                     OR (proposal_key IS NULL AND actual_outcome IS NULL)
                   )
                 ORDER BY id;",
            )?,
            stale_stories: audit_findings(
                &connection,
                "SELECT story.id, story.title
                 FROM story
                 JOIN trace ON trace.story_id = story.id
                 WHERE story.status <> 'implemented'
                 GROUP BY story.id, story.title
                 HAVING julianday('now') - julianday(MAX(trace.created_at)) > 30
                 ORDER BY story.id;",
            )?,
            broken_tools: Vec::new(),
        };

        let mut statement =
            connection.prepare("SELECT name, command, kind, status FROM tool ORDER BY name;")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for (name, command, kind, status) in collect_rows(rows)? {
            // Exec-probed kinds are checked live against PATH. Scanned kinds
            // (mcp/skill/http) are only "broken" once a scan has positively
            // found them missing; an un-scanned `unknown` is not drift.
            let broken = match kind.as_str() {
                "cli" | "binary" => !command_available(&self.repo_root, &command),
                _ => status == "missing",
            };
            if broken {
                result.broken_tools.push(AuditFinding {
                    id: name,
                    title: command,
                });
            }
        }
        Ok(result)
    }

    fn audit_record_evidence(&self) -> Result<AuditResult> {
        let result = self.audit()?;
        let mut current = Vec::new();
        for (rule, findings) in [
            ("orphaned-story", &result.orphaned_stories),
            ("unverified-story", &result.unverified_stories),
            ("unverified-decision", &result.unverified_decisions),
            (
                "implemented-backlog-without-outcome",
                &result.backlog_without_outcomes,
            ),
            ("stale-story", &result.stale_stories),
            ("broken-tool", &result.broken_tools),
        ] {
            for finding in findings {
                let key = format!("audit.{rule}:v1:entity:{}", finding.id);
                let fingerprint = sha256_hex(&format!("{key}\0{}", finding.title));
                current.push((key, fingerprint));
            }
        }
        let mut connection = self.open_existing()?;
        self.with_logged_write(&mut connection, |transaction| {
            let mut operations = Vec::new();
            let active: Vec<(String, String, String, i64)> = {
                let mut statement = transaction.prepare(
                    "SELECT uid, finding_key, evidence_fingerprint, revision FROM audit_evidence_episode WHERE cleared_at IS NULL",
                )?;
                let rows = statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?;
                collect_rows(rows)?
            };
            for (uid, finding_key, _, expected_revision) in &active {
                if !current.iter().any(|(key, _)| key == finding_key) {
                    transaction.execute(
                        "UPDATE audit_evidence_episode SET cleared_at=datetime('now') WHERE uid=?1",
                        params![uid],
                    )?;
                    operations.push(json!({"op":"audit.evidence.clear","version":3,"uid":uid,"expected_revision":expected_revision,"payload":{"cleared_at": transaction.query_row("SELECT cleared_at FROM audit_evidence_episode WHERE uid=?1", params![uid], |row| row.get::<_, String>(0))?}}));
                }
            }
            for (finding_key, fingerprint) in &current {
                let existing = active.iter().find(|(_, key, _, _)| key == finding_key);
                if existing.is_some_and(|(_, _, old, _)| old == fingerprint) {
                    continue;
                }
                if let Some((old_uid, _, _, expected_revision)) = existing {
                    transaction.execute("UPDATE audit_evidence_episode SET cleared_at=datetime('now') WHERE uid=?1", params![old_uid])?;
                    let cleared_at: String = transaction.query_row(
                        "SELECT cleared_at FROM audit_evidence_episode WHERE uid=?1",
                        params![old_uid],
                        |row| row.get(0),
                    )?;
                    operations.push(json!({
                        "op":"audit.evidence.clear",
                        "version":3,
                        "uid":old_uid,
                        "expected_revision":expected_revision,
                        "payload":{"cleared_at":cleared_at}
                    }));
                }
                let uid = Self::new_uid("aud", finding_key);
                transaction.execute(
                    "INSERT INTO audit_evidence_episode (uid, finding_key, evidence_fingerprint, opened_at) VALUES (?1, ?2, ?3, datetime('now'))",
                    params![uid, finding_key, fingerprint],
                )?;
                let opened_at: String = transaction.query_row("SELECT opened_at FROM audit_evidence_episode WHERE uid=?1", params![uid], |row| row.get(0))?;
                operations.push(json!({"op":"audit.evidence.open","version":1,"uid":uid,"payload":{"finding_key":finding_key,"evidence_fingerprint":fingerprint,"opened_at":opened_at}}));
            }
            Ok((result, operations))
        })
    }

    fn propose(&self, decision: ProposalDecision) -> Result<ProposalResult> {
        let mut connection = self.open_existing()?;
        let audit = self.audit()?;
        let mut proposals = Vec::new();

        for group in repeated_friction(&connection)? {
            let text = group.text;
            let count = group.count;
            let title = format!("Reduce repeated friction: {}", short_title(&text));
            let issue = format!("Failure attribution\0{title}");
            proposals.push(ImprovementProposal {
                key: proposal_key("improvement.proposal", 1, &issue),
                lifecycle_state: "pending".to_owned(),
                title,
                component: "Failure attribution".to_owned(),
                evidence: format!("{count} traces recorded similar friction: {text}"),
                predicted_impact: "Fewer repeated harness friction entries for similar tasks.".to_owned(),
                risk: "normal".to_owned(),
                suggested_action: "Update the relevant Harness docs, templates, or CLI guidance for this friction pattern.".to_owned(),
                validation_plan: "Review the next five related traces and compare friction frequency.".to_owned(),
                confidence: confidence_for_count(count),
                committed_backlog_id: None,
                evidence_items: group.evidence,
                predecessor_uid: None,
                lifecycle_explanation: None,
            });
        }

        for group in repeated_interventions(&connection)? {
            let key = group.text;
            let count = group.count;
            let title = format!("Address repeated intervention: {}", short_title(&key));
            let issue = format!("Intervention recording\0{title}");
            proposals.push(ImprovementProposal {
                key: proposal_key("improvement.proposal", 1, &issue),
                lifecycle_state: "pending".to_owned(),
                title,
                component: "Intervention recording".to_owned(),
                evidence: format!("{count} interventions share the pattern: {key}"),
                predicted_impact: "Fewer repeated human or review interventions for the same issue.".to_owned(),
                risk: "normal".to_owned(),
                suggested_action: "Clarify the relevant operating rule or validation gate that would have caught this earlier.".to_owned(),
                validation_plan: "Future interventions of this type should decrease after the rule change.".to_owned(),
                confidence: confidence_for_count(count),
                committed_backlog_id: None,
                evidence_items: group.evidence,
                predecessor_uid: None,
                lifecycle_explanation: None,
            });
        }

        for (rule, category, findings) in [
            (
                "orphaned-story",
                "orphaned planned or in-progress stories",
                &audit.orphaned_stories,
            ),
            (
                "unverified-story",
                "unverified story commands",
                &audit.unverified_stories,
            ),
            (
                "unverified-decision",
                "unverified decision commands",
                &audit.unverified_decisions,
            ),
            (
                "implemented-backlog-without-outcome",
                "implemented backlog items without outcomes",
                &audit.backlog_without_outcomes,
            ),
            (
                "stale-story",
                "stale unfinished stories",
                &audit.stale_stories,
            ),
            (
                "broken-tool",
                "broken registered tools",
                &audit.broken_tools,
            ),
        ] {
            let count = findings.len();
            if count > 0 {
                let title = format!("Clean up {category}");
                let issue = format!("Entropy auditing\0{title}");
                proposals.push(ImprovementProposal {
                    key: proposal_key("improvement.proposal", 1, &issue),
                    lifecycle_state: "pending".to_owned(),
                    title,
                    component: "Entropy auditing".to_owned(),
                    evidence: format!("Audit found {count} {category}."),
                    predicted_impact: "Lower entropy score and stronger completion evidence.".to_owned(),
                    risk: "tiny".to_owned(),
                    suggested_action: "Resolve the listed audit findings or record why they are intentionally retained.".to_owned(),
                    validation_plan: "Run harness-cli audit and confirm the category count decreases.".to_owned(),
                    confidence: "low".to_owned(),
                    committed_backlog_id: None,
                    evidence_items: audit_proposal_evidence(&connection, rule, findings)?,
                    predecessor_uid: None,
                    lifecycle_explanation: None,
                });
            }
        }

        proposals.sort_by(|left, right| left.key.cmp(&right.key));
        for proposal in &mut proposals {
            classify_proposal(&connection, proposal)?;
        }

        if matches!(decision, ProposalDecision::Preview) {
            proposals.retain(|proposal| proposal.lifecycle_state != "suppressed");
        }

        let message = match decision {
            ProposalDecision::Preview | ProposalDecision::PreviewSuppressed => None,
            ProposalDecision::Accept { key, schedule } => Some(self.decide_proposal(
                &mut connection,
                &proposals,
                &key,
                Some(schedule),
                None,
            )?),
            ProposalDecision::Reject { key, reason } => {
                Some(self.decide_proposal(&mut connection, &proposals, &key, None, Some(reason))?)
            }
        };
        Ok(ProposalResult { proposals, message })
    }

    fn apply_changeset(&self, path: &Path) -> Result<ChangesetApplyResult> {
        // Parsing, header version, base range, and content identity are all
        // established before migration or any other database write.
        let parsed = self.parse_changeset(path)?;
        self.migrate()?;
        let mut connection = self.open_existing()?;
        let applied_sha256: Option<Option<String>> = connection
            .query_row(
                "SELECT content_sha256 FROM changeset_applied WHERE id=?1;",
                params![parsed.id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(applied_sha256) = applied_sha256 {
            if applied_sha256.as_deref() == Some(parsed.content_sha256.as_str()) {
                return Ok(ChangesetApplyResult {
                    id: parsed.id,
                    content_sha256: parsed.content_sha256,
                    applied: false,
                    operations: 0,
                });
            }
            return Err(HarnessInfraError::ChangesetIdentityConflict {
                id: parsed.id,
                applied_sha256: applied_sha256.unwrap_or_else(|| "unknown-legacy-content".into()),
                content_sha256: parsed.content_sha256,
            });
        }

        let transaction = connection.transaction()?;
        let mut context = ChangesetApplyContext::default();
        let mut applied_operations = 0usize;
        for operation in parsed.operations.iter().skip(1) {
            apply_changeset_operation(&transaction, operation, &mut context, &parsed.id)?;
            applied_operations += 1;
        }
        transaction.execute(
            "INSERT INTO changeset_applied (id, path, content_sha256) VALUES (?1, ?2, ?3);",
            params![parsed.id, path.display().to_string(), parsed.content_sha256],
        )?;
        transaction.commit()?;

        Ok(ChangesetApplyResult {
            id: parsed.id,
            content_sha256: parsed.content_sha256,
            applied: true,
            operations: applied_operations,
        })
    }

    fn changeset_status(&self, path: &Path) -> Result<ChangesetStatusResult> {
        let parsed = self.parse_changeset(path)?;
        if !self.db_path.exists() {
            return Ok(ChangesetStatusResult {
                id: parsed.id,
                content_sha256: parsed.content_sha256,
                applied: false,
                operation_count: parsed.operations.len().saturating_sub(1),
            });
        }
        use rusqlite::OpenFlags;
        let connection = Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let has_applied_table: bool = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='changeset_applied';",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !has_applied_table {
            return Ok(ChangesetStatusResult {
                id: parsed.id,
                content_sha256: parsed.content_sha256,
                applied: false,
                operation_count: parsed.operations.len().saturating_sub(1),
            });
        }
        let has_sha_column: bool = connection
            .query_row(
                "SELECT 1 FROM pragma_table_info('changeset_applied') WHERE name='content_sha256';",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        let applied_sha256: Option<Option<String>> = if has_sha_column {
            connection
                .query_row(
                    "SELECT content_sha256 FROM changeset_applied WHERE id=?1;",
                    params![parsed.id],
                    |row| row.get(0),
                )
                .optional()?
        } else {
            connection
                .query_row(
                    "SELECT NULL FROM changeset_applied WHERE id=?1;",
                    params![parsed.id],
                    |row| row.get(0),
                )
                .optional()?
        };
        if let Some(applied_sha256) = applied_sha256 {
            if applied_sha256.as_deref() != Some(parsed.content_sha256.as_str()) {
                return Err(HarnessInfraError::ChangesetIdentityConflict {
                    id: parsed.id,
                    applied_sha256: applied_sha256
                        .unwrap_or_else(|| "unknown-legacy-content".to_owned()),
                    content_sha256: parsed.content_sha256,
                });
            }
            return Ok(ChangesetStatusResult {
                id: parsed.id,
                content_sha256: parsed.content_sha256,
                applied: true,
                operation_count: parsed.operations.len().saturating_sub(1),
            });
        }
        Ok(ChangesetStatusResult {
            id: parsed.id,
            content_sha256: parsed.content_sha256,
            applied: false,
            operation_count: parsed.operations.len().saturating_sub(1),
        })
    }

    fn snapshot_db(&self, output: &Path) -> Result<DbSnapshotResult> {
        if output.exists() {
            return Err(HarnessInfraError::SnapshotOutputExists(
                output.display().to_string(),
            ));
        }
        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        let file_name = output
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("harness.db");
        let temporary = parent.join(format!(
            ".{file_name}.tmp.{}.{}",
            std::process::id(),
            Self::unix_time_nanos()
        ));
        let attempt = (|| -> Result<DbSnapshotResult> {
            let mut source = self.open_existing()?;
            let transaction = source.transaction()?;
            let graph_revision = Self::work_graph_from(&transaction)?.revision;
            let source_logical_sha256 = logical_database_sha256(&transaction)?;
            transaction.backup(rusqlite::MAIN_DB, &temporary, None)?;
            transaction.commit()?;

            let snapshot = Connection::open(&temporary)?;
            // A published/stopped snapshot has no WAL to recover. Normalize its
            // persistent journal mode so later read-only verification cannot
            // create ignored -wal/-shm sidecars beside the tracked artifact.
            snapshot.pragma_update(None, "journal_mode", "DELETE")?;
            let integrity: String =
                snapshot.query_row("PRAGMA integrity_check;", [], |row| row.get(0))?;
            if integrity != "ok" {
                return Err(HarnessInfraError::InvalidChangeset(format!(
                    "snapshot integrity check failed: {integrity}"
                )));
            }
            let snapshot_logical_sha256 = logical_database_sha256(&snapshot)?;
            if snapshot_logical_sha256 != source_logical_sha256 {
                return Err(HarnessInfraError::InvalidChangeset(
                    "snapshot logical state differs from source read transaction".to_owned(),
                ));
            }
            drop(snapshot);
            // `sync_all` requires a writable handle on Windows even though the
            // snapshot bytes are only read here for hashing.
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&temporary)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            let snapshot_file_sha256 = sha256_bytes(&bytes);
            file.sync_all()?;
            // Windows does not permit renaming an open file without delete
            // sharing. Close the verified temporary snapshot before the
            // atomic move; Unix permits this but Windows release artifacts
            // exercise the stricter behavior.
            drop(file);
            fs::rename(&temporary, output)?;
            if let Ok(directory) = fs::File::open(parent) {
                let _ = directory.sync_all();
            }
            Ok(DbSnapshotResult {
                output: output.to_owned(),
                source_logical_sha256,
                graph_revision,
                snapshot_file_sha256,
            })
        })();
        if attempt.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        attempt
    }

    fn rebuild_db(&self, changeset_dir: &Path) -> Result<DbRebuildResult> {
        if self.db_path.exists() {
            return Err(HarnessInfraError::RebuildDatabaseExists(
                self.db_path.display().to_string(),
            ));
        }

        self.init()?;

        let mut changesets = Vec::new();
        if changeset_dir.exists() {
            for entry in fs::read_dir(changeset_dir)? {
                let entry = entry?;
                let path = entry.path();
                let is_changeset = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| value.ends_with(".changeset.jsonl"));
                if is_changeset {
                    changesets.push(path);
                }
            }
        }
        changesets.sort();

        let mut applied_count = 0usize;
        let mut operation_count = 0usize;
        for changeset in changesets {
            let result = self.apply_changeset(&changeset)?;
            if result.applied {
                applied_count += 1;
                operation_count += result.operations;
            }
        }

        Ok(DbRebuildResult {
            db_path: self.db_path.clone(),
            changesets: applied_count,
            operations: operation_count,
        })
    }

    fn query_sql(&self, sql: &str) -> Result<QueryTable> {
        let connection = self.open_existing_read_only()?;
        let mut statement = connection.prepare(sql).map_err(query_sql_error)?;
        let headers = statement
            .column_names()
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let column_count = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                let mut values = Vec::new();
                for index in 0..column_count {
                    values.push(sql_value_to_string(row.get_ref(index)?));
                }
                Ok(values)
            })
            .map_err(query_sql_error)?;

        Ok(QueryTable {
            headers,
            rows: rows
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(query_sql_error)?,
        })
    }
}

impl From<HarnessContext> for SqliteHarnessRepository {
    fn from(context: HarnessContext) -> Self {
        Self::new(context.repo_root, context.db_path, context.schema_dir)
    }
}

#[derive(Debug)]
struct MatrixColumns {
    story: Option<usize>,
    contract: Option<usize>,
    unit: Option<usize>,
    integration: Option<usize>,
    e2e: Option<usize>,
    platform: Option<usize>,
    status: Option<usize>,
    evidence: Option<usize>,
}

#[derive(Debug, Default)]
struct BacklogMarkdownItem {
    title: String,
    discovered_while: String,
    current_pain: String,
    suggested_improvement: String,
    risk: String,
    status: String,
}

impl MatrixColumns {
    fn from_header(fields: &[String]) -> Self {
        let mut columns = Self {
            story: None,
            contract: None,
            unit: None,
            integration: None,
            e2e: None,
            platform: None,
            status: None,
            evidence: None,
        };

        for (index, field) in fields.iter().enumerate() {
            match normalize_token(field).as_str() {
                "story" => columns.story = Some(index),
                "contract" => columns.contract = Some(index),
                "unit" => columns.unit = Some(index),
                "integration" => columns.integration = Some(index),
                "e2e" => columns.e2e = Some(index),
                "platform" => columns.platform = Some(index),
                "status" => columns.status = Some(index),
                "evidence" => columns.evidence = Some(index),
                _ => {}
            }
        }

        columns
    }
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>> {
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(HarnessInfraError::from)
}

fn trace_score_source_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TraceScoreSource> {
    Ok(TraceScoreSource {
        id: row.get(0)?,
        task_summary: row.get(1)?,
        intake_id: row.get(2)?,
        risk_lane: row.get(3)?,
        agent: row.get(4)?,
        actions_taken: row.get(5)?,
        files_read: row.get(6)?,
        files_changed: row.get(7)?,
        decisions_made: row.get(8)?,
        errors: row.get(9)?,
        outcome: row.get(10)?,
        duration_seconds: row.get(11)?,
        token_estimate: row.get(12)?,
        harness_friction: row.get(13)?,
        notes: row.get(14)?,
    })
}

fn markdown_table_fields(line: &str) -> Vec<String> {
    let trimmed = line.trim();
    let trimmed = trimmed.strip_prefix('|').unwrap_or(trimmed);
    let trimmed = trimmed.strip_suffix('|').unwrap_or(trimmed);
    trimmed
        .split('|')
        .map(|field| field.trim().to_owned())
        .collect()
}

fn field_at(fields: &[String], index: Option<usize>) -> Option<String> {
    index
        .and_then(|value| fields.get(value))
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn evidence_from_fields(fields: &[String], start_index: usize) -> Option<String> {
    fields
        .get(start_index..)
        .map(|values| values.join(" | "))
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn proof_from_cell(value: &str) -> i64 {
    match normalize_token(value).as_str() {
        ""
        | "no"
        | "none"
        | "n_a"
        | "na"
        | "planned"
        | "pending"
        | "blocked"
        | "not_attempted"
        | "not_operator_reviewed" => 0,
        token
            if token.starts_with("no_")
                || token.starts_with("pending")
                || token.starts_with("blocked")
                || token.contains("pending")
                || token.contains("blocked")
                || token.contains("not_attempted")
                || token.contains("not_operator_reviewed") =>
        {
            0
        }
        _ => 1,
    }
}

fn normalize_story_status(value: &str) -> String {
    match normalize_token(value).as_str() {
        "planned" => "planned",
        "in_progress" => "in_progress",
        "implemented" => "implemented",
        "changed" => "changed",
        "retired" => "retired",
        _ => "planned",
    }
    .to_owned()
}

fn normalize_decision_status(value: &str) -> String {
    let token = normalize_token(value);
    match token.as_str() {
        "proposed" => "proposed",
        "accepted" => "accepted",
        "superseded" => "superseded",
        "rejected" => "rejected",
        token if token.starts_with("superseded_") => "superseded",
        _ => "accepted",
    }
    .to_owned()
}

fn normalize_backlog_status(value: &str) -> String {
    match normalize_token(value).as_str() {
        "proposed" => "proposed",
        "accepted" => "accepted",
        "implemented" => "implemented",
        "rejected" => "rejected",
        _ => "proposed",
    }
    .to_owned()
}

fn markdown_section_first_value(content: &str, heading: &str) -> String {
    let target = format!("## {heading}");
    let mut found = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if found && !trimmed.is_empty() {
            return trimmed.to_owned();
        }
        if trimmed == target {
            found = true;
        }
    }
    String::new()
}

fn backlog_items(content: &str) -> Vec<BacklogMarkdownItem> {
    let mut in_items = false;
    let mut current_heading = String::new();
    let mut current = BacklogMarkdownItem::default();
    let mut items = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed == "## Items" {
            in_items = true;
            current_heading.clear();
            continue;
        }
        if !in_items {
            continue;
        }

        if let Some(heading) = trimmed.strip_prefix("### ") {
            let normalized = normalize_token(heading);
            if normalized == "title" && !current.title.is_empty() {
                items.push(current);
                current = BacklogMarkdownItem::default();
            }
            current_heading = normalized;
            continue;
        }

        if trimmed.is_empty() || current_heading.is_empty() {
            continue;
        }

        let target = match current_heading.as_str() {
            "title" => &mut current.title,
            "discovered_while" => &mut current.discovered_while,
            "current_pain" => &mut current.current_pain,
            "suggested_improvement" => &mut current.suggested_improvement,
            "risk" => &mut current.risk,
            "status" => &mut current.status,
            _ => continue,
        };
        if target.is_empty() {
            *target = trimmed.to_owned();
        }
    }

    if !current.title.is_empty() {
        items.push(current);
    }
    items
}

fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn command_available(repo_root: &Path, command: &str) -> bool {
    let first = command.split_whitespace().next().unwrap_or(command);
    if first.is_empty() {
        return false;
    }
    let candidate = Path::new(first);
    if candidate.is_absolute() {
        return candidate.exists();
    }
    if first.contains('/') || first.contains('\\') {
        return repo_root.join(first).exists();
    }
    env::var_os("PATH")
        .is_some_and(|path| env::split_paths(&path).any(|dir| dir.join(first).exists()))
}

/// Kind-aware presence probe. Returns `(status, detail)` where status is one of
/// `present` / `missing` / `unknown`. It never fails: an absent extension is a
/// fact to report, not an error to raise.
fn scan_tool_status(
    repo_root: &Path,
    kind: &str,
    command: &str,
    scan_target: Option<&str>,
) -> (&'static str, String) {
    match kind {
        "cli" | "binary" => {
            if command_available(repo_root, command) {
                ("present", command.to_owned())
            } else {
                ("missing", command.to_owned())
            }
        }
        "mcp" | "skill" => match scan_target.map(str::trim).filter(|t| !t.is_empty()) {
            Some(target) => {
                if scan_target_resolves(repo_root, target) {
                    ("present", target.to_owned())
                } else {
                    ("missing", target.to_owned())
                }
            }
            None => (
                "unknown",
                "no scan target; agent confirms availability".to_owned(),
            ),
        },
        "http" => match scan_target.map(str::trim).filter(|t| !t.is_empty()) {
            Some(target) => {
                if http_reachable(target) || scan_target_resolves(repo_root, target) {
                    ("present", target.to_owned())
                } else {
                    ("missing", target.to_owned())
                }
            }
            None => ("unknown", "no scan target".to_owned()),
        },
        _ => ("unknown", String::new()),
    }
}

/// Resolve a declarative scan target as a filesystem path: `~` expands to HOME,
/// absolute paths are tested directly, relative paths are tested against the
/// repo root.
fn scan_target_resolves(repo_root: &Path, target: &str) -> bool {
    let expanded = expand_home(target);
    let path = Path::new(&expanded);
    if path.is_absolute() {
        path.exists()
    } else {
        repo_root.join(&expanded).exists()
    }
}

fn expand_home(target: &str) -> String {
    if let Some(rest) = target.strip_prefix("~/") {
        if let Some(home) = env::var_os("HOME") {
            return format!("{}/{}", home.to_string_lossy(), rest);
        }
    }
    target.to_owned()
}

/// Best-effort TCP reachability for `http`/`https` scan targets. Any failure
/// (parse, DNS, timeout, refused) is reported as not reachable rather than an
/// error, so a down endpoint degrades the capability instead of breaking intake.
fn http_reachable(target: &str) -> bool {
    use std::net::{TcpStream, ToSocketAddrs};
    use std::time::Duration;

    let (default_port, rest) = if let Some(rest) = target.strip_prefix("https://") {
        (443u16, rest)
    } else if let Some(rest) = target.strip_prefix("http://") {
        (80u16, rest)
    } else {
        return false;
    };

    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return false;
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<u16>().unwrap_or(default_port)),
        None => (authority, default_port),
    };

    let Ok(addresses) = (host, port).to_socket_addrs() else {
        return false;
    };
    addresses
        .into_iter()
        .any(|address| TcpStream::connect_timeout(&address, Duration::from_secs(2)).is_ok())
}

fn tool_args_json(args: &[ToolArgSpec]) -> Option<String> {
    if args.is_empty() {
        return None;
    }
    Some(format!(
        "[{}]",
        args.iter()
            .map(|arg| {
                format!(
                    "{{\"name\":\"{}\",\"type\":\"{}\",\"required\":{},\"help\":\"{}\"}}",
                    escape_json(&arg.name),
                    escape_json(&arg.arg_type),
                    arg.required,
                    escape_json(arg.help.as_deref().unwrap_or(""))
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    ))
}

fn parse_observation_schedule(value: &str) -> Result<(String, Option<String>, Option<i64>)> {
    if value == "manual" {
        return Ok(("manual".to_owned(), None, None));
    }
    if let Some(due) = value.strip_prefix("due:") {
        let parsed = DateTime::parse_from_rfc3339(due).map_err(|_| {
            HarnessInfraError::ProposalDecision("outcome due time must be RFC3339".to_owned())
        })?;
        return Ok((
            "due_at".to_owned(),
            Some(parsed.with_timezone(&Utc).to_rfc3339()),
            None,
        ));
    }
    if let Some(count) = value.strip_prefix("traces:") {
        let count = count.parse::<i64>().map_err(|_| {
            HarnessInfraError::ProposalDecision(
                "outcome trace count must be a positive integer".to_owned(),
            )
        })?;
        if count <= 0 {
            return Err(HarnessInfraError::ProposalDecision(
                "outcome trace count must be a positive integer".to_owned(),
            ));
        }
        return Ok(("trace_count".to_owned(), None, Some(count)));
    }
    Err(HarnessInfraError::ProposalDecision(
        "invalid observation schedule".to_owned(),
    ))
}

fn stored_rejection_reason<'a>(
    structured: Option<&'a str>,
    legacy_notes: Option<&'a str>,
) -> Option<&'a str> {
    structured.or_else(|| {
        legacy_notes.and_then(|notes| {
            notes
                .lines()
                .find_map(|line| line.strip_prefix("rejection_reason: "))
        })
    })
}

fn legacy_reconcile_candidates(
    connection: &Connection,
    proposals: &[ImprovementProposal],
) -> Result<Vec<LegacyReconcileCandidate>> {
    let mut statement = connection.prepare(
        "SELECT id, title, status, actual_outcome,
                json_object(
                    'created_at', created_at,
                    'title', title,
                    'discovered_while', discovered_while,
                    'current_pain', current_pain,
                    'suggested_improvement', suggested_improvement,
                    'risk', risk,
                    'status', status,
                    'predicted_impact', predicted_impact,
                    'actual_outcome', actual_outcome,
                    'implemented_at', implemented_at,
                    'notes', notes,
                    'accepted_at', accepted_at,
                    'closed_at', closed_at,
                    'resolution_evidence', resolution_evidence,
                    'outcome_schedule_kind', outcome_schedule_kind,
                    'outcome_due_at', outcome_due_at,
                    'outcome_after_traces', outcome_after_traces,
                    'outcome_baseline_trace_count', outcome_baseline_trace_count
                )
         FROM backlog
         WHERE uid IS NULL AND proposal_key IS NULL
         ORDER BY id;",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(LegacyBacklogRow {
            id: row.get(0)?,
            title: row.get(1)?,
            status: row.get(2)?,
            actual_outcome: row.get(3)?,
            legacy_payload: serde_json::from_str(&row.get::<_, String>(4)?).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        })
    })?;
    let legacy_rows = collect_rows(rows)?;
    let mut candidates = Vec::new();

    for row in &legacy_rows {
        let matching = proposals
            .iter()
            .filter(|proposal| proposal.title == row.title)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            candidates.push(legacy_candidate(
                row,
                "manual",
                None,
                "no exact generated proposal title matches this historical row",
                "none",
                Vec::new(),
            ));
            continue;
        }
        if matching.len() != 1 {
            candidates.push(legacy_candidate(
                row,
                "ambiguous",
                None,
                "more than one generated proposal matches this historical row",
                "none",
                Vec::new(),
            ));
            continue;
        }
        let proposal = matching[0];
        let same_title = legacy_rows
            .iter()
            .filter(|candidate| candidate.title == row.title)
            .count();
        let keyed_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM backlog WHERE proposal_key=?1;",
            params![proposal.key],
            |query_row| query_row.get(0),
        )?;
        if same_title > 1 || keyed_count > 0 {
            candidates.push(legacy_candidate(
                row,
                "duplicate_candidate",
                Some(proposal.key.clone()),
                "the issue identity already has another plausible occurrence; human canonical selection is required",
                "none",
                Vec::new(),
            ));
            continue;
        }
        if proposal.evidence_items.is_empty() {
            candidates.push(legacy_candidate(
                row,
                "ambiguous",
                Some(proposal.key.clone()),
                "the generated proposal has no exact evidence item to preserve",
                "none",
                Vec::new(),
            ));
            continue;
        }

        let mut evidence = Vec::new();
        let mut ambiguity = None;
        for item in &proposal.evidence_items {
            if item.source_kind != "legacy_snapshot" {
                evidence.push((item.clone(), None));
                continue;
            }
            match resolve_legacy_evidence_capture(connection, item)? {
                Some(capture) => evidence.push((item.clone(), Some(capture))),
                None => {
                    ambiguity = Some(format!(
                        "evidence {} at {} does not resolve to exactly one UID-less trace or intervention",
                        item.fingerprint, item.observed_at
                    ));
                    break;
                }
            }
        }
        if let Some(reason) = ambiguity {
            candidates.push(legacy_candidate(
                row,
                "ambiguous",
                Some(proposal.key.clone()),
                &reason,
                "none",
                Vec::new(),
            ));
            continue;
        }

        let snapshot_count = evidence
            .iter()
            .filter(|(_, capture)| capture.is_some())
            .count();
        let observation = matches!(row.status.as_str(), "implemented" | "rejected")
            && row
                .actual_outcome
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty());
        let changes = format!(
            "uid, proposal_key, occurrence_kind, evidence_links={}{}",
            evidence.len(),
            if observation {
                ", legacy_recorded outcome"
            } else {
                ""
            }
        );
        candidates.push(LegacyReconcileCandidate {
            row: row.clone(),
            record: LegacyReconcileRecord {
                backlog_id: row.id,
                classification: "derivable".to_owned(),
                proposal_key: Some(proposal.key.clone()),
                reason: format!(
                    "one exact generated proposal and complete evidence resolve deterministically ({snapshot_count} embedded snapshot(s))"
                ),
                changes,
            },
            backlog_uid: Some(stable_uid("blg", &proposal.key)),
            evidence,
        });
    }
    Ok(candidates)
}

fn legacy_candidate(
    row: &LegacyBacklogRow,
    classification: &str,
    proposal_key: Option<String>,
    reason: &str,
    changes: &str,
    evidence: Vec<(ProposalEvidence, Option<LegacyEvidenceCapture>)>,
) -> LegacyReconcileCandidate {
    LegacyReconcileCandidate {
        row: row.clone(),
        record: LegacyReconcileRecord {
            backlog_id: row.id,
            classification: classification.to_owned(),
            proposal_key,
            reason: reason.to_owned(),
            changes: changes.to_owned(),
        },
        backlog_uid: None,
        evidence,
    }
}

fn resolve_legacy_evidence_capture(
    connection: &Connection,
    evidence: &ProposalEvidence,
) -> Result<Option<LegacyEvidenceCapture>> {
    let mut matches = Vec::new();
    {
        let mut statement = connection.prepare(
            "SELECT id, created_at, harness_friction FROM trace
             WHERE uid IS NULL AND harness_friction IS NOT NULL AND created_at=?1;",
        )?;
        let rows = statement.query_map(params![evidence.observed_at], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for (id, created_at, text) in collect_rows(rows)? {
            if sha256_hex(&text) == evidence.fingerprint {
                let canonical_payload = json!({
                    "created_at": created_at,
                    "harness_friction": text,
                })
                .to_string();
                let fingerprint = sha256_hex(&canonical_payload);
                matches.push(LegacyEvidenceCapture {
                    uid: stable_uid("leg", &format!("trace\0{fingerprint}")),
                    source_kind: "trace".to_owned(),
                    source_local_id: id,
                    fingerprint,
                    canonical_payload,
                });
            }
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT id, created_at, type, description FROM intervention
             WHERE uid IS NULL AND created_at=?1;",
        )?;
        let rows = statement.query_map(params![evidence.observed_at], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for (id, created_at, intervention_type, description) in collect_rows(rows)? {
            let text = format!("{intervention_type}: {description}");
            if sha256_hex(&text) == evidence.fingerprint {
                let canonical_payload = json!({
                    "created_at": created_at,
                    "description": description,
                    "type": intervention_type,
                })
                .to_string();
                let fingerprint = sha256_hex(&canonical_payload);
                matches.push(LegacyEvidenceCapture {
                    uid: stable_uid("leg", &format!("intervention\0{fingerprint}")),
                    source_kind: "intervention".to_owned(),
                    source_local_id: id,
                    fingerprint,
                    canonical_payload,
                });
            }
        }
    }
    Ok((matches.len() == 1).then(|| matches.remove(0)))
}

fn record_proposal_evidence(
    transaction: &Transaction<'_>,
    backlog_uid: &str,
    proposal: &ImprovementProposal,
) -> Result<()> {
    for evidence in &proposal.evidence_items {
        transaction.execute(
            "INSERT OR IGNORE INTO proposal_evidence_link (backlog_uid, source_kind, evidence_uid, evidence_fingerprint, observed_at)
             VALUES (?1, ?2, ?3, ?4, ?5);",
            params![
                backlog_uid,
                evidence.source_kind,
                evidence.uid,
                evidence.fingerprint,
                evidence.observed_at
            ],
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn proposal_decision_operation(
    uid: &str,
    key: &str,
    status: &str,
    proposal: &ImprovementProposal,
    schedule: Option<(&String, Option<&str>, Option<i64>)>,
    reason: Option<&String>,
    accepted_at: Option<&str>,
    closed_at: Option<&str>,
    notes: Option<&str>,
    created_at: Option<&str>,
    expected_revision: Option<i64>,
) -> Value {
    let occurrence_kind = match proposal.lifecycle_state.as_str() {
        "regression" => "regression",
        "reconsideration" => "reconsideration",
        _ => "original",
    };
    json!({
        "op": "backlog.proposal.decision",
        "version": if expected_revision.is_some() { 3 } else { 2 },
        "uid": uid,
        "expected_revision": expected_revision,
        "payload": {
            "proposal_key": key, "status": status, "occurrence_kind": occurrence_kind,
            "predecessor_uid": proposal.predecessor_uid,
            "title": proposal.title, "discovered_while": "harness-cli propose",
            "current_pain": proposal.evidence, "suggested_improvement": proposal.suggested_action,
            "risk": normalize_token(&proposal.risk), "predicted_impact": proposal.predicted_impact,
            "notes": notes, "accepted_at": accepted_at, "closed_at": closed_at,
            "created_at": created_at,
            "outcome_schedule_kind": schedule.as_ref().map(|item| item.0),
            "outcome_due_at": schedule.as_ref().and_then(|item| item.1),
            "outcome_after_traces": schedule.and_then(|item| item.2),
            "rejection_reason": reason,
            "evidence": proposal.evidence_items.iter().map(|item| json!({
                "source_kind": item.source_kind,
                "evidence_uid": item.uid,
                "evidence_fingerprint": item.fingerprint,
                "observed_at": item.observed_at,
            })).collect::<Vec<_>>(),
        }
    })
}

fn parse_stored_tool_args(value: Option<&str>) -> Vec<ToolArgSpec> {
    let Some(value) = value else {
        return Vec::new();
    };
    if !value.contains("\"name\"") {
        return Vec::new();
    }
    value
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split("},{")
        .filter_map(|raw| {
            let item = raw.trim_matches('{').trim_matches('}');
            let name = json_object_value(item, "name")?;
            let arg_type = json_object_value(item, "type").unwrap_or_else(|| "string".to_owned());
            let required = json_object_value(item, "required")
                .map(|value| value == "true")
                .unwrap_or(false);
            let help = json_object_value(item, "help").filter(|value| !value.is_empty());
            Some(ToolArgSpec {
                name,
                arg_type,
                required,
                help,
            })
        })
        .collect()
}

fn json_object_value(raw: &str, key: &str) -> Option<String> {
    let target = format!("\"{key}\":");
    let start = raw.find(&target)? + target.len();
    let rest = &raw[start..];
    if let Some(rest) = rest.strip_prefix('"') {
        let end = rest.find('"')?;
        Some(rest[..end].to_owned())
    } else {
        Some(rest.split(',').next().unwrap_or_default().trim().to_owned())
    }
}

fn escape_json(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn audit_findings(connection: &Connection, sql: &str) -> Result<Vec<AuditFinding>> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([], |row| {
        Ok(AuditFinding {
            id: row.get(0)?,
            title: row.get(1)?,
        })
    })?;
    collect_rows(rows)
}

fn health_category_rank(category: &str) -> u8 {
    match category {
        "audit" => 0,
        "proposal_decision" => 1,
        "accepted_work" => 2,
        "outcome_review" => 3,
        "recurrence" => 4,
        _ => 5,
    }
}

#[derive(Debug)]
struct ProposalEvidenceGroup {
    text: String,
    count: usize,
    evidence: Vec<ProposalEvidence>,
}

fn repeated_friction(connection: &Connection) -> Result<Vec<ProposalEvidenceGroup>> {
    let mut statement = connection.prepare(
        "SELECT uid, harness_friction, created_at FROM trace
         WHERE harness_friction IS NOT NULL
           AND TRIM(harness_friction) <> ''
           AND LOWER(TRIM(harness_friction)) <> 'none';",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let values = collect_rows(rows)?;
    Ok(repeated_values("trace", values))
}

fn repeated_interventions(connection: &Connection) -> Result<Vec<ProposalEvidenceGroup>> {
    let mut statement = connection.prepare(
        "SELECT uid, type || ': ' || description, created_at FROM intervention
         WHERE TRIM(description) <> '';",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let values = collect_rows(rows)?;
    Ok(repeated_values("intervention", values))
}

fn repeated_values(
    source_kind: &str,
    values: Vec<(Option<String>, String, String)>,
) -> Vec<ProposalEvidenceGroup> {
    let mut grouped: Vec<(String, String, Vec<ProposalEvidence>)> = Vec::new();
    for (uid, value, observed_at) in values {
        let key = normalize_token(&value);
        let evidence = ProposalEvidence {
            source_kind: uid
                .as_ref()
                .map(|_| source_kind.to_owned())
                .unwrap_or_else(|| "legacy_snapshot".to_owned()),
            uid: uid.unwrap_or_else(|| format!("legacy-{}", sha256_hex(&value))),
            fingerprint: sha256_hex(&value),
            observed_at,
        };
        if let Some(existing) = grouped.iter_mut().find(|item| item.0 == key) {
            existing.2.push(evidence);
        } else {
            grouped.push((key, value, vec![evidence]));
        }
    }
    grouped
        .into_iter()
        .filter(|(_, _, evidence)| evidence.len() >= 2)
        .map(|(_, text, evidence)| ProposalEvidenceGroup {
            count: evidence.len(),
            text,
            evidence,
        })
        .collect()
}

fn audit_proposal_evidence(
    connection: &Connection,
    rule: &str,
    findings: &[AuditFinding],
) -> Result<Vec<ProposalEvidence>> {
    findings
        .iter()
        .map(|finding| {
            let finding_key = format!("audit.{rule}:v1:entity:{}", finding.id);
            let active: Option<(String, String, String)> = connection
                .query_row(
                    "SELECT uid, evidence_fingerprint, opened_at FROM audit_evidence_episode
                     WHERE finding_key=?1 AND cleared_at IS NULL;",
                    params![finding_key],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            Ok(match active {
                Some((uid, fingerprint, observed_at)) => ProposalEvidence {
                    source_kind: "audit".to_owned(),
                    uid,
                    fingerprint,
                    observed_at,
                },
                None => ProposalEvidence {
                    source_kind: "legacy_snapshot".to_owned(),
                    uid: finding_key.clone(),
                    fingerprint: sha256_hex(&format!("{finding_key}\0{}", finding.title)),
                    observed_at: "1970-01-01 00:00:00".to_owned(),
                },
            })
        })
        .collect()
}

#[allow(clippy::type_complexity)]
fn classify_proposal(connection: &Connection, proposal: &mut ImprovementProposal) -> Result<()> {
    let latest: Option<(
        i64,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = connection
        .query_row(
            "SELECT backlog.id, backlog.uid, backlog.status, backlog.occurrence_kind,
                    backlog.resolution_evidence, backlog.closed_at,
                    (SELECT story_id FROM story_backlog_link
                     WHERE backlog_uid=backlog.uid AND relationship='resolves')
             FROM backlog WHERE proposal_key=?1 ORDER BY id DESC LIMIT 1;",
            params![proposal.key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;

    let Some((id, uid, status, kind, closure_proof, closed_at, resolver)) = latest else {
        let legacy: i64 = connection.query_row(
            "SELECT COUNT(*) FROM backlog WHERE proposal_key IS NULL AND title=?1;",
            params![proposal.title],
            |row| row.get(0),
        )?;
        if legacy > 0 {
            proposal.lifecycle_state = "legacy-unclassified".to_owned();
            proposal.lifecycle_explanation = Some(
                "plausible unkeyed legacy match; use US-080 reconciliation instead of guessing identity"
                    .to_owned(),
            );
        } else {
            proposal.lifecycle_state = "new".to_owned();
        }
        return Ok(());
    };

    proposal.committed_backlog_id = Some(id);
    match status.as_str() {
        "proposed" => {
            proposal.lifecycle_state = "pending".to_owned();
            proposal.lifecycle_explanation = Some(format!("existing proposed backlog #{id}"));
        }
        "accepted" => {
            proposal.lifecycle_state = "accepted".to_owned();
            proposal.lifecycle_explanation = Some(format!("active accepted backlog #{id}"));
        }
        "implemented" | "rejected" => {
            let mut statement = connection.prepare(
                "SELECT link.source_kind, link.evidence_uid
                 FROM proposal_evidence_link link
                 JOIN backlog ON backlog.uid=link.backlog_uid
                 WHERE backlog.proposal_key=?1;",
            )?;
            let rows = statement.query_map(params![proposal.key], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let covered = collect_rows(rows)?;
            proposal.evidence_items.retain(|item| {
                let explicitly_covered = covered
                    .iter()
                    .any(|covered| covered.0 == item.source_kind && covered.1 == item.uid);
                // Durable timestamps currently have one-second precision, so
                // equality is the conservative representation of "at/after".
                let observed_after_closure = closed_at
                    .as_ref()
                    .is_some_and(|closed| item.observed_at >= *closed);
                !explicitly_covered && observed_after_closure
            });
            proposal.predecessor_uid = Some(uid.clone());
            if proposal.evidence_items.is_empty() {
                proposal.lifecycle_state = "suppressed".to_owned();
                proposal.lifecycle_explanation = Some(format!(
                    "backlog #{id} ({}) closed by resolver {} with proof {}; no uncovered evidence",
                    kind.as_deref().unwrap_or("original"),
                    resolver.as_deref().unwrap_or("none"),
                    closure_proof.as_deref().unwrap_or("not recorded")
                ));
            } else {
                proposal.lifecycle_state = if status == "implemented" {
                    "regression"
                } else {
                    "reconsideration"
                }
                .to_owned();
                proposal.lifecycle_explanation = Some(format!(
                    "{} uncovered evidence item(s) after terminal backlog #{id}",
                    proposal.evidence_items.len()
                ));
            }
        }
        _ => {
            proposal.lifecycle_state = status;
        }
    }
    Ok(())
}

fn confidence_for_count(count: usize) -> String {
    if count >= 3 {
        "high".to_owned()
    } else {
        "medium".to_owned()
    }
}

fn short_title(value: &str) -> String {
    let words = value
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");
    if words.chars().count() > 72 {
        format!("{}...", words.chars().take(69).collect::<String>())
    } else {
        words
    }
}

fn verifier_shell() -> (&'static str, &'static str) {
    if cfg!(windows) {
        ("cmd", "/C")
    } else {
        ("sh", "-c")
    }
}

fn is_decision_file_name(file_name: &str) -> bool {
    let Some((prefix, _)) = file_name.split_once('-') else {
        return false;
    };
    prefix.len() == 4 && prefix.chars().all(|character| character.is_ascii_digit())
}

fn authorize_query_sql(context: AuthContext<'_>) -> Authorization {
    match context.action {
        AuthAction::Select
        | AuthAction::Read { .. }
        | AuthAction::Function { .. }
        | AuthAction::Recursive => Authorization::Allow,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } if query_sql_pragma_is_read_only(pragma_name, pragma_value) => Authorization::Allow,
        _ => Authorization::Deny,
    }
}

fn query_sql_pragma_is_read_only(name: &str, value: Option<&str>) -> bool {
    let name = name.to_ascii_lowercase();
    if matches!(
        name.as_str(),
        "foreign_key_check"
            | "foreign_key_list"
            | "index_info"
            | "index_list"
            | "index_xinfo"
            | "integrity_check"
            | "quick_check"
            | "table_info"
            | "table_list"
            | "table_xinfo"
    ) {
        return true;
    }

    value.is_none()
        && matches!(
            name.as_str(),
            "application_id"
                | "auto_vacuum"
                | "automatic_index"
                | "cache_size"
                | "cache_spill"
                | "cell_size_check"
                | "checkpoint_fullfsync"
                | "collation_list"
                | "compile_options"
                | "data_version"
                | "database_list"
                | "default_cache_size"
                | "defer_foreign_keys"
                | "encoding"
                | "foreign_keys"
                | "freelist_count"
                | "full_column_names"
                | "fullfsync"
                | "function_list"
                | "hard_heap_limit"
                | "ignore_check_constraints"
                | "journal_mode"
                | "journal_size_limit"
                | "legacy_alter_table"
                | "locking_mode"
                | "max_page_count"
                | "mmap_size"
                | "module_list"
                | "page_count"
                | "page_size"
                | "pragma_list"
                | "query_only"
                | "read_uncommitted"
                | "recursive_triggers"
                | "reverse_unordered_selects"
                | "schema_version"
                | "secure_delete"
                | "short_column_names"
                | "soft_heap_limit"
                | "synchronous"
                | "temp_store"
                | "threads"
                | "trusted_schema"
                | "user_version"
                | "wal_autocheckpoint"
                | "writable_schema"
        )
}

fn query_sql_error(error: rusqlite::Error) -> HarnessInfraError {
    if matches!(
        &error,
        rusqlite::Error::SqliteFailure(sqlite_error, _)
            if matches!(
                sqlite_error.code,
                rusqlite::ErrorCode::AuthorizationForStatementDenied
                    | rusqlite::ErrorCode::ReadOnly
            )
    ) {
        HarnessInfraError::QuerySqlWriteDenied
    } else {
        HarnessInfraError::Sqlite(error)
    }
}

fn sql_value_to_string(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => String::new(),
        ValueRef::Integer(value) => value.to_string(),
        ValueRef::Real(value) => value.to_string(),
        ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned(),
        ValueRef::Blob(value) => format!("<{} bytes>", value.len()),
    }
}

/// Canonical full logical database digest. Table and row ordering is detached
/// from SQLite's physical page layout so WAL checkpoints and VACUUM do not
/// change this identity.
fn logical_database_sha256(connection: &Connection) -> Result<String> {
    let mut table_statement = connection.prepare(
        "SELECT name, sql FROM sqlite_master
         WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;",
    )?;
    let tables = table_statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let tables = collect_rows(tables)?;
    let mut canonical_tables = Vec::new();
    for (table, schema) in tables {
        let escaped = table.replace('"', "\"\"");
        let mut statement = connection.prepare(&format!("SELECT * FROM \"{escaped}\";"))?;
        let columns = statement
            .column_names()
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        let column_count = statement.column_count();
        let rows = statement.query_map([], |row| {
            let mut values = Vec::with_capacity(column_count);
            for index in 0..column_count {
                let value = match row.get_ref(index)? {
                    ValueRef::Null => json!(["null"]),
                    ValueRef::Integer(value) => json!(["integer", value]),
                    ValueRef::Real(value) => json!(["real_bits", value.to_bits().to_string()]),
                    ValueRef::Text(value) => {
                        json!(["text", String::from_utf8_lossy(value).into_owned()])
                    }
                    ValueRef::Blob(value) => json!([
                        "blob_hex",
                        value
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    ]),
                };
                values.push(value);
            }
            serde_json::to_string(&values).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        })?;
        let mut rows = collect_rows(rows)?;
        rows.sort();
        canonical_tables.push(json!({
            "name": table,
            "schema": schema,
            "columns": columns,
            "rows": rows,
        }));
    }
    Ok(sha256_bytes(&serde_json::to_vec(&canonical_tables)?))
}

fn rollback_changeset_append(append: &mut ChangesetAppend) -> Result<()> {
    // Prefer reusing the descriptor from the original append: it still holds
    // the exclusive lock, so `set_len` cannot race with another process
    // concurrently writing to the same changeset file. `as_mut` is required
    // because we seek the descriptor and seek needs a mutable reference; we
    // shadow the binding to keep the rollback block readable.
    if let Some(file) = append.file.as_mut() {
        file.set_len(append.original_len)?;
        file.seek(SeekFrom::Start(append.original_len))?;
        file.sync_all()?;
    } else {
        // Defensive branch — see the struct invariant on `file`. Re-acquire the
        // exclusive lock and hold it for the whole rollback window so a
        // concurrent writer cannot clobber the truncation mid-operation, then
        // flush before letting the descriptor drop.
        let mut file = OpenOptions::new().write(true).open(&append.path)?;
        file.lock_exclusive()?;
        file.set_len(append.original_len)?;
        file.seek(SeekFrom::Start(append.original_len))?;
        file.sync_all()?;
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ChangesetApplyContext {
    intake_ids: std::collections::HashMap<i64, i64>,
    backlog_ids: std::collections::HashMap<i64, i64>,
    trace_ids: std::collections::HashMap<i64, i64>,
}

fn mapped_id(source_id: Option<i64>, ids: &std::collections::HashMap<i64, i64>) -> Option<i64> {
    source_id.map(|id| ids.get(&id).copied().unwrap_or(id))
}

fn apply_changeset_operation(
    transaction: &Transaction<'_>,
    operation: &Value,
    context: &mut ChangesetApplyContext,
    changeset_id: &str,
) -> Result<()> {
    let op = required_string(operation, "op")?;
    let version = operation
        .get("version")
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let payload = operation.get("payload").unwrap_or(&Value::Null);
    if !(1..=3).contains(&version) {
        return Err(HarnessInfraError::InvalidChangeset(format!(
            "unsupported version {version} for operation {op}"
        )));
    }
    match op.as_str() {
        "intake.add" if version == 2 => {
            let uid = required_uid(operation, "uid", "ink")?;
            transaction.execute(
                "INSERT INTO intake (uid, created_at, input_type, summary, risk_lane, risk_flags, affected_docs, story_id, notes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(uid) DO NOTHING;",
                params![uid, required_timestamp(payload, "created_at")?, required_string(payload, "input_type")?, required_string(payload, "summary")?, required_string(payload, "risk_lane")?, optional_string(payload, "risk_flags"), optional_string(payload, "affected_docs"), optional_string(payload, "story_id"), optional_string(payload, "notes")],
            )?;
            1
        }
        "intake.add" => {
            let source_id = required_i64(operation, "id")?;
            transaction.execute(
            "INSERT INTO intake (
                input_type, summary, risk_lane, risk_flags, affected_docs, story_id, notes
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
            params![
                required_string(payload, "input_type")?,
                required_string(payload, "summary")?,
                required_string(payload, "risk_lane")?,
                optional_string(payload, "risk_flags"),
                optional_string(payload, "affected_docs"),
                optional_string(payload, "story_id"),
                optional_string(payload, "notes"),
            ],
            )?;
            context
                .intake_ids
                .insert(source_id, transaction.last_insert_rowid());
            1
        }
        "story.add" if version >= 2 => transaction.execute(
            "INSERT INTO story (id, created_at, title, risk_lane, contract_doc, verify_command, notes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
            params![
                required_string(operation, "id")?,
                required_timestamp(payload, "created_at")?,
                required_string(payload, "title")?,
                required_string(payload, "risk_lane")?,
                optional_string(payload, "contract_doc"),
                optional_string(payload, "verify_command"),
                optional_string(payload, "notes"),
            ],
        )?,
        "story.add" => transaction.execute(
            "INSERT INTO story (id, title, risk_lane, contract_doc, verify_command, notes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
            params![
                required_string(operation, "id")?,
                required_string(payload, "title")?,
                required_string(payload, "risk_lane")?,
                optional_string(payload, "contract_doc"),
                optional_string(payload, "verify_command"),
                optional_string(payload, "notes"),
            ],
        )?,
        "story.update" => {
            let id = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("story", "story", "id", &id),
            )?;
            transaction.execute(
                "UPDATE story SET
                contract_doc=COALESCE(?1, contract_doc),
                status=COALESCE(?2, status),
                evidence=COALESCE(?3, evidence),
                unit_proof=COALESCE(?4, unit_proof),
                integration_proof=COALESCE(?5, integration_proof),
                e2e_proof=COALESCE(?6, e2e_proof),
                platform_proof=COALESCE(?7, platform_proof),
                verify_command=COALESCE(?8, verify_command)
             WHERE id=?9;",
            params![
                optional_string(payload, "contract_doc"),
                optional_string(payload, "status"),
                optional_string(payload, "evidence"),
                optional_i64(payload, "unit_proof"),
                optional_i64(payload, "integration_proof"),
                optional_i64(payload, "e2e_proof"),
                optional_i64(payload, "platform_proof"),
                optional_string(payload, "verify_command"),
                id,
            ],
            )?
        }
        "story.dependency.add" => {
            let blocker = required_string(operation, "id")?;
            let blocked = required_string(payload, "blocked")?;
            if blocker == blocked {
                return Err(HarnessInfraError::StoryDependencySelf(blocker));
            }
            ensure_story_exists(transaction, &blocker)?;
            ensure_story_exists(transaction, &blocked)?;
            if dependency_path_exists(transaction, &blocked, &blocker)? {
                return Err(HarnessInfraError::StoryDependencyCycle(blocker, blocked));
            }
            transaction.execute(
                "INSERT INTO story_dependency (story_id, blocks_story_id, created_at) VALUES (?1, ?2, COALESCE(?3, datetime('now')))
                 ON CONFLICT(story_id, blocks_story_id) DO NOTHING;",
                params![
                    blocker,
                    blocked,
                    if version >= 2 {
                        Some(required_timestamp(payload, "created_at")?)
                    } else {
                        None
                    }
                ],
            )?
        }
        "story.dependency.remove" => transaction.execute(
            "DELETE FROM story_dependency WHERE story_id=?1 AND blocks_story_id=?2;",
            params![
                required_string(operation, "id")?,
                required_string(payload, "blocked")?,
            ],
        )?,
        "story.hierarchy.add" => {
            let parent = required_string(operation, "id")?;
            let child = required_string(payload, "child")?;
            if parent == child {
                return Err(HarnessInfraError::StoryHierarchySelf(parent));
            }
            ensure_story_exists(transaction, &parent)?;
            ensure_story_exists(transaction, &child)?;
            if hierarchy_path_exists(transaction, &child, &parent)? {
                return Err(HarnessInfraError::StoryHierarchyCycle(parent, child));
            }
            transaction.execute(
                "INSERT INTO story_hierarchy (parent_story_id, child_story_id, created_at) VALUES (?1, ?2, COALESCE(?3, datetime('now')))
                 ON CONFLICT(parent_story_id, child_story_id) DO NOTHING;",
                params![
                    parent,
                    child,
                    if version >= 2 {
                        Some(required_timestamp(payload, "created_at")?)
                    } else {
                        None
                    }
                ],
            )?
        }
        "story.hierarchy.remove" => transaction.execute(
            "DELETE FROM story_hierarchy WHERE parent_story_id=?1 AND child_story_id=?2;",
            params![
                required_string(operation, "id")?,
                required_string(payload, "child")?,
            ],
        )?,
        "story.backlog.link" => {
            let story_id = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("story", "story", "id", &story_id),
            )?;
            let backlog_uid = required_uid(payload, "backlog_uid", "blg")?;
            let relationship = required_string(payload, "relationship")?;
            let (backlog_id, backlog_status): (i64, String) = transaction.query_row("SELECT id, status FROM backlog WHERE uid=?1;", params![backlog_uid], |row| Ok((row.get(0)?, row.get(1)?))).optional()?.ok_or_else(|| HarnessInfraError::InvalidChangeset(format!("story backlog link references missing backlog uid '{backlog_uid}'")))?;
            let story_status: String = transaction.query_row("SELECT status FROM story WHERE id=?1;", params![story_id], |row| row.get(0)).optional()?.ok_or_else(|| HarnessInfraError::StoryNotFound(story_id.clone()))?;
            let previous: Option<String> = transaction.query_row("SELECT relationship FROM story_backlog_link WHERE story_id=?1 AND backlog_uid=?2;", params![story_id, backlog_uid], |row| row.get(0)).optional()?;
            if previous.as_deref() != Some(&relationship) {
                if relationship == "resolves" || previous.as_deref() == Some("resolves") { validate_resolver_mutation(transaction, &story_id, backlog_id, &backlog_status, &story_status, &backlog_uid)?; }
                if !matches!(relationship.as_str(), "resolves" | "references") { return Err(HarnessInfraError::StoryBacklogRelationship); }
                let linked_at = if version >= 2 {
                    Some(required_timestamp(payload, "linked_at")?)
                } else {
                    optional_string(payload, "linked_at")
                };
                transaction.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at, linked_at_unix_ns) VALUES (?1, ?2, ?3, COALESCE(?4, datetime('now')), ?5) ON CONFLICT(story_id, backlog_uid) DO UPDATE SET relationship=excluded.relationship, linked_at=excluded.linked_at, linked_at_unix_ns=excluded.linked_at_unix_ns;", params![story_id, backlog_uid, relationship, linked_at, optional_i64(payload, "linked_at_unix_ns")])?;
                if relationship == "resolves" || previous.as_deref() == Some("resolves") { transaction.execute("UPDATE story SET last_verified_at=NULL, last_verified_result=NULL WHERE id=?1;", params![story_id])?; }
            }
            1
        }
        "story.backlog.unlink" => {
            let story_id = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("story", "story", "id", &story_id),
            )?;
            let backlog_uid = required_uid(payload, "backlog_uid", "blg")?;
            let linked: Option<(i64, String, String)> = transaction.query_row("SELECT backlog.id, backlog.status, link.relationship FROM story_backlog_link AS link JOIN backlog ON backlog.uid=link.backlog_uid WHERE link.story_id=?1 AND link.backlog_uid=?2;", params![story_id, backlog_uid], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).optional()?;
            if let Some((backlog_id, backlog_status, relationship)) = linked {
                if relationship == "resolves" { let story_status: String = transaction.query_row("SELECT status FROM story WHERE id=?1;", params![story_id], |row| row.get(0))?; validate_resolver_mutation(transaction, &story_id, backlog_id, &backlog_status, &story_status, &backlog_uid)?; transaction.execute("UPDATE story SET last_verified_at=NULL, last_verified_result=NULL WHERE id=?1;", params![story_id])?; }
                transaction.execute("DELETE FROM story_backlog_link WHERE story_id=?1 AND backlog_uid=?2;", params![story_id, backlog_uid])?;
            }
            1
        }
        "story.verify" => {
            let id = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("story", "story", "id", &id),
            )?;
            let verified_at = match (version, optional_string(payload, "verified_at")) {
                (2.., Some(value)) => canonical_sqlite_timestamp(value, "verified_at")?,
                (2.., None) => {
                    return Err(HarnessInfraError::InvalidChangeset(
                        "story.verify version 2 requires verified_at".to_owned(),
                    ));
                }
                (_, Some(value)) => value,
                (_, None) => transaction.query_row("SELECT datetime('now')", [], |row| row.get(0))?,
            };
            transaction.execute(
                "UPDATE story SET last_verified_at=?1, last_verified_result=?2 WHERE id=?3;",
                params![
                    verified_at,
                    required_string(payload, "result")?,
                    id,
                ],
            )?
        }
        "story.complete" => {
            let id = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("story", "story", "id", &id),
            )?;
            let completed_at = match (version, optional_string(payload, "completed_at")) {
                (2.., Some(value)) => canonical_sqlite_timestamp(value, "completed_at")?,
                (2.., None) => return Err(HarnessInfraError::InvalidChangeset(
                    "story.complete version 2 requires completed_at".to_owned(),
                )),
                (_, Some(value)) => value,
                (_, None) => transaction.query_row("SELECT datetime('now')", [], |row| row.get(0))?,
            };
            transaction.execute(
                "UPDATE story SET status='implemented', last_verified_at=?1, last_verified_result=?2 WHERE id=?3;",
                params![completed_at, required_string(payload, "result")?, id],
            )?
        }
        "backlog.complete" => {
            let uid = required_uid(operation, "uid", "blg")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("backlog", "backlog", "uid", &uid),
            )?;
            let story_id = required_string(payload, "story_id")?;
            let baseline = optional_i64(payload, "trace_baseline");
            let evidence = optional_string(payload, "resolution_evidence")
                .unwrap_or_else(|| json!({"story_id": story_id, "result":"pass"}).to_string());
            let completed_at = match (version, optional_string(payload, "completed_at")) {
                (2.., Some(value)) => canonical_sqlite_timestamp(value, "completed_at")?,
                (2.., None) => return Err(HarnessInfraError::InvalidChangeset(
                    "backlog.complete version 2 requires completed_at".to_owned(),
                )),
                (_, Some(value)) => value,
                (_, None) => transaction.query_row("SELECT datetime('now')", [], |row| row.get(0))?,
            };
            transaction.execute(
                "UPDATE backlog SET status='implemented', implemented_at=?1, closed_at=?1, resolution_evidence=COALESCE(resolution_evidence, ?2), outcome_baseline_trace_count=COALESCE(?3, outcome_baseline_trace_count) WHERE uid=?4;",
                params![completed_at, evidence, baseline, uid],
            )?
        }
        "legacy.evidence.capture" => {
            let uid = required_uid(operation, "uid", "leg")?;
            let source_kind = required_string(payload, "source_kind")?;
            if !matches!(source_kind.as_str(), "trace" | "intervention") {
                return Err(HarnessInfraError::InvalidChangeset(
                    "legacy evidence source_kind must be trace or intervention".to_owned(),
                ));
            }
            transaction.execute(
                "INSERT INTO legacy_evidence_snapshot
                    (uid, source_kind, source_local_id, evidence_fingerprint,
                     canonical_payload, captured_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(source_kind, evidence_fingerprint) DO NOTHING;",
                params![
                    uid,
                    source_kind,
                    optional_i64(payload, "source_local_id"),
                    required_string(payload, "evidence_fingerprint")?,
                    required_string(payload, "canonical_payload")?,
                    required_timestamp(payload, "captured_at")?,
                ],
            )?
        }
        "backlog.legacy.reconcile" => {
            let uid = required_uid(operation, "uid", "blg")?;
            let title = required_string(payload, "title")?;
            let existing: Option<i64> = transaction
                .query_row("SELECT id FROM backlog WHERE uid=?1;", params![uid], |row| {
                    row.get(0)
                })
                .optional()?;
            if existing.is_none() {
                let ids = {
                    let mut statement = transaction.prepare(
                        "SELECT id FROM backlog
                         WHERE title=?1 AND uid IS NULL AND proposal_key IS NULL
                         ORDER BY id;",
                    )?;
                    let rows = statement.query_map(params![title], |row| row.get::<_, i64>(0))?;
                    collect_rows(rows)?
                };
                if ids.len() > 1 {
                    return Err(HarnessInfraError::InvalidChangeset(format!(
                        "legacy reconciliation expected at most one unkeyed row titled '{title}', found {}",
                        ids.len()
                    )));
                }
                if let Some(id) = ids.first() {
                    transaction.execute(
                        "UPDATE backlog SET uid=?1, proposal_key=?2, occurrence_kind=?3
                         WHERE id=?4 AND uid IS NULL AND proposal_key IS NULL;",
                        params![
                            uid,
                            required_string(payload, "proposal_key")?,
                            required_string(payload, "occurrence_kind")?,
                            id
                        ],
                    )?;
                } else {
                    let legacy = payload.get("legacy_row").ok_or_else(|| {
                        HarnessInfraError::InvalidChangeset(
                            "legacy reconciliation cannot reconstruct a missing row without legacy_row"
                                .to_owned(),
                        )
                    })?;
                    transaction.execute(
                        "INSERT INTO backlog
                            (uid, proposal_key, occurrence_kind, created_at, title,
                             discovered_while, current_pain, suggested_improvement, risk, status,
                             predicted_impact, actual_outcome, implemented_at, notes, accepted_at,
                             closed_at, resolution_evidence, outcome_schedule_kind, outcome_due_at,
                             outcome_after_traces, outcome_baseline_trace_count)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                                 ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21);",
                        params![
                            uid,
                            required_string(payload, "proposal_key")?,
                            required_string(payload, "occurrence_kind")?,
                            required_string(legacy, "created_at")?,
                            required_string(legacy, "title")?,
                            optional_string(legacy, "discovered_while"),
                            optional_string(legacy, "current_pain"),
                            optional_string(legacy, "suggested_improvement"),
                            optional_string(legacy, "risk"),
                            required_string(legacy, "status")?,
                            optional_string(legacy, "predicted_impact"),
                            optional_string(legacy, "actual_outcome"),
                            optional_string(legacy, "implemented_at"),
                            optional_string(legacy, "notes"),
                            optional_string(legacy, "accepted_at"),
                            optional_string(legacy, "closed_at"),
                            optional_string(legacy, "resolution_evidence"),
                            optional_string(legacy, "outcome_schedule_kind"),
                            optional_string(legacy, "outcome_due_at"),
                            optional_i64(legacy, "outcome_after_traces"),
                            optional_i64(legacy, "outcome_baseline_trace_count"),
                        ],
                    )?;
                }
            }
            if let Some(evidence) = payload.get("evidence").and_then(Value::as_array) {
                for item in evidence {
                    let observed_at = if version >= 2 {
                        Some(required_timestamp(item, "observed_at")?)
                    } else {
                        optional_string(item, "observed_at")
                    };
                    transaction.execute(
                        "INSERT OR IGNORE INTO proposal_evidence_link
                            (backlog_uid, source_kind, evidence_uid, evidence_fingerprint, observed_at)
                         VALUES (?1, ?2, ?3, ?4, ?5);",
                        params![
                            uid,
                            required_string(item, "source_kind")?,
                            required_string(item, "evidence_uid")?,
                            required_string(item, "evidence_fingerprint")?,
                            observed_at,
                        ],
                    )?;
                }
            }
            1
        }
        "backlog.outcome.observe" => {
            let uid = required_uid(operation, "uid", "obs")?;
            let backlog_uid = required_uid(payload, "backlog_uid", "blg")?;
            let status = required_string(payload, "status")?;
            if !matches!(
                status.as_str(),
                "confirmed" | "ineffective" | "reverted" | "legacy_recorded"
            ) {
                return Err(HarnessInfraError::InvalidChangeset(
                    "invalid backlog outcome observation status".to_owned(),
                ));
            }
            transaction.execute(
                "INSERT INTO backlog_outcome_observation
                    (uid, backlog_uid, ordinal, status, outcome, evidence, observed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(uid) DO NOTHING;",
                params![
                    uid,
                    backlog_uid,
                    required_i64(payload, "ordinal")?,
                    status,
                    required_string(payload, "outcome")?,
                    optional_string(payload, "evidence"),
                    required_timestamp(payload, "observed_at")?,
                ],
            )?
        }
        "decision.add" if version >= 2 => transaction.execute(
            "INSERT INTO decision (id, created_at, title, status, doc_path, verify_command, predicted_impact, notes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
            params![
                required_string(operation, "id")?,
                required_timestamp(payload, "created_at")?,
                required_string(payload, "title")?,
                required_string(payload, "status")?,
                optional_string(payload, "doc_path"),
                optional_string(payload, "verify_command"),
                optional_string(payload, "predicted_impact"),
                optional_string(payload, "notes"),
            ],
        )?,
        "decision.add" => transaction.execute(
            "INSERT INTO decision (id, title, status, doc_path, verify_command, predicted_impact, notes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
            params![
                required_string(operation, "id")?,
                required_string(payload, "title")?,
                required_string(payload, "status")?,
                optional_string(payload, "doc_path"),
                optional_string(payload, "verify_command"),
                optional_string(payload, "predicted_impact"),
                optional_string(payload, "notes"),
            ],
        )?,
        "decision.verify" => {
            let id = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("decision", "decision", "id", &id),
            )?;
            transaction.execute(
                "UPDATE decision
             SET last_verified_at=?1, last_verified_result=?2
             WHERE id=?3;",
            params![
                if version >= 2 {
                    required_timestamp(payload, "verified_at")?
                } else {
                    transaction.query_row("SELECT datetime('now');", [], |row| row.get(0))?
                },
                required_string(payload, "result")?,
                id,
            ],
            )?
        }
        "backlog.proposal.decision" => {
            let uid = required_uid(operation, "uid", "blg")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("backlog", "backlog", "uid", &uid),
            )?;
            let accepted_at = if version >= 2 {
                optional_timestamp(payload, "accepted_at")?
            } else {
                optional_string(payload, "accepted_at")
            };
            let closed_at = if version >= 2 {
                optional_timestamp(payload, "closed_at")?
            } else {
                optional_string(payload, "closed_at")
            };
            let created_at = if version >= 2 {
                optional_timestamp(payload, "created_at")?
            } else {
                optional_string(payload, "created_at")
            }
            .unwrap_or(transaction.query_row("SELECT datetime('now');", [], |row| row.get(0))?);
            transaction.execute(
                "INSERT INTO backlog (uid, proposal_key, predecessor_uid, occurrence_kind, created_at, title, discovered_while, current_pain, suggested_improvement, risk, status, predicted_impact, notes, accepted_at, closed_at, outcome_schedule_kind, outcome_due_at, outcome_after_traces, outcome_baseline_trace_count, rejection_reason)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, NULL, ?19)
                 ON CONFLICT(uid) DO UPDATE SET
                   status=excluded.status, notes=excluded.notes, accepted_at=excluded.accepted_at,
                   closed_at=excluded.closed_at, outcome_schedule_kind=excluded.outcome_schedule_kind,
                   outcome_due_at=excluded.outcome_due_at, outcome_after_traces=excluded.outcome_after_traces,
                   rejection_reason=excluded.rejection_reason;",
                params![uid, required_string(payload, "proposal_key")?, optional_string(payload, "predecessor_uid"), required_string(payload, "occurrence_kind")?, created_at, required_string(payload, "title")?, optional_string(payload, "discovered_while"), optional_string(payload, "current_pain"), optional_string(payload, "suggested_improvement"), optional_string(payload, "risk"), required_string(payload, "status")?, optional_string(payload, "predicted_impact"), optional_string(payload, "notes"), accepted_at, closed_at, optional_string(payload, "outcome_schedule_kind"), optional_string(payload, "outcome_due_at"), optional_i64(payload, "outcome_after_traces"), optional_string(payload, "rejection_reason")]
            )?;
            if let Some(evidence) = payload.get("evidence").and_then(Value::as_array) {
                for item in evidence {
                    let observed_at = if version >= 2 {
                        Some(required_timestamp(item, "observed_at")?)
                    } else {
                        optional_string(item, "observed_at")
                    };
                    transaction.execute(
                        "INSERT OR IGNORE INTO proposal_evidence_link (backlog_uid, source_kind, evidence_uid, evidence_fingerprint, observed_at)
                         VALUES (?1, ?2, ?3, ?4, COALESCE(?5, datetime('now')));",
                        params![uid, required_string(item, "source_kind")?, required_string(item, "evidence_uid")?, required_string(item, "evidence_fingerprint")?, observed_at],
                    )?;
                }
            } else {
                transaction.execute(
                    "INSERT OR IGNORE INTO proposal_evidence_link (backlog_uid, source_kind, evidence_uid, evidence_fingerprint, observed_at)
                     VALUES (?1, 'legacy_snapshot', ?2, ?3, datetime('now'));",
                    params![uid, required_string(payload, "evidence_uid")?, required_string(payload, "evidence_fingerprint")?]
                )?;
            }
            1
        }
        "backlog.add" if version == 2 => {
            let uid = required_uid(operation, "uid", "blg")?;
            transaction.execute(
                "INSERT INTO backlog (uid, created_at, proposal_key, occurrence_kind, title, discovered_while, current_pain, suggested_improvement, risk, predicted_impact, notes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11);",
                params![uid, required_timestamp(payload, "created_at")?, optional_string(payload, "proposal_key"), optional_string(payload, "occurrence_kind"), required_string(payload, "title")?, optional_string(payload, "discovered_while"), optional_string(payload, "current_pain"), optional_string(payload, "suggested_improvement"), optional_string(payload, "risk"), optional_string(payload, "predicted_impact"), optional_string(payload, "notes")],
            )?;
            1
        }
        "backlog.add" => {
            let source_id = required_i64(operation, "id")?;
            transaction.execute(
            "INSERT INTO backlog (
                title, discovered_while, current_pain, suggested_improvement,
                risk, predicted_impact, notes
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
            params![
                required_string(payload, "title")?,
                optional_string(payload, "discovered_while"),
                optional_string(payload, "current_pain"),
                optional_string(payload, "suggested_improvement"),
                optional_string(payload, "risk"),
                optional_string(payload, "predicted_impact"),
                optional_string(payload, "notes"),
            ],
            )?;
            context
                .backlog_ids
                .insert(source_id, transaction.last_insert_rowid());
            1
        }
        "backlog.close" if version >= 3 => {
            let uid = required_uid(operation, "uid", "blg")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("backlog", "backlog", "uid", &uid),
            )?;
            transaction.execute(
                "UPDATE backlog
             SET status=?1, actual_outcome=?2, implemented_at=?3
             WHERE uid=?4;",
            params![
                required_string(payload, "status")?,
                optional_string(payload, "actual_outcome"),
                required_timestamp(payload, "implemented_at")?,
                uid,
            ],
            )?
        }
        "backlog.close" => transaction.execute(
            "UPDATE backlog
             SET status=?1, actual_outcome=?2, implemented_at=datetime('now')
             WHERE id=?3;",
            params![
                required_string(payload, "status")?,
                optional_string(payload, "actual_outcome"),
                mapped_id(Some(required_i64(operation, "id")?), &context.backlog_ids),
            ],
        )?,
        "tool.register" if version >= 2 => transaction.execute(
            "INSERT INTO tool
                (name, provider, created_at, command, description, args, responsibility, since,
                 kind, capability, scan_target, status)
             VALUES (?1, 'custom', ?2, ?3, ?4, ?5, ?6, 'registered', ?7, ?8, ?9, 'unknown');",
            params![
                required_string(operation, "id")?,
                required_timestamp(payload, "created_at")?,
                required_string(payload, "command")?,
                required_string(payload, "description")?,
                optional_string(payload, "args"),
                required_string(payload, "responsibility")?,
                required_string(payload, "kind")?,
                optional_string(payload, "capability"),
                optional_string(payload, "scan_target"),
            ],
        )?,
        "tool.register" => transaction.execute(
            "INSERT INTO tool
                (name, provider, command, description, args, responsibility, since,
                 kind, capability, scan_target, status)
             VALUES (?1, 'custom', ?2, ?3, ?4, ?5, 'registered', ?6, ?7, ?8, 'unknown');",
            params![
                required_string(operation, "id")?,
                required_string(payload, "command")?,
                required_string(payload, "description")?,
                optional_string(payload, "args"),
                required_string(payload, "responsibility")?,
                required_string(payload, "kind")?,
                optional_string(payload, "capability"),
                optional_string(payload, "scan_target"),
            ],
        )?,
        "tool.check" => {
            let name = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("tool", "tool", "name", &name),
            )?;
            transaction.execute(
                "UPDATE tool SET status=?1, checked_at=?2 WHERE name=?3;",
            params![
                required_string(payload, "status")?,
                if version >= 2 {
                    required_timestamp(payload, "checked_at")?
                } else {
                    transaction.query_row("SELECT datetime('now');", [], |row| row.get(0))?
                },
                name,
            ],
            )?
        }
        "tool.remove" => {
            let name = required_string(operation, "id")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                ("tool", "tool", "name", &name),
            )?;
            transaction.execute("DELETE FROM tool WHERE name=?1;", params![name])?
        }
        "intervention.add" if version == 2 => {
            let uid = required_uid(operation, "uid", "int")?;
            let trace_id = resolve_uid(transaction, optional_string(payload, "trace_uid"), "trace")?;
            transaction.execute(
            "INSERT INTO intervention (uid, created_at, trace_id, story_id, type, description, source, impact)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
            params![uid, required_timestamp(payload, "created_at")?, trace_id, optional_string(payload, "story_id"), required_string(payload, "type")?, required_string(payload, "description")?, required_string(payload, "source")?, optional_string(payload, "impact")],
            )?
        }
        "intervention.add" => transaction.execute(
            "INSERT INTO intervention (trace_id, story_id, type, description, source, impact)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
            params![
                mapped_id(optional_i64(payload, "trace_id"), &context.trace_ids),
                optional_string(payload, "story_id"),
                required_string(payload, "type")?,
                required_string(payload, "description")?,
                required_string(payload, "source")?,
                optional_string(payload, "impact"),
            ],
        )?,
        "trace.add" if version == 2 => {
            let uid = required_uid(operation, "uid", "trc")?;
            let intake_id = resolve_uid(transaction, optional_string(payload, "intake_uid"), "intake")?;
            transaction.execute(
            "INSERT INTO trace (uid, created_at, recorded_at_unix_ns, intake_uid, task_summary, intake_id, story_id, agent,
                actions_taken, files_read, files_changed, decisions_made, errors,
                outcome, duration_seconds, token_estimate, harness_friction, notes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
             ON CONFLICT(uid) DO NOTHING;",
            params![uid, required_timestamp(payload, "created_at")?, optional_i64(payload, "recorded_at_unix_ns"), optional_string(payload, "intake_uid"), required_string(payload, "task_summary")?, intake_id, optional_string(payload, "story_id"), optional_string(payload, "agent"), optional_string(payload, "actions_taken"), optional_string(payload, "files_read"), optional_string(payload, "files_changed"), optional_string(payload, "decisions_made"), optional_string(payload, "errors"), optional_string(payload, "outcome"), optional_i64(payload, "duration_seconds"), optional_i64(payload, "token_estimate"), optional_string(payload, "harness_friction"), optional_string(payload, "notes")],
            )?;
            1
        }
        "audit.evidence.open" => {
            transaction.execute(
                "INSERT INTO audit_evidence_episode (uid, finding_key, evidence_fingerprint, opened_at) VALUES (?1, ?2, ?3, ?4)",
                params![required_string(operation, "uid")?, required_string(payload, "finding_key")?, required_string(payload, "evidence_fingerprint")?, required_timestamp(payload, "opened_at")?],
            )?;
            1
        }
        "audit.evidence.clear" => {
            let uid = required_string(operation, "uid")?;
            enforce_revision_guard(
                transaction,
                operation,
                version,
                changeset_id,
                (
                    "audit_evidence_episode",
                    "audit_evidence_episode",
                    "uid",
                    &uid,
                ),
            )?;
            transaction.execute(
                "UPDATE audit_evidence_episode SET cleared_at=?1 WHERE uid=?2",
                params![required_timestamp(payload, "cleared_at")?, uid],
            )?;
            1
        }
        "trace.add" => {
            let source_id = required_i64(operation, "id")?;
            transaction.execute(
            "INSERT INTO trace (
                task_summary, intake_id, story_id, agent,
                actions_taken, files_read, files_changed, decisions_made, errors,
                outcome, duration_seconds, token_estimate, harness_friction, notes
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);",
            params![
                required_string(payload, "task_summary")?,
                mapped_id(optional_i64(payload, "intake_id"), &context.intake_ids),
                optional_string(payload, "story_id"),
                optional_string(payload, "agent"),
                optional_string(payload, "actions_taken"),
                optional_string(payload, "files_read"),
                optional_string(payload, "files_changed"),
                optional_string(payload, "decisions_made"),
                optional_string(payload, "errors"),
                optional_string(payload, "outcome"),
                optional_i64(payload, "duration_seconds"),
                optional_i64(payload, "token_estimate"),
                optional_string(payload, "harness_friction"),
                optional_string(payload, "notes"),
            ],
            )?;
            context
                .trace_ids
                .insert(source_id, transaction.last_insert_rowid());
            1
        }
        _ => return Err(HarnessInfraError::UnsupportedChangesetOp(op)),
    };
    Ok(())
}

fn enforce_revision_guard(
    transaction: &Transaction<'_>,
    operation: &Value,
    version: i64,
    changeset_id: &str,
    entity: (&str, &str, &str, &str),
) -> Result<()> {
    if version < 3 {
        return Ok(());
    }
    let (entity_kind, table, key_column, entity_id) = entity;
    let expected_revision = required_i64(operation, "expected_revision")?;
    let query = format!("SELECT revision FROM {table} WHERE {key_column}=?1;");
    let actual_revision = transaction
        .query_row(&query, params![entity_id], |row| row.get::<_, i64>(0))
        .optional()?;
    if actual_revision == Some(expected_revision) {
        Ok(())
    } else {
        Err(HarnessInfraError::EntityRevisionConflict {
            changeset_id: changeset_id.to_owned(),
            entity_kind: entity_kind.to_owned(),
            entity_id: entity_id.to_owned(),
            expected_revision,
            actual_revision,
        })
    }
}

fn ensure_story_exists(transaction: &Transaction<'_>, id: &str) -> Result<()> {
    let exists = transaction
        .query_row("SELECT 1 FROM story WHERE id=?1;", params![id], |_| Ok(()))
        .optional()?
        .is_some();
    if exists {
        Ok(())
    } else {
        Err(HarnessInfraError::StoryNotFound(id.to_owned()))
    }
}

fn reject_ordinary_story_implementation(id: &str, status: Option<&str>) -> Result<()> {
    if status.is_some_and(|status| status.trim().eq_ignore_ascii_case("implemented")) {
        return Err(HarnessInfraError::StoryImplementedRequiresCompletion(
            id.to_owned(),
        ));
    }
    Ok(())
}

fn story_completion_context(
    connection: &Connection,
    story_id: &str,
) -> Result<StoryCompletionContext> {
    let resolver_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM story_backlog_link WHERE story_id=?1 AND relationship='resolves';",
        params![story_id],
        |row| row.get(0),
    )?;
    if resolver_count == 0 {
        return Ok(StoryCompletionContext::default());
    }

    let intake_exists = connection
        .query_row(
            "SELECT 1 FROM intake WHERE story_id=?1 AND input_type='harness_improvement' AND uid IS NOT NULL LIMIT 1;",
            params![story_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !intake_exists {
        return Err(HarnessInfraError::StoryCompletion(
            "resolver stories require a linked harness_improvement intake with stable identity"
                .to_owned(),
        ));
    }

    let proof: Option<(String, String)> = connection
        .query_row(
            "SELECT intake.uid, trace.uid
             FROM intake
             JOIN trace ON trace.intake_uid=intake.uid
             WHERE intake.story_id=?1
               AND intake.input_type='harness_improvement'
               AND intake.uid IS NOT NULL
               AND trace.story_id=?1
               AND trace.uid IS NOT NULL
               AND trace.outcome='completed'
               AND COALESCE(trace.actions_taken,'') NOT IN ('','[]')
               AND COALESCE(trace.files_changed,'') NOT IN ('','[]')
               AND (
                   (
                       (SELECT linked_at_unix_ns
                        FROM story_backlog_link
                        WHERE story_id=?1 AND relationship='resolves'
                        ORDER BY linked_at DESC,
                                 linked_at_unix_ns IS NOT NULL DESC,
                                 backlog_uid DESC
                        LIMIT 1) IS NOT NULL
                       AND trace.recorded_at_unix_ns > (
                           SELECT linked_at_unix_ns FROM story_backlog_link
                           WHERE story_id=?1 AND relationship='resolves'
                           ORDER BY linked_at DESC,
                                    linked_at_unix_ns IS NOT NULL DESC,
                                    backlog_uid DESC
                           LIMIT 1
                       )
                   )
                   OR (
                       (SELECT linked_at_unix_ns
                        FROM story_backlog_link
                        WHERE story_id=?1 AND relationship='resolves'
                        ORDER BY linked_at DESC,
                                 linked_at_unix_ns IS NOT NULL DESC,
                                 backlog_uid DESC
                        LIMIT 1) IS NULL
                       AND trace.created_at > (
                           SELECT linked_at FROM story_backlog_link
                           WHERE story_id=?1 AND relationship='resolves'
                           ORDER BY linked_at DESC,
                                    linked_at_unix_ns IS NOT NULL DESC,
                                    backlog_uid DESC
                           LIMIT 1
                       )
                   )
               )
             ORDER BY trace.id DESC
             LIMIT 1;",
            params![story_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (intake_uid, trace_uid) = proof.ok_or_else(|| {
        HarnessInfraError::StoryCompletion(
            "resolver stories require a completed linked implementation trace recorded after the newest resolver link"
                .to_owned(),
        )
    })?;

    let invalid_target: Option<(i64, String)> = connection
        .query_row(
            "SELECT backlog.id, backlog.status
             FROM story_backlog_link AS link
             JOIN backlog ON backlog.uid=link.backlog_uid
             WHERE link.story_id=?1
               AND link.relationship='resolves'
               AND NOT (
                   backlog.status='accepted'
                   OR (
                       backlog.status='implemented'
                       AND json_extract(backlog.resolution_evidence, '$.story_id')=?1
                   )
               )
             ORDER BY backlog.id
             LIMIT 1;",
            params![story_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((backlog_id, backlog_status)) = invalid_target {
        return Err(HarnessInfraError::StoryCompletion(format!(
            "resolver target #{backlog_id} is '{backlog_status}', not accepted or already completed by this story"
        )));
    }

    Ok(StoryCompletionContext {
        intake_uid: Some(intake_uid),
        trace_uid: Some(trace_uid),
    })
}

#[allow(clippy::type_complexity)]
fn story_completion_links(
    connection: &Connection,
    story_id: &str,
) -> Result<(Vec<(i64, String, Option<String>)>, Vec<i64>, Vec<i64>)> {
    let mut accepted = connection.prepare(
        "SELECT backlog.id, backlog.uid, backlog.outcome_schedule_kind
         FROM story_backlog_link AS link
         JOIN backlog ON backlog.uid=link.backlog_uid
         WHERE link.story_id=?1 AND link.relationship='resolves' AND backlog.status='accepted'
         ORDER BY backlog.id;",
    )?;
    let accepted = accepted
        .query_map(params![story_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut already_closed = connection.prepare(
        "SELECT backlog.id
         FROM story_backlog_link AS link
         JOIN backlog ON backlog.uid=link.backlog_uid
         WHERE link.story_id=?1
           AND link.relationship='resolves'
           AND backlog.status='implemented'
           AND json_extract(backlog.resolution_evidence, '$.story_id')=?1
         ORDER BY backlog.id;",
    )?;
    let already_closed = already_closed
        .query_map(params![story_id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut references = connection.prepare(
        "SELECT backlog.id
         FROM story_backlog_link AS link
         JOIN backlog ON backlog.uid=link.backlog_uid
         WHERE link.story_id=?1 AND link.relationship='references'
         ORDER BY backlog.id;",
    )?;
    let references = references
        .query_map(params![story_id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok((accepted, already_closed, references))
}

fn linked_backlog(transaction: &Transaction<'_>, backlog_id: i64) -> Result<(String, String)> {
    let row: Option<(Option<String>, String)> = transaction
        .query_row(
            "SELECT uid, status FROM backlog WHERE id=?1;",
            params![backlog_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match row {
        None => Err(HarnessInfraError::StoryBacklogNotFound(backlog_id)),
        Some((None, _)) => Err(HarnessInfraError::StoryBacklogLegacy(backlog_id)),
        Some((Some(uid), status)) => Ok((uid, status)),
    }
}

fn validate_resolver_mutation(
    transaction: &Transaction<'_>,
    story_id: &str,
    backlog_id: i64,
    backlog_status: &str,
    story_status: &str,
    backlog_uid: &str,
) -> Result<()> {
    if matches!(story_status, "implemented" | "retired") {
        return Err(HarnessInfraError::StoryBacklogTerminalStory(
            story_id.to_owned(),
        ));
    }
    if backlog_status != "accepted" {
        if matches!(backlog_status, "implemented" | "rejected") {
            return Err(HarnessInfraError::StoryBacklogResolverImmutable(backlog_id));
        }
        return Err(HarnessInfraError::StoryBacklogResolverRequiresAccepted(
            backlog_id,
        ));
    }
    let resolver: Option<String> = transaction.query_row("SELECT story_id FROM story_backlog_link WHERE backlog_uid=?1 AND relationship='resolves' AND story_id<>?2;", params![backlog_uid, story_id], |row| row.get(0)).optional()?;
    if let Some(resolver) = resolver {
        return Err(HarnessInfraError::StoryBacklogResolverExists(
            backlog_id, resolver,
        ));
    }
    Ok(())
}

fn dependency_path_exists(transaction: &Transaction<'_>, from: &str, to: &str) -> Result<bool> {
    transaction
        .query_row(
            "WITH RECURSIVE reachable(id) AS (
                SELECT blocks_story_id FROM story_dependency WHERE story_id=?1
                UNION
                SELECT dependency.blocks_story_id
                FROM story_dependency AS dependency
                JOIN reachable ON dependency.story_id=reachable.id
             )
             SELECT 1 FROM reachable WHERE id=?2 LIMIT 1;",
            params![from, to],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(HarnessInfraError::from)
}

fn hierarchy_path_exists(transaction: &Transaction<'_>, from: &str, to: &str) -> Result<bool> {
    transaction
        .query_row(
            "WITH RECURSIVE reachable(id) AS (
                SELECT child_story_id FROM story_hierarchy WHERE parent_story_id=?1
                UNION
                SELECT hierarchy.child_story_id
                FROM story_hierarchy AS hierarchy
                JOIN reachable ON hierarchy.parent_story_id=reachable.id
             )
             SELECT 1 FROM reachable WHERE id=?2 LIMIT 1;",
            params![from, to],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(HarnessInfraError::from)
}

fn required_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| HarnessInfraError::InvalidChangeset(format!("missing string field {field}")))
}

fn required_uid(value: &Value, field: &str, prefix: &str) -> Result<String> {
    let uid = required_string(value, field)?;
    let valid = uid.strip_prefix(&format!("{prefix}_")).is_some_and(|hex| {
        hex.len() == 32 && hex.chars().all(|character| character.is_ascii_hexdigit())
    });
    if valid {
        Ok(uid)
    } else {
        Err(HarnessInfraError::InvalidChangeset(format!(
            "invalid {prefix} uid in {field}"
        )))
    }
}

fn resolve_uid(
    transaction: &Transaction<'_>,
    uid: Option<String>,
    table: &str,
) -> Result<Option<i64>> {
    let Some(uid) = uid else { return Ok(None) };
    let sql = format!("SELECT id FROM {table} WHERE uid=?1");
    transaction
        .query_row(&sql, params![uid], |row| row.get(0))
        .optional()?
        .map(Some)
        .ok_or_else(|| HarnessInfraError::InvalidChangeset(format!("unknown {table} uid")))
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn canonical_sqlite_timestamp(value: String, field: &str) -> Result<String> {
    let parsed = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S").map_err(|_| {
        HarnessInfraError::InvalidChangeset(format!(
            "{field} must use YYYY-MM-DD HH:MM:SS, got '{value}'"
        ))
    })?;
    if parsed.format("%Y-%m-%d %H:%M:%S").to_string() != value {
        return Err(HarnessInfraError::InvalidChangeset(format!(
            "{field} must use canonical YYYY-MM-DD HH:MM:SS, got '{value}'"
        )));
    }
    Ok(value)
}

fn required_timestamp(value: &Value, field: &str) -> Result<String> {
    canonical_sqlite_timestamp(required_string(value, field)?, field)
}

fn optional_timestamp(value: &Value, field: &str) -> Result<Option<String>> {
    optional_string(value, field)
        .map(|timestamp| canonical_sqlite_timestamp(timestamp, field))
        .transpose()
}

fn required_i64(value: &Value, field: &str) -> Result<i64> {
    value.get(field).and_then(Value::as_i64).ok_or_else(|| {
        HarnessInfraError::InvalidChangeset(format!("missing integer field {field}"))
    })
}

fn optional_i64(value: &Value, field: &str) -> Option<i64> {
    value.get(field).and_then(Value::as_i64)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::application::{
        BacklogAddInput, BacklogCloseInput, BacklogOutcomeInput, DecisionAddInput, IntakeInput,
        InterventionAddInput, InterventionFilter, StoryAddInput, StoryDependencyInput,
        StoryHierarchyInput, StoryUpdateInput, ToolRegisterInput, TraceInput,
    };
    use crate::domain::{BacklogFilter, BoolFlag, CsvList, InputType, RiskLane, TraceQualityTier};

    fn env_clean_verification_command() -> &'static str {
        if cfg!(windows) {
            "if defined HARNESS_RUN_ID (exit /b 1) else (if defined HARNESS_RUN_MODE (exit /b 1) else (if defined HARNESS_DB_PATH (exit /b 1) else (exit /b 0)))"
        } else {
            "test -z \"${HARNESS_RUN_ID-}\" && test -z \"${HARNESS_RUN_MODE-}\" && test -z \"${HARNESS_DB_PATH-}\""
        }
    }

    fn test_repository() -> (TempDir, SqliteHarnessRepository) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf();
        let repository = SqliteHarnessRepository::new(
            repo_root.clone(),
            temp_dir.path().join("harness.db"),
            repo_root.join("scripts/schema"),
        );
        (temp_dir, repository)
    }

    fn isolated_test_repository() -> (TempDir, SqliteHarnessRepository) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        fs::create_dir_all(&repo_root).unwrap();
        let schema_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf();
        let repository = SqliteHarnessRepository::new(
            repo_root.clone(),
            repo_root.join("harness.db"),
            schema_root.join("scripts/schema"),
        );
        (temp_dir, repository)
    }

    fn source_tracked_test_repository() -> (TempDir, SqliteHarnessRepository) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        fs::create_dir_all(repo_root.join("crates/harness-cli")).unwrap();
        fs::write(repo_root.join("Cargo.toml"), "[workspace]\n").unwrap();
        fs::write(
            repo_root.join("crates/harness-cli/Cargo.toml"),
            "[package]\nname = \"harness-cli\"\nversion = \"0.0.0\"\n",
        )
        .unwrap();
        let schema_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf();
        let repository = SqliteHarnessRepository::new(
            repo_root.clone(),
            repo_root.join("harness.db"),
            schema_root.join("scripts/schema"),
        );
        (temp_dir, repository)
    }

    fn automatic_capture_intake(repository: &SqliteHarnessRepository, summary: &str) {
        repository
            .record_intake(IntakeInput {
                input_type: InputType::HarnessImprovement,
                summary: summary.to_owned(),
                risk_lane: RiskLane::Normal,
                risk_flags: CsvList::from_optional(None),
                affected_docs: CsvList::from_optional(None),
                story_id: None,
                notes: None,
            })
            .unwrap();
    }

    fn passing_command() -> &'static str {
        if cfg!(windows) {
            "exit /b 0"
        } else {
            "exit 0"
        }
    }

    fn failing_command() -> &'static str {
        if cfg!(windows) {
            "exit /b 1"
        } else {
            "exit 1"
        }
    }

    fn record_proposal_friction(repository: &SqliteHarnessRepository, text: &str) {
        repository
            .record_trace(TraceInput {
                task_summary: "Proposal recurrence fixture".to_owned(),
                intake_id: None,
                story_id: None,
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: Some(text.to_owned()),
                notes: None,
                actions: CsvList::from_optional(Some("observe".to_owned())),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(None),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
    }

    fn seed_legacy_reconciliation_fixture(repository: &SqliteHarnessRepository, offset_ids: bool) {
        let connection = repository.open_existing().unwrap();
        if offset_ids {
            connection
                .execute("INSERT INTO trace (task_summary) VALUES ('offset');", [])
                .unwrap();
            connection.execute("INSERT INTO intervention (type, description, source) VALUES ('approval', 'offset', 'agent');", []).unwrap();
            connection
                .execute("INSERT INTO backlog (title) VALUES ('offset');", [])
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO trace (created_at, task_summary, harness_friction) VALUES
                ('2026-01-01 00:00:01', 'legacy friction one', 'phase5 repeated smoke friction'),
                ('2026-01-01 00:00:02', 'legacy friction two', 'phase5 repeated smoke friction');",
                [],
            )
            .unwrap();
        connection.execute(
            "INSERT INTO intervention (created_at, type, description, source) VALUES
                ('2026-01-01 00:00:03', 'correction', 'Use deterministic context scoring rules', 'human'),
                ('2026-01-01 00:00:04', 'correction', 'Use deterministic context scoring rules', 'human');",
            [],
        ).unwrap();
        connection.execute(
            "INSERT INTO backlog (created_at, title, status, notes) VALUES
                ('2026-01-01 00:00:05', 'Reduce repeated friction: phase5 repeated smoke friction', 'proposed', 'legacy generated row');",
            [],
        ).unwrap();
        connection.execute(
            "INSERT INTO backlog (created_at, title, status, actual_outcome, implemented_at, notes) VALUES
                ('2026-01-01 00:00:06', 'Address repeated intervention: correction: Use deterministic context scoring rules', 'implemented', 'Kept the original legacy result', '2026-01-02 00:00:00', 'legacy generated terminal row');",
            [],
        ).unwrap();
        connection
            .execute(
                "INSERT INTO backlog (created_at, title, status) VALUES
                ('2026-01-01 00:00:07', 'Manual improvement with similar wording', 'proposed');",
                [],
            )
            .unwrap();
    }

    fn add_completion_story(
        repository: &SqliteHarnessRepository,
        id: &str,
        verify_command: Option<&str>,
    ) {
        repository
            .add_story(StoryAddInput {
                id: id.to_owned(),
                title: "Completion fixture".to_owned(),
                risk_lane: RiskLane::HighRisk,
                contract_doc: None,
                verify_command: verify_command.map(ToOwned::to_owned),
                notes: None,
            })
            .unwrap();
        repository
            .update_story(StoryUpdateInput {
                id: id.to_owned(),
                contract_doc: None,
                status: Some("in_progress".to_owned()),
                evidence: None,
                unit: None,
                integration: None,
                e2e: None,
                platform: None,
                verify_command: None,
            })
            .unwrap();
    }

    fn seed_resolver_completion_fixture(
        repository: &SqliteHarnessRepository,
        story_id: &str,
        verify_command: &str,
    ) -> (i64, String, String) {
        add_completion_story(repository, story_id, Some(verify_command));
        let connection = repository.open_existing().unwrap();
        let intake_uid = "ink_11111111111111111111111111111111".to_owned();
        let trace_uid = "trc_22222222222222222222222222222222".to_owned();
        let backlog_uid = "blg_33333333333333333333333333333333".to_owned();
        connection.execute("INSERT INTO intake (uid, created_at, input_type, summary, risk_lane, story_id) VALUES (?1, '2026-01-01 00:00:00', 'harness_improvement', 'completion fixture', 'high_risk', ?2);", params![intake_uid, story_id]).unwrap();
        connection.execute("INSERT INTO backlog (uid, title, status, risk, outcome_schedule_kind, outcome_after_traces) VALUES (?1, 'accepted resolver', 'accepted', 'high_risk', 'trace_count', 5);", params![backlog_uid]).unwrap();
        let backlog_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at) VALUES (?1, ?2, 'resolves', '2026-01-02 00:00:00');", params![story_id, backlog_uid]).unwrap();
        connection.execute("INSERT INTO trace (uid, created_at, intake_uid, task_summary, story_id, actions_taken, files_changed, outcome) VALUES (?1, '2026-01-03 00:00:00', ?2, 'completed implementation trace', ?3, '[\"implemented\"]', '[\"src.rs\"]', 'completed');", params![trace_uid, intake_uid, story_id]).unwrap();
        (backlog_id, intake_uid, trace_uid)
    }

    fn story_columns(connection: &Connection) -> Vec<String> {
        let mut statement = connection.prepare("PRAGMA table_info(story);").unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap();
        rows.collect::<std::result::Result<Vec<_>, _>>().unwrap()
    }

    #[test]
    fn init_creates_database_and_schema() {
        let (_temp_dir, repository) = test_repository();

        let result = repository.init().unwrap();

        assert!(matches!(result, InitResult::Created { .. }));
        assert_eq!(repository.query_stats().unwrap().intakes, 0);
        let connection = repository.open_existing().unwrap();
        let schema_version = SqliteHarnessRepository::schema_version(&connection).unwrap();
        assert_eq!(schema_version, 14);
        let story_columns = story_columns(&connection);
        assert!(story_columns.contains(&"verify_command".to_owned()));
        assert!(story_columns.contains(&"last_verified_at".to_owned()));
        assert!(story_columns.contains(&"last_verified_result".to_owned()));
        assert!(story_columns.contains(&"revision".to_owned()));
        let dependency_table_exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='story_dependency';",
                [],
                |_| Ok(()),
            )
            .is_ok();
        assert!(dependency_table_exists);
        let hierarchy_table_exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='story_hierarchy';",
                [],
                |_| Ok(()),
            )
            .is_ok();
        assert!(hierarchy_table_exists);
    }

    #[test]
    fn legacy_proposal_reconciliation_migrates_an_existing_v10_database() {
        let (_temp_dir, repository) = test_repository();
        let connection = repository.open_or_create().unwrap();
        repository.apply_schema_v1(&connection).unwrap();
        for (version, path) in repository.migration_files().unwrap() {
            if (2..=10).contains(&version) {
                connection
                    .execute_batch(&fs::read_to_string(path).unwrap())
                    .unwrap();
            }
        }
        drop(connection);

        let result = repository.migrate().unwrap();
        assert_eq!(result.applied, vec![11, 12, 13, 14]);
        let connection = repository.open_existing().unwrap();
        assert_eq!(
            SqliteHarnessRepository::schema_version(&connection).unwrap(),
            14
        );
        assert!(connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='legacy_evidence_snapshot';",
                [],
                |_| Ok(())
            )
            .is_ok());
    }

    #[test]
    fn post_review_migration_backfills_exact_legacy_rejection_reason() {
        let (temp_dir, repository) = test_repository();
        let connection = repository.open_or_create().unwrap();
        repository.apply_schema_v1(&connection).unwrap();
        for (version, path) in repository.migration_files().unwrap() {
            if (2..=11).contains(&version) {
                connection
                    .execute_batch(&fs::read_to_string(path).unwrap())
                    .unwrap();
            }
        }
        connection.execute(
            "INSERT INTO backlog (
                uid, proposal_key, occurrence_kind, title, status, notes
             ) VALUES (
                'blg_99999999999999999999999999999999',
                'prp_99999999999999999999999999999999',
                'original', 'legacy rejection', 'rejected',
                'covered_evidence: old fixture\nrejection_reason: not useful yet\nrejection_reason: ignored duplicate'
             )",
            [],
        ).unwrap();
        drop(connection);

        assert_eq!(repository.migrate().unwrap().applied, vec![12, 13, 14]);
        let connection = repository.open_existing().unwrap();
        let reason: Option<String> = connection
            .query_row(
                "SELECT rejection_reason FROM backlog WHERE proposal_key='prp_99999999999999999999999999999999'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reason.as_deref(), Some("not useful yet"));

        let replay_root = temp_dir.path().join("rejection-replay");
        fs::create_dir_all(&replay_root).unwrap();
        let replay = SqliteHarnessRepository::new(
            replay_root.clone(),
            replay_root.join("harness.db"),
            repository.schema_dir.clone(),
        );
        replay.init().unwrap();
        let changeset = temp_dir.path().join("rejection.changeset.jsonl");
        fs::write(
            &changeset,
            r#"{"op":"changeset.header","version":1,"run_id":"run_rejection_parity","base_schema_version":11}
{"op":"backlog.proposal.decision","version":2,"uid":"blg_99999999999999999999999999999999","payload":{"proposal_key":"prp_99999999999999999999999999999999","occurrence_kind":"original","title":"legacy rejection","status":"rejected","notes":"covered_evidence: old fixture\nrejection_reason: not useful yet\nrejection_reason: ignored duplicate","rejection_reason":"not useful yet","evidence":[]}}
"#,
        )
        .unwrap();
        replay.apply_changeset(&changeset).unwrap();
        let replay_reason: Option<String> = replay
            .open_existing()
            .unwrap()
            .query_row(
                "SELECT rejection_reason FROM backlog WHERE proposal_key='prp_99999999999999999999999999999999'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(replay_reason, reason);
    }

    #[test]
    fn semantic_integrity_rejects_noncanonical_timestamps_across_operation_families() {
        assert_eq!(
            canonical_sqlite_timestamp("2099-01-02 03:04:05".to_owned(), "time").unwrap(),
            "2099-01-02 03:04:05"
        );
        for invalid in ["garbage", "2099-1-2 3:4:5", "2099-02-30 03:04:05"] {
            assert!(canonical_sqlite_timestamp(invalid.to_owned(), "time").is_err());
        }

        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        add_completion_story(&repository, "US-TIME-LINK", Some(passing_command()));
        repository.open_existing().unwrap().execute(
            "INSERT INTO backlog (uid, title, status) VALUES ('blg_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'time link', 'accepted')",
            [],
        ).unwrap();
        let operations = vec![
            json!({"op":"intake.add","version":2,"uid":"ink_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"created_at":"garbage","input_type":"harness_improvement","summary":"bad","risk_lane":"high_risk"}}),
            json!({"op":"backlog.add","version":2,"uid":"blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"created_at":"garbage","title":"bad"}}),
            json!({"op":"intervention.add","version":2,"uid":"int_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"created_at":"garbage","type":"correction","description":"bad","source":"agent"}}),
            json!({"op":"trace.add","version":2,"uid":"trc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"created_at":"garbage","task_summary":"bad"}}),
            json!({"op":"story.backlog.link","version":2,"id":"US-TIME-LINK","payload":{"backlog_uid":"blg_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","relationship":"resolves","linked_at":"garbage","linked_at_unix_ns":1}}),
            json!({"op":"story.verify","version":2,"id":"US-NONE","payload":{"result":"pass","verified_at":"garbage"}}),
            json!({"op":"story.complete","version":2,"id":"US-NONE","payload":{"result":"pass","completed_at":"garbage"}}),
            json!({"op":"backlog.complete","version":2,"uid":"blg_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","payload":{"story_id":"US-NONE","completed_at":"garbage"}}),
            json!({"op":"backlog.outcome.observe","version":2,"uid":"obs_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"backlog_uid":"blg_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ordinal":1,"status":"confirmed","outcome":"bad","observed_at":"garbage"}}),
            json!({"op":"backlog.proposal.decision","version":2,"uid":"blg_cccccccccccccccccccccccccccccccc","payload":{"proposal_key":"prp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","occurrence_kind":"original","title":"bad","status":"accepted","accepted_at":"garbage","evidence":[]}}),
            json!({"op":"backlog.proposal.decision","version":2,"uid":"blg_dddddddddddddddddddddddddddddddd","payload":{"proposal_key":"prp_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","occurrence_kind":"original","title":"bad evidence time","status":"accepted","accepted_at":"2099-01-02 03:04:05","evidence":[{"source_kind":"trace","evidence_uid":"trc_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","evidence_fingerprint":"bad","observed_at":"garbage"}]}}),
            json!({"op":"legacy.evidence.capture","version":1,"uid":"leg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"source_kind":"trace","evidence_fingerprint":"bad","canonical_payload":"{}","captured_at":"garbage"}}),
            json!({"op":"audit.evidence.open","version":1,"uid":"aud_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"finding_key":"audit.bad","evidence_fingerprint":"bad","opened_at":"garbage"}}),
            json!({"op":"audit.evidence.clear","version":1,"uid":"aud_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","payload":{"cleared_at":"garbage"}}),
        ];
        for (index, operation) in operations.into_iter().enumerate() {
            let path = temp_dir.path().join(format!("invalid-{index}.jsonl"));
            fs::write(
                &path,
                format!(
                    "{}\n{}\n",
                    json!({"op":"changeset.header","version":1,"run_id":format!("run_invalid_{index}"),"base_schema_version":12}),
                    operation
                ),
            )
            .unwrap();
            let error = repository.apply_changeset(&path).unwrap_err();
            assert!(
                matches!(&error, HarnessInfraError::InvalidChangeset(message) if message.contains("YYYY-MM-DD HH:MM:SS")),
                "operation {index} failed outside timestamp validation: {error:?}"
            );
        }
    }

    #[test]
    fn legacy_proposal_reconciliation_is_conservative_replayable_and_idempotent() {
        let (_temp_dir, repository) = isolated_test_repository();
        let repository = repository.with_run_id("run_legacy_reconciliation");
        repository.init().unwrap();
        seed_legacy_reconciliation_fixture(&repository, false);

        let changeset = repository.changeset_path("run_legacy_reconciliation");
        let dry_run = repository.reconcile_legacy_improvements(false).unwrap();
        assert!(!dry_run.applied);
        assert_eq!(dry_run.changed, 2);
        assert_eq!(
            dry_run
                .records
                .iter()
                .map(|record| record.classification.as_str())
                .collect::<Vec<_>>(),
            vec!["derivable", "derivable", "manual"]
        );
        assert!(!changeset.exists());
        let connection = repository.open_existing().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM backlog WHERE proposal_key IS NOT NULL;",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        drop(connection);

        let applied = repository.reconcile_legacy_improvements(true).unwrap();
        assert!(applied.applied);
        assert_eq!(applied.changed, 2);
        assert!(applied.trace_id.is_some());
        let connection = repository.open_existing().unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM legacy_evidence_snapshot;",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            4
        );
        assert_eq!(
            connection.query_row("SELECT COUNT(*) FROM proposal_evidence_link WHERE source_kind='legacy_snapshot';", [], |row| row.get::<_, i64>(0)).unwrap(),
            4
        );
        assert_eq!(
            connection.query_row("SELECT COUNT(*) FROM backlog_outcome_observation WHERE status='legacy_recorded' AND outcome='Kept the original legacy result';", [], |row| row.get::<_, i64>(0)).unwrap(),
            1
        );
        let terminal: (String, String, String) = connection.query_row(
            "SELECT status, actual_outcome, implemented_at FROM backlog WHERE title='Address repeated intervention: correction: Use deterministic context scoring rules';",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(
            terminal,
            (
                "implemented".to_owned(),
                "Kept the original legacy result".to_owned(),
                "2026-01-02 00:00:00".to_owned()
            )
        );
        let trace_count: i64 = connection.query_row("SELECT COUNT(*) FROM trace WHERE agent='harness-cli' AND task_summary LIKE 'Reconciled % legacy improvement row(s)';", [], |row| row.get(0)).unwrap();
        assert_eq!(trace_count, 1);
        drop(connection);

        let no_op = repository.reconcile_legacy_improvements(true).unwrap();
        assert_eq!(no_op.changed, 0);
        assert!(no_op.trace_id.is_none());
        assert_eq!(
            repository.open_existing().unwrap().query_row("SELECT COUNT(*) FROM trace WHERE agent='harness-cli' AND task_summary LIKE 'Reconciled % legacy improvement row(s)';", [], |row| row.get::<_, i64>(0)).unwrap(),
            1
        );

        let replay_temp = tempfile::tempdir().unwrap();
        let replay_root = replay_temp.path().join("repo");
        fs::create_dir_all(&replay_root).unwrap();
        let replay = SqliteHarnessRepository::new(
            replay_root.clone(),
            replay_root.join("harness.db"),
            repository.schema_dir.clone(),
        );
        replay.init().unwrap();
        seed_legacy_reconciliation_fixture(&replay, true);
        let replay_result = replay.apply_changeset(&changeset).unwrap();
        assert!(replay_result.applied);
        let replay_connection = replay.open_existing().unwrap();
        assert_eq!(
            replay_connection
                .query_row(
                    "SELECT COUNT(*) FROM backlog WHERE proposal_key IS NOT NULL;",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            2
        );
        assert_eq!(
            replay_connection
                .query_row(
                    "SELECT COUNT(*) FROM legacy_evidence_snapshot;",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            4
        );
        assert_eq!(
            replay_connection.query_row("SELECT COUNT(*) FROM backlog_outcome_observation WHERE status='legacy_recorded';", [], |row| row.get::<_, i64>(0)).unwrap(),
            1
        );

        let fresh_temp = tempfile::tempdir().unwrap();
        let fresh_root = fresh_temp.path().join("repo");
        fs::create_dir_all(&fresh_root).unwrap();
        let fresh = SqliteHarnessRepository::new(
            fresh_root.clone(),
            fresh_root.join("harness.db"),
            repository.schema_dir.clone(),
        );
        fresh.init().unwrap();
        assert!(fresh.apply_changeset(&changeset).unwrap().applied);
        let fresh_connection = fresh.open_existing().unwrap();
        assert_eq!(
            fresh_connection
                .query_row(
                    "SELECT COUNT(*) FROM backlog WHERE proposal_key IS NOT NULL;",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            2
        );
        assert_eq!(
            fresh_connection
                .query_row(
                    "SELECT COUNT(*) FROM legacy_evidence_snapshot;",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            4
        );
    }

    #[test]
    fn logged_write_appends_header_and_semantic_operation() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let mut connection = repository.open_existing().unwrap();

        repository
            .with_logged_write_for_run(&mut connection, Some("run_test"), |transaction| {
                transaction
                    .execute(
                        "INSERT INTO intake (input_type, summary, risk_lane)
                         VALUES ('harness_improvement', 'Logged write test', 'normal');",
                        [],
                    )
                    .unwrap();
                let id = transaction.last_insert_rowid();
                Ok((
                    id,
                    vec![json!({
                        "op": "intake.add",
                        "version": 1,
                        "id": id,
                        "payload": {
                            "summary": "Logged write test",
                        },
                    })],
                ))
            })
            .unwrap();

        let changeset = fs::read_to_string(repository.changeset_path("run_test")).unwrap();
        let lines = changeset.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        let header: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["op"], "changeset.header");
        assert_eq!(header["run_id"], "run_test");
        assert_eq!(header["base_schema_version"], 14);
        let operation: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(operation["op"], "intake.add");
        assert_eq!(operation["payload"]["summary"], "Logged write test");

        let count = connection
            .query_row("SELECT COUNT(*) FROM intake;", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn automatic_source_changeset_captures_default_source_database_write() {
        let (_temp_dir, repository) = source_tracked_test_repository();
        repository.init().unwrap();

        automatic_capture_intake(&repository, "automatic source capture");

        let changeset_dir = repository.repo_root.join(".harness/changesets");
        let paths = fs::read_dir(&changeset_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 1);
        let content = fs::read_to_string(&paths[0]).unwrap();
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        let header: Value = serde_json::from_str(lines[0]).unwrap();
        let operation: Value = serde_json::from_str(lines[1]).unwrap();
        let run_id = header["run_id"].as_str().unwrap();
        assert!(run_id.starts_with("run_auto_"));
        assert_eq!(
            paths[0].file_name().unwrap().to_string_lossy(),
            format!("{run_id}.changeset.jsonl")
        );
        assert_eq!(operation["op"], "intake.add");
        assert_eq!(operation["payload"]["summary"], "automatic source capture");
    }

    #[test]
    fn automatic_source_changeset_uses_unique_file_per_repository_invocation() {
        let (_temp_dir, first) = source_tracked_test_repository();
        first.init().unwrap();
        automatic_capture_intake(&first, "first invocation");

        let second = SqliteHarnessRepository::new(
            first.repo_root.clone(),
            first.db_path.clone(),
            first.schema_dir.clone(),
        );
        automatic_capture_intake(&second, "second invocation");

        let paths = fs::read_dir(first.repo_root.join(".harness/changesets"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 2);
        assert_ne!(paths[0].file_name(), paths[1].file_name());
    }

    #[test]
    fn automatic_source_changeset_does_not_change_consumer_capture_default() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();

        automatic_capture_intake(&repository, "consumer-local write");

        assert!(!repository.repo_root.join(".harness/changesets").exists());
    }

    #[test]
    fn automatic_source_changeset_failed_write_leaves_no_file_or_row() {
        let (_temp_dir, repository) = source_tracked_test_repository();
        repository.init().unwrap();
        let mut connection = repository.open_existing().unwrap();

        let result: Result<()> = repository.with_logged_write(&mut connection, |transaction| {
            transaction.execute(
                "INSERT INTO intake (input_type, summary, risk_lane)
                 VALUES ('harness_improvement', 'automatic failed write', 'normal');",
                [],
            )?;
            Err(HarnessInfraError::StoryNotFound("US-NOPE".to_owned()))
        });

        assert!(result.is_err());
        assert!(!repository.repo_root.join(".harness/changesets").exists());
        let count = connection
            .query_row("SELECT COUNT(*) FROM intake;", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn story_dependency_command_validates_mutation_and_query_contract() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        for id in ["US-A", "US-B", "US-C"] {
            repository
                .add_story(StoryAddInput {
                    id: id.to_owned(),
                    title: id.to_owned(),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: None,
                    notes: None,
                })
                .unwrap();
        }

        assert!(repository
            .add_story_dependency(StoryDependencyInput {
                blocker: "US-A".to_owned(),
                blocked: "US-B".to_owned(),
            })
            .unwrap());
        assert!(!repository
            .add_story_dependency(StoryDependencyInput {
                blocker: "US-A".to_owned(),
                blocked: "US-B".to_owned(),
            })
            .unwrap());
        assert!(matches!(
            repository.add_story_dependency(StoryDependencyInput {
                blocker: "US-MISSING".to_owned(),
                blocked: "US-B".to_owned(),
            }),
            Err(HarnessInfraError::StoryNotFound(id)) if id == "US-MISSING"
        ));
        assert!(matches!(
            repository.add_story_dependency(StoryDependencyInput {
                blocker: "US-A".to_owned(),
                blocked: "US-A".to_owned(),
            }),
            Err(HarnessInfraError::StoryDependencySelf(id)) if id == "US-A"
        ));
        assert!(repository
            .add_story_dependency(StoryDependencyInput {
                blocker: "US-B".to_owned(),
                blocked: "US-C".to_owned(),
            })
            .unwrap());
        assert!(matches!(
            repository.add_story_dependency(StoryDependencyInput {
                blocker: "US-C".to_owned(),
                blocked: "US-A".to_owned(),
            }),
            Err(HarnessInfraError::StoryDependencyCycle(blocker, blocked))
                if blocker == "US-C" && blocked == "US-A"
        ));

        assert_eq!(
            repository.query_story_dependencies(None).unwrap(),
            vec![
                StoryDependencyRecord {
                    blocker: "US-A".to_owned(),
                    blocked: "US-B".to_owned(),
                },
                StoryDependencyRecord {
                    blocker: "US-B".to_owned(),
                    blocked: "US-C".to_owned(),
                },
            ]
        );
        assert!(repository
            .remove_story_dependency(StoryDependencyInput {
                blocker: "US-A".to_owned(),
                blocked: "US-B".to_owned(),
            })
            .unwrap());
        assert!(!repository
            .remove_story_dependency(StoryDependencyInput {
                blocker: "US-A".to_owned(),
                blocked: "US-B".to_owned(),
            })
            .unwrap());
    }

    #[test]
    fn story_dependency_changeset_replays_idempotently() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let changeset_path = temp_dir.path().join("dependency.changeset.jsonl");
        fs::write(
            &changeset_path,
            r#"{"op":"changeset.header","version":1,"run_id":"run_dependency","base_schema_version":8}
{"op":"story.add","version":1,"id":"US-A","payload":{"title":"A","risk_lane":"normal","contract_doc":null,"verify_command":null,"notes":null}}
{"op":"story.add","version":1,"id":"US-B","payload":{"title":"B","risk_lane":"normal","contract_doc":null,"verify_command":null,"notes":null}}
{"op":"story.dependency.add","version":1,"id":"US-A","payload":{"blocked":"US-B"}}
{"op":"story.dependency.remove","version":1,"id":"US-A","payload":{"blocked":"US-B"}}
{"op":"story.dependency.add","version":1,"id":"US-A","payload":{"blocked":"US-B"}}
"#,
        )
        .unwrap();

        assert!(repository.apply_changeset(&changeset_path).unwrap().applied);
        assert!(!repository.apply_changeset(&changeset_path).unwrap().applied);
        assert_eq!(
            repository.query_story_dependencies(None).unwrap(),
            vec![StoryDependencyRecord {
                blocker: "US-A".to_owned(),
                blocked: "US-B".to_owned(),
            }]
        );
    }

    #[test]
    fn story_backlog_relationship_validates_authority_and_replays() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        for id in ["US-A", "US-B", "US-C"] {
            repository
                .add_story(StoryAddInput {
                    id: id.to_owned(),
                    title: id.to_owned(),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: Some("true".to_owned()),
                    notes: None,
                })
                .unwrap();
        }
        let connection = repository.open_existing().unwrap();
        connection.execute("INSERT INTO backlog (uid, title, status) VALUES ('blg_11111111111111111111111111111111', 'Accepted', 'accepted'), ('blg_22222222222222222222222222222222', 'Open', 'proposed');", []).unwrap();
        let accepted_id: i64 = connection
            .query_row(
                "SELECT id FROM backlog WHERE uid='blg_11111111111111111111111111111111';",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let open_id: i64 = connection
            .query_row(
                "SELECT id FROM backlog WHERE uid='blg_22222222222222222222222222222222';",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(repository
            .link_story_backlog(StoryBacklogLinkInput {
                story_id: "US-A".to_owned(),
                backlog_id: accepted_id,
                relationship: "resolves".to_owned()
            })
            .unwrap());
        assert!(!repository
            .link_story_backlog(StoryBacklogLinkInput {
                story_id: "US-A".to_owned(),
                backlog_id: accepted_id,
                relationship: "resolves".to_owned()
            })
            .unwrap());
        assert!(
            matches!(repository.link_story_backlog(StoryBacklogLinkInput { story_id: "US-B".to_owned(), backlog_id: accepted_id, relationship: "resolves".to_owned() }), Err(HarnessInfraError::StoryBacklogResolverExists(id, story)) if id == accepted_id && story == "US-A")
        );
        assert!(repository
            .link_story_backlog(StoryBacklogLinkInput {
                story_id: "US-B".to_owned(),
                backlog_id: accepted_id,
                relationship: "references".to_owned()
            })
            .unwrap());
        assert!(
            matches!(repository.link_story_backlog(StoryBacklogLinkInput { story_id: "US-C".to_owned(), backlog_id: open_id, relationship: "resolves".to_owned() }), Err(HarnessInfraError::StoryBacklogResolverRequiresAccepted(id)) if id == open_id)
        );
        assert_eq!(
            repository
                .query_story_backlog_links(None, Some(accepted_id))
                .unwrap()
                .len(),
            2
        );
        assert!(repository
            .unlink_story_backlog("US-B", accepted_id)
            .unwrap());

        let changeset_path = temp_dir.path().join("relationship.changeset.jsonl");
        fs::write(&changeset_path, r#"{"op":"changeset.header","version":1,"run_id":"run_relationship","base_schema_version":10}
{"op":"story.backlog.link","version":1,"id":"US-B","payload":{"backlog_uid":"blg_11111111111111111111111111111111","relationship":"references"}}
"#).unwrap();
        assert!(repository.apply_changeset(&changeset_path).unwrap().applied);
        assert!(!repository.apply_changeset(&changeset_path).unwrap().applied);
        assert_eq!(
            repository
                .query_story_backlog_links(Some("US-B"), Some(accepted_id))
                .unwrap()[0]
                .relationship,
            "references"
        );
    }

    #[test]
    fn failed_logged_write_rolls_back_without_changeset() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let mut connection = repository.open_existing().unwrap();

        let result: Result<i64> = repository.with_logged_write_for_run(
            &mut connection,
            Some("run_fail"),
            |transaction| {
                transaction
                    .execute(
                        "INSERT INTO intake (input_type, summary, risk_lane)
                         VALUES ('harness_improvement', 'Failed write test', 'normal');",
                        [],
                    )
                    .unwrap();
                Err(HarnessInfraError::StoryNotFound("US-NOPE".to_owned()))
            },
        );

        assert!(result.is_err());
        assert!(!repository.changeset_path("run_fail").exists());
        let count = connection
            .query_row("SELECT COUNT(*) FROM intake;", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn apply_changeset_replays_operations_once() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let changeset_path = temp_dir.path().join("fixture.changeset.jsonl");
        fs::write(
            &changeset_path,
            r#"{"op":"changeset.header","version":1,"run_id":"run_apply","base_schema_version":6}
{"op":"intake.add","version":1,"id":10,"payload":{"input_type":"harness_improvement","summary":"Apply changeset intake","risk_lane":"normal","risk_flags":null,"affected_docs":null,"story_id":null,"notes":null}}
{"op":"story.add","version":1,"id":"US-APPLY","payload":{"title":"Apply changeset story","risk_lane":"normal","contract_doc":null,"verify_command":null,"notes":null}}
{"op":"story.update","version":1,"id":"US-APPLY","payload":{"contract_doc":"docs/product/replay.md","status":"implemented","evidence":"applied","unit_proof":1,"integration_proof":null,"e2e_proof":null,"platform_proof":null,"verify_command":null}}
"#,
        )
        .unwrap();

        let first = repository.apply_changeset(&changeset_path).unwrap();
        assert!(first.applied);
        assert_eq!(first.id, "run_apply");
        assert_eq!(first.operations, 3);
        let second = repository.apply_changeset(&changeset_path).unwrap();
        assert!(!second.applied);
        assert_eq!(second.operations, 0);

        let connection = repository.open_existing().unwrap();
        let (status, contract_doc) = connection
            .query_row(
                "SELECT status, contract_doc FROM story WHERE id='US-APPLY';",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap();
        assert_eq!(status, "implemented");
        assert_eq!(contract_doc, "docs/product/replay.md");
        let applied = connection
            .query_row(
                "SELECT COUNT(*) FROM changeset_applied WHERE id='run_apply';",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(applied, 1);
    }

    #[test]
    fn revision_guard_same_entity_conflicts_and_independent_entities_commute() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        for id in ["US-REV-A", "US-REV-B"] {
            repository
                .add_story(StoryAddInput {
                    id: id.to_owned(),
                    title: id.to_owned(),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: None,
                    notes: None,
                })
                .unwrap();
        }

        let first = temp_dir.path().join("revision-first.jsonl");
        fs::write(
            &first,
            r#"{"op":"changeset.header","version":1,"run_id":"run_revision_first","base_schema_version":14}
{"op":"story.update","version":3,"id":"US-REV-A","expected_revision":0,"payload":{"status":"in_progress"}}
"#,
        )
        .unwrap();
        let independent = temp_dir.path().join("revision-independent.jsonl");
        fs::write(
            &independent,
            r#"{"op":"changeset.header","version":1,"run_id":"run_revision_independent","base_schema_version":14}
{"op":"story.update","version":3,"id":"US-REV-B","expected_revision":0,"payload":{"status":"changed"}}
"#,
        )
        .unwrap();

        assert!(repository.apply_changeset(&first).unwrap().applied);
        assert!(repository.apply_changeset(&independent).unwrap().applied);

        let stale = temp_dir.path().join("revision-stale.jsonl");
        fs::write(
            &stale,
            r#"{"op":"changeset.header","version":1,"run_id":"run_revision_stale","base_schema_version":14}
{"op":"story.update","version":3,"id":"US-REV-B","expected_revision":1,"payload":{"status":"retired"}}
{"op":"story.update","version":3,"id":"US-REV-A","expected_revision":0,"payload":{"status":"changed"}}
"#,
        )
        .unwrap();
        let error = repository.apply_changeset(&stale).unwrap_err();
        assert!(matches!(
            error,
            HarnessInfraError::EntityRevisionConflict {
                changeset_id,
                entity_kind,
                entity_id,
                expected_revision: 0,
                actual_revision: Some(1),
            } if changeset_id == "run_revision_stale"
                && entity_kind == "story"
                && entity_id == "US-REV-A"
        ));

        let connection = repository.open_existing().unwrap();
        let story_a: (String, i64) = connection
            .query_row(
                "SELECT status, revision FROM story WHERE id='US-REV-A';",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let story_b: (String, i64) = connection
            .query_row(
                "SELECT status, revision FROM story WHERE id='US-REV-B';",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(story_a, ("in_progress".to_owned(), 1));
        assert_eq!(story_b, ("changed".to_owned(), 1));
        drop(connection);

        let legacy = temp_dir.path().join("revision-legacy.jsonl");
        fs::write(
            &legacy,
            r#"{"op":"changeset.header","version":1,"run_id":"run_revision_legacy","base_schema_version":13}
{"op":"story.update","version":1,"id":"US-REV-A","payload":{"evidence":"legacy operation remains replayable"}}
"#,
        )
        .unwrap();
        assert!(repository.apply_changeset(&legacy).unwrap().applied);
        assert_eq!(
            repository
                .open_existing()
                .unwrap()
                .query_row(
                    "SELECT revision FROM story WHERE id='US-REV-A';",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2
        );
    }

    #[test]
    fn replay_preserves_generated_timestamps_for_new_core_state() {
        let (temp_dir, repository) = isolated_test_repository();
        let repository = repository.with_run_id("run_timestamp_parity");
        repository.init().unwrap();
        for id in ["US-TIME-A", "US-TIME-B"] {
            repository
                .add_story(StoryAddInput {
                    id: id.to_owned(),
                    title: id.to_owned(),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: None,
                    notes: None,
                })
                .unwrap();
        }
        repository
            .add_story_dependency(StoryDependencyInput {
                blocker: "US-TIME-A".to_owned(),
                blocked: "US-TIME-B".to_owned(),
            })
            .unwrap();
        repository
            .add_story_hierarchy(StoryHierarchyInput {
                parent: "US-TIME-A".to_owned(),
                child: "US-TIME-B".to_owned(),
            })
            .unwrap();
        repository
            .add_decision(DecisionAddInput {
                id: "D-TIME".to_owned(),
                title: "Timestamp parity".to_owned(),
                status: "accepted".to_owned(),
                doc_path: None,
                verify_command: Some(passing_command().to_owned()),
                predicted_impact: None,
                notes: None,
            })
            .unwrap();
        repository.verify_decision("D-TIME").unwrap();

        let changeset = repository
            .repo_root
            .join(".harness/changesets/run_timestamp_parity.changeset.jsonl");
        let contents = fs::read_to_string(&changeset).unwrap();
        assert!(contents.matches("\"created_at\"").count() >= 5);
        assert!(contents.contains("\"verified_at\""));

        let target_root = temp_dir.path().join("target");
        fs::create_dir_all(&target_root).unwrap();
        let target = SqliteHarnessRepository::new(
            target_root.clone(),
            target_root.join("harness.db"),
            repository.schema_dir.clone(),
        );
        target.init().unwrap();
        target.apply_changeset(&changeset).unwrap();

        let source = repository.open_existing().unwrap();
        let replay = target.open_existing().unwrap();
        for sql in [
            "SELECT created_at FROM story WHERE id='US-TIME-A'",
            "SELECT created_at FROM story_dependency WHERE story_id='US-TIME-A' AND blocks_story_id='US-TIME-B'",
            "SELECT created_at FROM story_hierarchy WHERE parent_story_id='US-TIME-A' AND child_story_id='US-TIME-B'",
            "SELECT created_at FROM decision WHERE id='D-TIME'",
            "SELECT last_verified_at FROM decision WHERE id='D-TIME'",
        ] {
            let expected: String = source.query_row(sql, [], |row| row.get(0)).unwrap();
            let actual: String = replay.query_row(sql, [], |row| row.get(0)).unwrap();
            assert_eq!(actual, expected, "timestamp differs for {sql}");
        }
    }

    #[test]
    fn revision_guard_covers_each_mutable_entity_family() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let connection = repository.open_existing().unwrap();
        connection
            .execute(
                "INSERT INTO decision (id, title) VALUES ('D-REV', 'revision decision');",
                [],
            )
            .unwrap();
        connection.execute("INSERT INTO backlog (uid, title) VALUES ('blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'revision backlog');", []).unwrap();
        connection.execute("INSERT INTO tool (name, command, description, responsibility) VALUES ('revision-tool', 'skill:revision', 'revision guard fixture description', 'Verification');", []).unwrap();
        connection.execute("INSERT INTO audit_evidence_episode (uid, finding_key, evidence_fingerprint, opened_at) VALUES ('aud_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'audit.revision', 'fingerprint', '2026-01-01 00:00:00');", []).unwrap();
        drop(connection);

        let success = temp_dir.path().join("revision-families.jsonl");
        fs::write(
            &success,
            r#"{"op":"changeset.header","version":1,"run_id":"run_revision_families","base_schema_version":14}
{"op":"decision.verify","version":3,"id":"D-REV","expected_revision":0,"payload":{"result":"pass","verified_at":"2026-01-02 00:00:00"}}
{"op":"backlog.close","version":3,"uid":"blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expected_revision":0,"payload":{"status":"implemented","actual_outcome":"done","implemented_at":"2026-01-02 00:00:00"}}
{"op":"tool.check","version":3,"id":"revision-tool","expected_revision":0,"payload":{"status":"present","detail":"fixture","checked_at":"2026-01-02 00:00:00"}}
{"op":"audit.evidence.clear","version":3,"uid":"aud_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expected_revision":0,"payload":{"cleared_at":"2026-01-02 00:00:00"}}
"#,
        )
        .unwrap();
        assert!(repository.apply_changeset(&success).unwrap().applied);

        let connection = repository.open_existing().unwrap();
        for (table, key_column, key) in [
            ("decision", "id", "D-REV"),
            ("backlog", "uid", "blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ("tool", "name", "revision-tool"),
            (
                "audit_evidence_episode",
                "uid",
                "aud_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
        ] {
            let revision: i64 = connection
                .query_row(
                    &format!("SELECT revision FROM {table} WHERE {key_column}=?1"),
                    params![key],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(revision, 1, "{table} revision did not advance once");
        }
        drop(connection);

        let stale_cases = [
            (
                "decision",
                r#"{"op":"decision.verify","version":3,"id":"D-REV","expected_revision":0,"payload":{"result":"fail"}}"#,
            ),
            (
                "backlog",
                r#"{"op":"backlog.close","version":3,"uid":"blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expected_revision":0,"payload":{"status":"rejected"}}"#,
            ),
            (
                "tool",
                r#"{"op":"tool.check","version":3,"id":"revision-tool","expected_revision":0,"payload":{"status":"missing","detail":"stale"}}"#,
            ),
            (
                "audit_evidence_episode",
                r#"{"op":"audit.evidence.clear","version":3,"uid":"aud_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expected_revision":0,"payload":{"cleared_at":"2026-01-03 00:00:00"}}"#,
            ),
        ];
        for (index, (entity_kind, operation)) in stale_cases.into_iter().enumerate() {
            let path = temp_dir
                .path()
                .join(format!("revision-family-stale-{index}.jsonl"));
            fs::write(
                &path,
                format!(
                    "{{\"op\":\"changeset.header\",\"version\":1,\"run_id\":\"run_revision_family_stale_{index}\",\"base_schema_version\":14}}\n{operation}\n"
                ),
            )
            .unwrap();
            let error = repository.apply_changeset(&path).unwrap_err();
            assert!(matches!(
                error,
                HarnessInfraError::EntityRevisionConflict {
                    entity_kind: actual,
                    expected_revision: 0,
                    actual_revision: Some(1),
                    ..
                } if actual == entity_kind
            ));
        }
    }

    #[test]
    fn apply_changeset_accepts_already_materialized_stable_records() {
        let (_temp_dir, mut repository) = isolated_test_repository();
        repository.init().unwrap();
        repository.run_id_override = Some("run_materialized".to_owned());
        let intake_id = repository
            .record_intake(IntakeInput {
                input_type: InputType::HarnessImprovement,
                summary: "Materialized intake replay".to_owned(),
                risk_lane: RiskLane::HighRisk,
                risk_flags: CsvList::from_optional(None),
                affected_docs: CsvList::from_optional(None),
                story_id: None,
                notes: None,
            })
            .unwrap();
        repository
            .record_trace(TraceInput {
                task_summary: "Materialized trace replay".to_owned(),
                intake_id: Some(intake_id),
                story_id: None,
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: Some("none".to_owned()),
                notes: None,
                actions: CsvList::from_optional(Some("implemented".to_owned())),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(Some("src.rs".to_owned())),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
        let changeset_path = repository.changeset_path("run_materialized");
        repository.run_id_override = None;

        let applied = repository.apply_changeset(&changeset_path).unwrap();

        assert!(applied.applied);
        assert_eq!(applied.operations, 2);
        let connection = repository.open_existing().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM intake;", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM trace;", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn apply_changeset_migrates_existing_database_before_idempotency_check() {
        let (temp_dir, repository) = isolated_test_repository();
        let connection = repository.open_or_create().unwrap();
        repository.apply_schema_v1(&connection).unwrap();
        for file in [
            "002-story-verify.sql",
            "003-tool-registry.sql",
            "004-intervention.sql",
            "005-tool-extensions.sql",
        ] {
            let sql = fs::read_to_string(repository.schema_dir.join(file)).unwrap();
            connection.execute_batch(&sql).unwrap();
        }
        assert_eq!(
            SqliteHarnessRepository::schema_version(&connection).unwrap(),
            5
        );
        drop(connection);

        let changeset_path = temp_dir.path().join("fixture.changeset.jsonl");
        fs::write(
            &changeset_path,
            r#"{"op":"changeset.header","version":1,"run_id":"run_migrated_apply","base_schema_version":6}
{"op":"story.add","version":1,"id":"US-MIGRATED-APPLY","payload":{"title":"Migrated apply story","risk_lane":"normal","contract_doc":null,"verify_command":null,"notes":null}}
"#,
        )
        .unwrap();

        let result = repository.apply_changeset(&changeset_path).unwrap();

        assert!(result.applied);
        assert_eq!(result.operations, 1);
        let connection = repository.open_existing().unwrap();
        assert_eq!(
            SqliteHarnessRepository::schema_version(&connection).unwrap(),
            14
        );
        let applied = connection
            .query_row(
                "SELECT COUNT(*) FROM changeset_applied WHERE id='run_migrated_apply';",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(applied, 1);
    }

    #[test]
    fn apply_changesets_remaps_local_numeric_ids() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();

        for (run_id, summary) in [
            ("run_worktree_a", "First worktree trace"),
            ("run_worktree_b", "Second worktree trace"),
        ] {
            fs::write(
                temp_dir.path().join(format!("{run_id}.changeset.jsonl")),
                format!(
                    r#"{{"op":"changeset.header","version":1,"run_id":"{run_id}","base_schema_version":8}}
{{"op":"intake.add","version":1,"id":1,"payload":{{"input_type":"change_request","summary":"{summary} intake","risk_lane":"normal","risk_flags":null,"affected_docs":null,"story_id":null,"notes":null}}}}
{{"op":"trace.add","version":1,"id":1,"payload":{{"task_summary":"{summary}","intake_id":1,"story_id":null,"agent":"Codex","actions_taken":null,"files_read":null,"files_changed":null,"decisions_made":null,"errors":null,"outcome":"completed","duration_seconds":null,"token_estimate":null,"harness_friction":null,"notes":null}}}}
"#
                ),
            )
            .unwrap();

            let result = repository
                .apply_changeset(&temp_dir.path().join(format!("{run_id}.changeset.jsonl")))
                .unwrap();
            assert!(result.applied);
            assert_eq!(result.operations, 2);
        }

        let connection = repository.open_existing().unwrap();
        let counts = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM intake),
                    (SELECT COUNT(*) FROM trace),
                    (SELECT COUNT(DISTINCT intake_id) FROM trace)
                 ;",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(counts, (2, 2, 2));
    }

    #[test]
    fn rebuild_db_creates_fresh_database_from_changesets() {
        let (temp_dir, repository) = isolated_test_repository();
        let changeset_dir = temp_dir.path().join("changesets");
        fs::create_dir_all(&changeset_dir).unwrap();
        fs::write(
            changeset_dir.join("001.changeset.jsonl"),
            r#"{"op":"changeset.header","version":1,"run_id":"run_rebuild","base_schema_version":6}
{"op":"story.add","version":1,"id":"US-REBUILD","payload":{"title":"Rebuild story","risk_lane":"normal","contract_doc":null,"verify_command":null,"notes":null}}
{"op":"story.update","version":1,"id":"US-REBUILD","payload":{"status":"implemented","evidence":"rebuilt","unit_proof":1,"integration_proof":1,"e2e_proof":null,"platform_proof":null,"verify_command":null}}
"#,
        )
        .unwrap();

        let result = repository.rebuild_db(&changeset_dir).unwrap();
        assert_eq!(result.changesets, 1);
        assert_eq!(result.operations, 2);

        let connection = repository.open_existing().unwrap();
        let evidence = connection
            .query_row(
                "SELECT evidence FROM story WHERE id='US-REBUILD';",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(evidence, "rebuilt");
    }

    #[test]
    fn rebuild_db_refuses_existing_database() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let result = repository.rebuild_db(temp_dir.path());

        assert!(matches!(
            result,
            Err(HarnessInfraError::RebuildDatabaseExists(_))
        ));
    }

    #[test]
    fn migrate_applies_story_verify_columns_to_existing_database() {
        let (_temp_dir, repository) = test_repository();
        let connection = repository.open_or_create().unwrap();
        repository.apply_schema_v1(&connection).unwrap();
        drop(connection);

        let result = repository.migrate().unwrap();

        assert_eq!(result.current_version, 1);
        assert_eq!(
            result.applied,
            vec![2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        );
        let connection = repository.open_existing().unwrap();
        assert_eq!(
            SqliteHarnessRepository::schema_version(&connection).unwrap(),
            14
        );
        let story_columns = story_columns(&connection);
        assert!(story_columns.contains(&"verify_command".to_owned()));
        assert!(story_columns.contains(&"last_verified_at".to_owned()));
        assert!(story_columns.contains(&"last_verified_result".to_owned()));
    }

    #[test]
    fn migration_005_backfills_kind_from_command_prefix() {
        let (_temp_dir, repository) = test_repository();
        let schema_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .join("scripts/schema");

        // Build a pre-kind (v4) database: v1 base plus migrations 002-004 only.
        let connection = repository.open_or_create().unwrap();
        repository.apply_schema_v1(&connection).unwrap();
        for file in [
            "002-story-verify.sql",
            "003-tool-registry.sql",
            "004-intervention.sql",
        ] {
            let sql = std::fs::read_to_string(schema_dir.join(file)).unwrap();
            connection.execute_batch(&sql).unwrap();
        }
        assert_eq!(
            SqliteHarnessRepository::schema_version(&connection).unwrap(),
            4
        );

        // Insert tools the old way (no kind column existed yet).
        for (name, command) in [
            ("mcp-example", "mcp:example-server"),
            ("skill-example", "skill:example-skill"),
            ("cli-example", "./deploy.sh"),
        ] {
            connection
                .execute(
                    "INSERT INTO tool (name, command, description, responsibility)
                     VALUES (?1, ?2, 'pre-kind registered tool example', 'Verification');",
                    params![name, command],
                )
                .unwrap();
        }
        drop(connection);

        // Upgrade: migration 005 must infer kind from the command prefix.
        assert_eq!(
            repository.migrate().unwrap().applied,
            vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        );
        let connection = repository.open_existing().unwrap();
        let kind_of = |name: &str| -> String {
            connection
                .query_row(
                    "SELECT kind FROM tool WHERE name=?1;",
                    params![name],
                    |row| row.get::<_, String>(0),
                )
                .unwrap()
        };
        assert_eq!(kind_of("mcp-example"), "mcp");
        assert_eq!(kind_of("skill-example"), "skill");
        assert_eq!(kind_of("cli-example"), "cli");
    }

    #[test]
    fn records_and_queries_intake() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();

        let id = repository
            .record_intake(IntakeInput {
                input_type: InputType::HarnessImprovement,
                summary: "Port one CLI slice".to_owned(),
                risk_lane: RiskLane::HighRisk,
                risk_flags: CsvList::from_optional(Some("public contracts".to_owned())),
                affected_docs: CsvList::from_optional(None),
                story_id: Some("US-002".to_owned()),
                notes: None,
            })
            .unwrap();

        let intakes = repository.query_intakes().unwrap();
        assert_eq!(id, 1);
        assert_eq!(intakes[0].summary, "Port one CLI slice");
        assert_eq!(intakes[0].input_type, "harness_improvement");
        assert_eq!(intakes[0].risk_lane, "high_risk");

        let connection = repository.open_existing().unwrap();
        let missing_lists_are_null: (bool, bool) = connection
            .query_row(
                "SELECT risk_flags IS NULL, affected_docs IS NULL FROM intake WHERE id=?1;",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(missing_lists_are_null, (false, true));
    }

    #[test]
    fn decision_verify_runs_from_repo_root() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        fs::create_dir_all(&repo_root).unwrap();
        let schema_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf()
            .join("scripts/schema");
        let repository = SqliteHarnessRepository::new(
            repo_root.clone(),
            temp_dir.path().join("harness.db"),
            schema_root,
        );
        repository.init().unwrap();

        let pwd_output = repo_root.join("verify-pwd.txt");
        let verify_command = if cfg!(windows) {
            "cd > verify-pwd.txt".to_owned()
        } else {
            "pwd > verify-pwd.txt".to_owned()
        };
        repository
            .add_decision(DecisionAddInput {
                id: "0001-test".to_owned(),
                title: "Verify from root".to_owned(),
                status: "accepted".to_owned(),
                doc_path: None,
                verify_command: Some(verify_command),
                predicted_impact: None,
                notes: None,
            })
            .unwrap();

        let result = repository.verify_decision("0001-test").unwrap();

        assert_eq!(result.result, "pass");
        assert_eq!(
            fs::canonicalize(fs::read_to_string(pwd_output).unwrap().trim()).unwrap(),
            fs::canonicalize(repo_root).unwrap()
        );
    }

    #[test]
    fn matrix_runnable_projection_matches_protocol_story_discovery() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();

        for (id, verify_command) in [
            ("US-BLOCKER", Some("true")),
            ("US-BLOCKED", Some("true")),
            ("US-READY", Some("true")),
            ("US-NO-PROOF", None),
        ] {
            repository
                .add_story(StoryAddInput {
                    id: id.to_owned(),
                    title: format!("{id} title"),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: verify_command.map(str::to_owned),
                    notes: None,
                })
                .unwrap();
        }
        repository
            .add_story_dependency(StoryDependencyInput {
                blocker: "US-BLOCKER".to_owned(),
                blocked: "US-BLOCKED".to_owned(),
            })
            .unwrap();

        let protocol_stories = repository.query_orchestration_stories().unwrap();
        let matrix = repository.query_matrix().unwrap();
        assert_eq!(matrix.len(), protocol_stories.len());
        for matrix_story in matrix {
            let protocol_story = protocol_stories
                .iter()
                .find(|story| story.id == matrix_story.id)
                .unwrap();
            assert_eq!(
                matrix_story.runnable, protocol_story.runnable,
                "runnable mismatch for {}",
                matrix_story.id
            );
        }
    }

    #[test]
    fn story_add_update_and_verify_status_store_verify_command() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();

        repository
            .add_story(StoryAddInput {
                id: "US-VERIFY".to_owned(),
                title: "Verify command story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some("echo ok".to_owned()),
                notes: None,
            })
            .unwrap();
        assert_eq!(
            repository
                .story_verify_status("US-VERIFY")
                .unwrap()
                .verify_command
                .as_deref(),
            Some("echo ok")
        );

        repository
            .update_story(StoryUpdateInput {
                id: "US-VERIFY".to_owned(),
                contract_doc: Some("docs/product/updated.md".to_owned()),
                status: None,
                evidence: None,
                unit: None,
                integration: None,
                e2e: None,
                platform: None,
                verify_command: Some("npm test".to_owned()),
            })
            .unwrap();

        assert_eq!(
            repository
                .story_verify_status("US-VERIFY")
                .unwrap()
                .verify_command
                .as_deref(),
            Some("npm test")
        );
        let connection = repository.open_existing().unwrap();
        let contract_doc = connection
            .query_row(
                "SELECT contract_doc FROM story WHERE id='US-VERIFY';",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(contract_doc, "docs/product/updated.md");
    }

    #[test]
    fn story_update_rejects_implemented_and_preserves_the_existing_story() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-GATE-TEXT".to_owned(),
                title: "Text completion gate".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some(passing_command().to_owned()),
                notes: None,
            })
            .unwrap();

        let error = repository
            .update_story(StoryUpdateInput {
                id: "US-GATE-TEXT".to_owned(),
                contract_doc: None,
                status: Some("implemented".to_owned()),
                evidence: Some("unverified completion".to_owned()),
                unit: Some(BoolFlag(1)),
                integration: None,
                e2e: None,
                platform: None,
                verify_command: None,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            HarnessInfraError::StoryImplementedRequiresCompletion(id) if id == "US-GATE-TEXT"
        ));

        let connection = repository.open_existing().unwrap();
        let unchanged: (String, Option<String>, i64) = connection
            .query_row(
                "SELECT status, evidence, unit_proof FROM story WHERE id='US-GATE-TEXT';",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(unchanged, ("planned".to_owned(), None, 0));
        drop(connection);

        repository
            .update_story(StoryUpdateInput {
                id: "US-GATE-TEXT".to_owned(),
                contract_doc: None,
                status: Some("in_progress".to_owned()),
                evidence: Some("work started".to_owned()),
                unit: Some(BoolFlag(1)),
                integration: None,
                e2e: None,
                platform: None,
                verify_command: None,
            })
            .unwrap();
        let connection = repository.open_existing().unwrap();
        let updated: (String, Option<String>, i64) = connection
            .query_row(
                "SELECT status, evidence, unit_proof FROM story WHERE id='US-GATE-TEXT';",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            updated,
            ("in_progress".to_owned(), Some("work started".to_owned()), 1)
        );
    }

    #[test]
    fn story_update_cas_rejects_implemented_without_a_write_or_operation_log() {
        let (_temp_dir, repository) = isolated_test_repository();
        let repository = repository.with_run_id("run_completion_gate");
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-GATE-CAS".to_owned(),
                title: "CAS completion gate".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some(passing_command().to_owned()),
                notes: None,
            })
            .unwrap();
        let changeset = repository.changeset_path("run_completion_gate");
        let before = fs::read_to_string(&changeset).unwrap();

        let error = repository
            .update_story_cas(StoryCasUpdateInput {
                id: "US-GATE-CAS".to_owned(),
                status: "implemented".to_owned(),
                expected_status: "planned".to_owned(),
                require_runnable: true,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            HarnessInfraError::StoryImplementedRequiresCompletion(id) if id == "US-GATE-CAS"
        ));
        assert_eq!(fs::read_to_string(&changeset).unwrap(), before);
        assert_eq!(
            repository
                .query_orchestration_stories()
                .unwrap()
                .into_iter()
                .find(|story| story.id == "US-GATE-CAS")
                .unwrap()
                .status,
            "planned"
        );

        let changed = repository
            .update_story_cas(StoryCasUpdateInput {
                id: "US-GATE-CAS".to_owned(),
                status: "in_progress".to_owned(),
                expected_status: "planned".to_owned(),
                require_runnable: true,
            })
            .unwrap();
        assert_eq!(changed.before_status, "planned");
        assert_eq!(changed.after_status, "in_progress");
        assert!(changed.runnable_before);
        assert!(fs::read_to_string(changeset)
            .unwrap()
            .contains("\"status\":\"in_progress\""));
    }

    #[test]
    fn story_completion_rejects_ineligible_story_states_before_verification() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-PLANNED".to_owned(),
                title: "Planned".to_owned(),
                risk_lane: RiskLane::HighRisk,
                contract_doc: None,
                verify_command: Some(passing_command().to_owned()),
                notes: None,
            })
            .unwrap();
        assert!(matches!(
            repository.complete_story("US-PLANNED"),
            Err(HarnessInfraError::StoryCompletion(message)) if message.contains("planned")
        ));

        add_completion_story(&repository, "US-RETIRED", Some(passing_command()));
        repository
            .update_story(StoryUpdateInput {
                id: "US-RETIRED".to_owned(),
                contract_doc: None,
                status: Some("retired".to_owned()),
                evidence: None,
                unit: None,
                integration: None,
                e2e: None,
                platform: None,
                verify_command: None,
            })
            .unwrap();
        assert!(matches!(
            repository.complete_story("US-RETIRED"),
            Err(HarnessInfraError::StoryCompletion(message)) if message.contains("retired")
        ));

        add_completion_story(&repository, "US-NO-VERIFY", None);
        assert!(matches!(
            repository.complete_story("US-NO-VERIFY"),
            Err(HarnessInfraError::MissingStoryVerifyCommand(id)) if id == "US-NO-VERIFY"
        ));
        assert!(matches!(
            repository.complete_story("US-MISSING"),
            Err(HarnessInfraError::StoryNotFound(id)) if id == "US-MISSING"
        ));
    }

    #[test]
    fn story_completion_requires_stable_intake_and_post_link_trace() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        add_completion_story(&repository, "US-COMPLETE", Some(passing_command()));
        let connection = repository.open_existing().unwrap();
        connection.execute("INSERT INTO backlog (uid, title, status) VALUES ('blg_33333333333333333333333333333333', 'resolver', 'accepted');", []).unwrap();
        connection.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at) VALUES ('US-COMPLETE', 'blg_33333333333333333333333333333333', 'resolves', '2026-01-02 00:00:00');", []).unwrap();

        assert!(
            matches!(repository.complete_story("US-COMPLETE"), Err(HarnessInfraError::StoryCompletion(message)) if message.contains("linked harness_improvement intake"))
        );

        connection.execute("INSERT INTO intake (uid, created_at, input_type, summary, risk_lane, story_id) VALUES ('ink_11111111111111111111111111111111', '2026-01-01 00:00:00', 'harness_improvement', 'fixture', 'high_risk', 'US-COMPLETE');", []).unwrap();
        connection.execute("INSERT INTO trace (created_at, intake_id, task_summary, story_id, actions_taken, files_changed, outcome) VALUES ('2026-01-03 00:00:00', 1, 'local id only', 'US-COMPLETE', '[\"work\"]', '[\"src.rs\"]', 'completed');", []).unwrap();
        connection.execute("INSERT INTO trace (uid, created_at, intake_uid, task_summary, story_id, actions_taken, files_changed, outcome) VALUES ('trc_22222222222222222222222222222222', '2026-01-01 00:00:00', 'ink_11111111111111111111111111111111', 'too early', 'US-COMPLETE', '[\"work\"]', '[\"src.rs\"]', 'completed');", []).unwrap();
        connection.execute("INSERT INTO trace (uid, created_at, intake_uid, task_summary, story_id, actions_taken, files_changed, outcome) VALUES ('trc_33333333333333333333333333333333', '2026-01-03 00:00:00', 'ink_11111111111111111111111111111111', 'incomplete trace', 'US-COMPLETE', '[]', '[\"src.rs\"]', 'completed');", []).unwrap();
        connection.execute("INSERT INTO trace (uid, created_at, intake_uid, task_summary, story_id, actions_taken, files_changed, outcome) VALUES ('trc_66666666666666666666666666666666', '2026-01-03 00:00:00', 'ink_11111111111111111111111111111111', 'failed trace', 'US-COMPLETE', '[\"work\"]', '[\"src.rs\"]', 'failed');", []).unwrap();
        assert!(
            matches!(repository.complete_story("US-COMPLETE"), Err(HarnessInfraError::StoryCompletion(message)) if message.contains("after the newest resolver link"))
        );

        connection.execute("INSERT INTO trace (uid, created_at, intake_uid, task_summary, story_id, actions_taken, files_changed, outcome) VALUES ('trc_44444444444444444444444444444444', '2026-01-03 00:00:00', 'ink_11111111111111111111111111111111', 'qualifying trace', 'US-COMPLETE', '[\"work\"]', '[\"src.rs\"]', 'completed');", []).unwrap();
        connection.execute("INSERT INTO intake (uid, created_at, input_type, summary, risk_lane, story_id) VALUES ('ink_55555555555555555555555555555555', '2026-01-04 00:00:00', 'harness_improvement', 'newer intake without trace', 'high_risk', 'US-COMPLETE');", []).unwrap();

        let result = repository.complete_story("US-COMPLETE").unwrap();
        assert_eq!(result.result, "pass");
        assert_eq!(
            result.intake_uid.as_deref(),
            Some("ink_11111111111111111111111111111111")
        );
        assert_eq!(
            result.implementation_trace_uid.as_deref(),
            Some("trc_44444444444444444444444444444444")
        );
    }

    #[test]
    fn story_completion_atomically_closes_resolvers_and_replays_exact_evidence() {
        let (temp_dir, mut repository) = isolated_test_repository();
        repository.init().unwrap();
        let (first_id, intake_uid, trace_uid) =
            seed_resolver_completion_fixture(&repository, "US-COMPLETE", passing_command());
        let connection = repository.open_existing().unwrap();
        connection.execute("INSERT INTO backlog (uid, title, status, outcome_schedule_kind) VALUES ('blg_44444444444444444444444444444444', 'second resolver', 'accepted', 'manual');", []).unwrap();
        let second_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at) VALUES ('US-COMPLETE', 'blg_44444444444444444444444444444444', 'resolves', '2026-01-02 00:00:00');", []).unwrap();
        connection.execute("INSERT INTO backlog (uid, title, status, resolution_evidence) VALUES ('blg_55555555555555555555555555555555', 'self closed', 'implemented', '{\"story_id\":\"US-COMPLETE\",\"result\":\"pass\"}');", []).unwrap();
        let self_closed_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at) VALUES ('US-COMPLETE', 'blg_55555555555555555555555555555555', 'resolves', '2026-01-02 00:00:00');", []).unwrap();
        connection.execute("INSERT INTO backlog (uid, title, status) VALUES ('blg_66666666666666666666666666666666', 'reference only', 'proposed');", []).unwrap();
        let reference_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at) VALUES ('US-COMPLETE', 'blg_66666666666666666666666666666666', 'references', '2026-01-02 00:00:00');", []).unwrap();

        repository.run_id_override = Some("run_completion_replay".to_owned());
        let result = repository.complete_story("US-COMPLETE").unwrap();
        let changeset_path = repository.changeset_path("run_completion_replay");
        let first_changeset_len = fs::metadata(&changeset_path).unwrap().len();
        let repeated = repository.complete_story("US-COMPLETE").unwrap();
        assert_eq!(
            fs::metadata(&changeset_path).unwrap().len(),
            first_changeset_len
        );

        assert_eq!(result.result, "pass");
        assert_eq!(result.intake_uid.as_deref(), Some(intake_uid.as_str()));
        assert_eq!(
            result.implementation_trace_uid.as_deref(),
            Some(trace_uid.as_str())
        );
        assert_eq!(result.closed_backlog_ids, vec![first_id, second_id]);
        assert_eq!(result.already_closed_backlog_ids, vec![self_closed_id]);
        assert_eq!(result.referenced_backlog_ids, vec![reference_id]);
        assert_eq!(repeated.result, "already-completed");
        assert_eq!(
            repeated.already_closed_backlog_ids,
            vec![first_id, second_id, self_closed_id]
        );

        let live: (String, Option<i64>, Option<String>, String) = connection.query_row("SELECT resolution_evidence, outcome_baseline_trace_count, actual_outcome, closed_at FROM backlog WHERE id=?1;", params![first_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap();
        let live_verified_at: String = connection
            .query_row(
                "SELECT last_verified_at FROM story WHERE id='US-COMPLETE'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(live.0.contains("completion_uid"));
        assert!(live.0.contains("completed_at"));
        assert!(live.0.contains("verify_command"));
        assert_eq!(live.1, Some(1));
        assert_eq!(live.2, None);
        let reference_status: String = connection
            .query_row(
                "SELECT status FROM backlog WHERE id=?1;",
                params![reference_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reference_status, "proposed");

        repository.run_id_override = None;
        repository
            .update_story(StoryUpdateInput {
                id: "US-COMPLETE".to_owned(),
                contract_doc: None,
                status: None,
                evidence: None,
                unit: None,
                integration: None,
                e2e: None,
                platform: None,
                verify_command: Some(failing_command().to_owned()),
            })
            .unwrap();
        assert_eq!(
            repository.verify_story("US-COMPLETE").unwrap().result,
            "fail"
        );
        let post_failure: (String, String) = connection
            .query_row(
                "SELECT story.status, backlog.resolution_evidence FROM story JOIN story_backlog_link AS link ON link.story_id=story.id JOIN backlog ON backlog.uid=link.backlog_uid WHERE story.id='US-COMPLETE' AND backlog.id=?1;",
                params![first_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(post_failure, ("implemented".to_owned(), live.0.clone()));

        let changeset = fs::read_to_string(&changeset_path).unwrap();
        assert!(changeset.contains("\"op\":\"story.complete\""));
        assert_eq!(changeset.matches("\"op\":\"backlog.complete\"").count(), 2);
        assert!(changeset.contains("resolution_evidence"));

        let replay_root = temp_dir.path().join("replay");
        fs::create_dir_all(&replay_root).unwrap();
        let replay = SqliteHarnessRepository::new(
            replay_root.clone(),
            replay_root.join("harness.db"),
            repository.schema_dir.clone(),
        );
        replay.init().unwrap();
        seed_resolver_completion_fixture(&replay, "US-COMPLETE", passing_command());
        let replay_connection = replay.open_existing().unwrap();
        replay_connection.execute("INSERT INTO backlog (uid, title, status, outcome_schedule_kind) VALUES ('blg_44444444444444444444444444444444', 'second resolver', 'accepted', 'manual');", []).unwrap();
        replay_connection.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at) VALUES ('US-COMPLETE', 'blg_44444444444444444444444444444444', 'resolves', '2026-01-02 00:00:00');", []).unwrap();
        assert!(replay.apply_changeset(&changeset_path).unwrap().applied);
        let replayed: (String, Option<i64>, String) = replay_connection.query_row("SELECT resolution_evidence, outcome_baseline_trace_count, closed_at FROM backlog WHERE uid='blg_33333333333333333333333333333333';", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        assert_eq!(replayed, (live.0, live.1, live.3));
        let replayed_verified_at: String = replay_connection
            .query_row(
                "SELECT last_verified_at FROM story WHERE id='US-COMPLETE'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(replayed_verified_at, live_verified_at);
    }

    #[test]
    fn story_completion_failure_and_transaction_error_close_nothing() {
        let (_failure_dir, failure_repository) = isolated_test_repository();
        failure_repository.init().unwrap();
        let (failure_backlog_id, _, _) =
            seed_resolver_completion_fixture(&failure_repository, "US-FAIL", failing_command());
        let failed = failure_repository.complete_story("US-FAIL").unwrap();
        assert_eq!(failed.result, "fail");
        let failure_connection = failure_repository.open_existing().unwrap();
        let failure_state: (String, String, String) = failure_connection.query_row("SELECT story.status, story.last_verified_result, backlog.status FROM story JOIN story_backlog_link AS link ON link.story_id=story.id JOIN backlog ON backlog.uid=link.backlog_uid WHERE story.id='US-FAIL' AND backlog.id=?1;", params![failure_backlog_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        assert_eq!(
            failure_state,
            (
                "in_progress".to_owned(),
                "fail".to_owned(),
                "accepted".to_owned()
            )
        );

        let (_rollback_dir, rollback_repository) = isolated_test_repository();
        rollback_repository.init().unwrap();
        let (rollback_backlog_id, _, _) = seed_resolver_completion_fixture(
            &rollback_repository,
            "US-ROLLBACK",
            passing_command(),
        );
        let rollback_connection = rollback_repository.open_existing().unwrap();
        rollback_connection.execute_batch("CREATE TRIGGER fail_completion_backlog BEFORE UPDATE OF status ON backlog WHEN NEW.status='implemented' BEGIN SELECT RAISE(FAIL, 'injected completion failure'); END;").unwrap();
        assert!(rollback_repository.complete_story("US-ROLLBACK").is_err());
        let rollback_state: (String, Option<String>, String) = rollback_connection.query_row("SELECT story.status, story.last_verified_result, backlog.status FROM story JOIN story_backlog_link AS link ON link.story_id=story.id JOIN backlog ON backlog.uid=link.backlog_uid WHERE story.id='US-ROLLBACK' AND backlog.id=?1;", params![rollback_backlog_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        assert_eq!(
            rollback_state,
            ("in_progress".to_owned(), None, "accepted".to_owned())
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn story_completion_concurrent_calls_commit_once() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        add_completion_story(
            &repository,
            "US-CONCURRENT",
            Some("touch completion-ready-$$; while [ \"$(find . -maxdepth 1 -name 'completion-ready-*' | wc -l | tr -d ' ')\" -lt 2 ]; do sleep 0.01; done"),
        );

        let root = repository.repo_root.clone();
        let db = repository.db_path.clone();
        let schema = repository.schema_dir.clone();
        let first = std::thread::spawn({
            let root = root.clone();
            let db = db.clone();
            let schema = schema.clone();
            move || {
                SqliteHarnessRepository::new(root, db, schema)
                    .with_run_id("run_completion_concurrent")
                    .complete_story("US-CONCURRENT")
                    .unwrap()
                    .result
            }
        });
        let second = std::thread::spawn(move || {
            SqliteHarnessRepository::new(root, db, schema)
                .with_run_id("run_completion_concurrent")
                .complete_story("US-CONCURRENT")
                .unwrap()
                .result
        });
        let mut results = vec![first.join().unwrap(), second.join().unwrap()];
        results.sort();
        assert_eq!(results, vec!["already-completed", "pass"]);
        let changeset = fs::read_to_string(
            temp_dir
                .path()
                .join("repo/.harness/changesets/run_completion_concurrent.changeset.jsonl"),
        )
        .unwrap();
        assert_eq!(changeset.matches("\"op\":\"story.complete\"").count(), 1);
    }

    #[test]
    fn story_completion_rejects_ineligible_resolver_target_before_verification() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let (backlog_id, _, _) =
            seed_resolver_completion_fixture(&repository, "US-TARGET", passing_command());
        let connection = repository.open_existing().unwrap();
        for (status, evidence) in [
            ("proposed", None),
            ("rejected", None),
            (
                "implemented",
                Some("{\"story_id\":\"US-OTHER\",\"result\":\"pass\"}"),
            ),
        ] {
            connection
                .execute(
                    "UPDATE backlog SET status=?1, resolution_evidence=?2 WHERE id=?3;",
                    params![status, evidence, backlog_id],
                )
                .unwrap();
            assert!(
                matches!(repository.complete_story("US-TARGET"), Err(HarnessInfraError::StoryCompletion(message)) if message.contains(&format!("is '{status}'")))
            );
        }
        let status: String = connection
            .query_row(
                "SELECT status FROM story WHERE id='US-TARGET';",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "in_progress");
    }

    #[test]
    fn story_verify_records_pass_fail_and_missing_command() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        fs::create_dir_all(&repo_root).unwrap();
        let schema_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf()
            .join("scripts/schema");
        let repository = SqliteHarnessRepository::new(
            repo_root.clone(),
            temp_dir.path().join("harness.db"),
            schema_root,
        );
        repository.init().unwrap();

        let pwd_output = repo_root.join("story-verify-pwd.txt");
        let verify_command = if cfg!(windows) {
            "cd > story-verify-pwd.txt".to_owned()
        } else {
            "pwd > story-verify-pwd.txt".to_owned()
        };
        repository
            .add_story(StoryAddInput {
                id: "US-PASS".to_owned(),
                title: "Passing story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some(verify_command),
                notes: None,
            })
            .unwrap();
        let pass = repository.verify_story("US-PASS").unwrap();
        assert_eq!(pass.result, "pass");
        assert_eq!(
            fs::canonicalize(fs::read_to_string(pwd_output).unwrap().trim()).unwrap(),
            fs::canonicalize(repo_root).unwrap()
        );
        assert_eq!(
            repository
                .story_verify_status("US-PASS")
                .unwrap()
                .last_verified_result
                .as_deref(),
            Some("pass")
        );

        repository
            .add_story(StoryAddInput {
                id: "US-FAIL".to_owned(),
                title: "Failing story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some("exit 1".to_owned()),
                notes: None,
            })
            .unwrap();
        let fail = repository.verify_story("US-FAIL").unwrap();
        assert_eq!(fail.result, "fail");
        assert_eq!(
            repository
                .story_verify_status("US-FAIL")
                .unwrap()
                .last_verified_result
                .as_deref(),
            Some("fail")
        );

        repository
            .add_story(StoryAddInput {
                id: "US-MISSING".to_owned(),
                title: "Missing command story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: None,
                notes: None,
            })
            .unwrap();
        assert!(matches!(
            repository.verify_story("US-MISSING"),
            Err(HarnessInfraError::MissingStoryVerifyCommand(id)) if id == "US-MISSING"
        ));
    }

    #[test]
    fn validation_subprocesses_do_not_inherit_run_operation_log_env() {
        let (_temp_dir, mut repository) = isolated_test_repository();
        repository.init().unwrap();

        for (id, status) in [
            ("US-VERIFY", None),
            ("US-VERIFY-ALL", None),
            ("US-COMPLETE-ENV", Some("in_progress")),
        ] {
            repository
                .add_story(StoryAddInput {
                    id: id.to_owned(),
                    title: id.to_owned(),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: Some(env_clean_verification_command().to_owned()),
                    notes: None,
                })
                .unwrap();
            if let Some(status) = status {
                repository
                    .update_story(StoryUpdateInput {
                        id: id.to_owned(),
                        contract_doc: None,
                        status: Some(status.to_owned()),
                        evidence: None,
                        unit: None,
                        integration: None,
                        e2e: None,
                        platform: None,
                        verify_command: None,
                    })
                    .unwrap();
            }
        }

        repository.run_id_override = Some("run_validation_env".to_owned());
        repository.verification_env_override = vec![
            ("HARNESS_RUN_ID".to_owned(), "run_validation_env".to_owned()),
            ("HARNESS_RUN_MODE".to_owned(), "execute".to_owned()),
            (
                "HARNESS_DB_PATH".to_owned(),
                repository.db_path.display().to_string(),
            ),
        ];

        assert_eq!(repository.verify_story("US-VERIFY").unwrap().result, "pass");
        assert_eq!(repository.verify_all_stories().unwrap().failed(), 0);
        assert_eq!(
            repository.complete_story("US-COMPLETE-ENV").unwrap().result,
            "pass"
        );

        let changeset =
            fs::read_to_string(repository.changeset_path("run_validation_env")).unwrap();
        assert!(changeset.contains("\"op\":\"story.verify\""));
        assert!(changeset.contains("\"op\":\"story.complete\""));
    }

    #[test]
    fn story_verify_all_reports_pass_fail_and_skipped() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        for (id, command) in [
            ("US-PASS", Some("exit 0")),
            ("US-FAIL", Some("exit 1")),
            ("US-SKIP", None),
        ] {
            repository
                .add_story(StoryAddInput {
                    id: id.to_owned(),
                    title: id.to_owned(),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: command.map(str::to_owned),
                    notes: None,
                })
                .unwrap();
        }

        let result = repository.verify_all_stories().unwrap();

        assert_eq!(result.passed(), 1);
        assert_eq!(result.failed(), 1);
        assert_eq!(result.skipped(), 1);
        assert_eq!(
            repository
                .story_verify_status("US-PASS")
                .unwrap()
                .last_verified_result
                .as_deref(),
            Some("pass")
        );
        assert_eq!(
            repository
                .story_verify_status("US-FAIL")
                .unwrap()
                .last_verified_result
                .as_deref(),
            Some("fail")
        );
    }

    #[test]
    fn tool_registry_register_query_and_remove_work() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();

        repository
            .register_tool(ToolRegisterInput {
                name: "deploy-check".to_owned(),
                command: "definitely-missing-tool".to_owned(),
                description: "Verify deploy health before release".to_owned(),
                responsibility: "Verification".to_owned(),
                args: Vec::new(),
                force: true,
                kind: "cli".to_owned(),
                capability: Some("deploy-verification".to_owned()),
                scan_target: None,
            })
            .unwrap();
        assert!(matches!(
            repository.register_tool(ToolRegisterInput {
                name: "deploy-check".to_owned(),
                command: "definitely-missing-tool".to_owned(),
                description: "Verify deploy health before release".to_owned(),
                responsibility: "Verification".to_owned(),
                args: Vec::new(),
                force: true,
                kind: "cli".to_owned(),
                capability: Some("deploy-verification".to_owned()),
                scan_target: None,
            }),
            Err(HarnessInfraError::ToolAlreadyExists(_, _))
        ));

        let verification_tools = repository
            .query_tools(Some("Verification".to_owned()), None)
            .unwrap();
        assert!(verification_tools
            .iter()
            .any(|tool| tool.name == "deploy-check" && tool.source == "registered"));

        // Capability lookup returns the registered provider.
        let by_capability = repository
            .query_tools(None, Some("deploy-verification".to_owned()))
            .unwrap();
        assert!(by_capability.iter().any(|tool| tool.name == "deploy-check"));

        repository.remove_tool("deploy-check").unwrap();
        assert!(!repository
            .query_tools(None, None)
            .unwrap()
            .iter()
            .any(|tool| tool.name == "deploy-check"));
    }

    #[test]
    fn tool_check_scans_and_persists_status_per_kind() {
        let (temp_dir, repository) = test_repository();
        repository.init().unwrap();

        // Absolute scan targets keep the test hermetic: test_repository's
        // repo_root points at the real project, so relative targets would
        // resolve against the checkout rather than the temp dir.
        let present_target = temp_dir.path().join("skill-present");
        std::fs::create_dir_all(&present_target).unwrap();
        let missing_target = temp_dir.path().join("mcp-missing");

        // An mcp tool whose scan target does not exist -> missing.
        repository
            .register_tool(ToolRegisterInput {
                name: "mcp-example".to_owned(),
                command: "mcp:example-server".to_owned(),
                description: "Example MCP-backed provider".to_owned(),
                responsibility: "Verification".to_owned(),
                args: Vec::new(),
                force: false,
                kind: "mcp".to_owned(),
                capability: Some("impact-analysis".to_owned()),
                scan_target: Some(missing_target.to_string_lossy().into_owned()),
            })
            .unwrap();

        // A skill tool whose scan target exists -> present.
        repository
            .register_tool(ToolRegisterInput {
                name: "skill-example".to_owned(),
                command: "skill:example-skill".to_owned(),
                description: "Example skill-backed provider".to_owned(),
                responsibility: "Verification".to_owned(),
                args: Vec::new(),
                force: false,
                kind: "skill".to_owned(),
                capability: Some("impact-analysis".to_owned()),
                scan_target: Some(present_target.to_string_lossy().into_owned()),
            })
            .unwrap();

        let results = repository.check_tools(None).unwrap();
        let mcp_tool = results.iter().find(|r| r.name == "mcp-example").unwrap();
        let skill_tool = results.iter().find(|r| r.name == "skill-example").unwrap();
        assert_eq!(mcp_tool.status, "missing");
        assert_eq!(skill_tool.status, "present");

        // Status is persisted, not just returned.
        let stored = repository
            .query_tools(None, Some("impact-analysis".to_owned()))
            .unwrap();
        assert_eq!(stored.len(), 2);
        assert!(stored
            .iter()
            .all(|tool| tool.checked_at.as_deref().is_some_and(|v| !v.is_empty())));
        assert_eq!(
            stored
                .iter()
                .find(|t| t.name == "skill-example")
                .unwrap()
                .status,
            "present"
        );
    }

    #[test]
    fn interventions_can_be_added_and_filtered() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-I".to_owned(),
                title: "Intervention story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: None,
                notes: None,
            })
            .unwrap();
        let trace_id = repository
            .record_trace(TraceInput {
                task_summary: "Trace for intervention".to_owned(),
                intake_id: None,
                story_id: Some("US-I".to_owned()),
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: Some("none".to_owned()),
                notes: None,
                actions: CsvList::from_optional(None),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(None),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
        repository
            .add_intervention(InterventionAddInput {
                trace_id: Some(trace_id),
                story_id: Some("US-I".to_owned()),
                intervention_type: "correction".to_owned(),
                description: "Use error handling instead of unwrap".to_owned(),
                source: "human".to_owned(),
                impact: Some("Reduced panic risk".to_owned()),
            })
            .unwrap();

        assert_eq!(
            repository
                .query_interventions(InterventionFilter {
                    trace_id: Some(trace_id),
                    story_id: None,
                    intervention_type: None,
                })
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            repository
                .query_interventions(InterventionFilter {
                    trace_id: None,
                    story_id: Some("US-I".to_owned()),
                    intervention_type: Some("override".to_owned()),
                })
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn audit_detects_drift_and_propose_can_commit_backlog_items() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-AUDIT".to_owned(),
                title: "Audit story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some("exit 0".to_owned()),
                notes: None,
            })
            .unwrap();
        repository
            .update_story(StoryUpdateInput {
                id: "US-AUDIT".to_owned(),
                contract_doc: None,
                status: Some("in_progress".to_owned()),
                evidence: None,
                unit: None,
                integration: None,
                e2e: None,
                platform: None,
                verify_command: None,
            })
            .unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-RETIRED".to_owned(),
                title: "Retired audit story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some("exit 0".to_owned()),
                notes: None,
            })
            .unwrap();
        repository
            .update_story(StoryUpdateInput {
                id: "US-RETIRED".to_owned(),
                contract_doc: None,
                status: Some("retired".to_owned()),
                evidence: None,
                unit: None,
                integration: None,
                e2e: None,
                platform: None,
                verify_command: None,
            })
            .unwrap();
        repository
            .add_backlog(BacklogAddInput {
                title: "Implemented without outcome".to_owned(),
                discovered_while: None,
                current_pain: None,
                suggestion: None,
                risk: Some(RiskLane::Tiny),
                predicted_impact: Some("Expected improvement".to_owned()),
                notes: None,
            })
            .unwrap();
        repository
            .close_backlog(BacklogCloseInput {
                id: 1,
                status: "implemented".to_owned(),
                actual_outcome: None,
            })
            .unwrap();
        repository
            .register_tool(ToolRegisterInput {
                name: "missing-tool".to_owned(),
                command: "definitely-missing-tool".to_owned(),
                description: "Missing command for audit coverage".to_owned(),
                responsibility: "Verification".to_owned(),
                args: Vec::new(),
                force: true,
                kind: "cli".to_owned(),
                capability: None,
                scan_target: None,
            })
            .unwrap();
        for _ in 0..2 {
            repository
                .record_trace(TraceInput {
                    task_summary: "Repeated friction trace".to_owned(),
                    intake_id: None,
                    story_id: None,
                    agent: Some("codex".to_owned()),
                    outcome: Some("completed".to_owned()),
                    duration_seconds: None,
                    token_estimate: None,
                    friction: Some("Context rules missed schema decision".to_owned()),
                    notes: None,
                    actions: CsvList::from_optional(Some("read".to_owned())),
                    files_read: CsvList::from_optional(Some("docs/HARNESS.md".to_owned())),
                    files_changed: CsvList::from_optional(Some(
                        "scripts/schema/003-tool-registry.sql".to_owned(),
                    )),
                    decisions: CsvList::from_optional(None),
                    errors: CsvList::from_optional(None),
                })
                .unwrap();
        }

        let audit = repository.audit().unwrap();
        assert_eq!(audit.orphaned_stories.len(), 1);
        assert_eq!(audit.unverified_stories.len(), 1);
        assert_eq!(audit.backlog_without_outcomes.len(), 1);
        assert_eq!(audit.broken_tools.len(), 1);
        assert!(audit.entropy_score() > 0);

        let proposals = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals;
        assert!(proposals.iter().any(|proposal| proposal
            .evidence
            .contains("Context rules missed schema decision")));
        assert!(proposals
            .iter()
            .all(|proposal| proposal.committed_backlog_id.is_none()));
        assert!(repository
            .query_backlog(BacklogFilter::Open)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn proposal_decision_accepts_one_key_idempotently_and_rejects_one_key() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        for _ in 0..2 {
            repository
                .record_trace(TraceInput {
                    task_summary: "Proposal decision fixture".to_owned(),
                    intake_id: None,
                    story_id: None,
                    agent: Some("codex".to_owned()),
                    outcome: Some("completed".to_owned()),
                    duration_seconds: None,
                    token_estimate: None,
                    friction: Some("Repeatable proposal decision fixture".to_owned()),
                    notes: None,
                    actions: CsvList::from_optional(None),
                    files_read: CsvList::from_optional(None),
                    files_changed: CsvList::from_optional(None),
                    decisions: CsvList::from_optional(None),
                    errors: CsvList::from_optional(None),
                })
                .unwrap();
        }
        let proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .next()
            .unwrap();
        let accepted = repository
            .propose(ProposalDecision::Accept {
                key: proposal.key.clone(),
                schedule: "traces:3".to_owned(),
            })
            .unwrap();
        assert!(accepted.message.unwrap().contains("Accepted proposal"));
        let unchanged = repository
            .propose(ProposalDecision::Accept {
                key: proposal.key.clone(),
                schedule: "traces:3".to_owned(),
            })
            .unwrap();
        assert!(unchanged.message.unwrap().contains("unchanged"));
        assert!(repository
            .propose(ProposalDecision::Reject {
                key: proposal.key,
                reason: "not now".to_owned(),
            })
            .is_err());
    }

    #[test]
    fn proposal_recurrence_classifies_suppression_and_regression() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        for _ in 0..2 {
            record_proposal_friction(&repository, "Lifecycle regression fixture");
        }
        let proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("Lifecycle regression fixture"))
            .unwrap();
        assert_eq!(proposal.lifecycle_state, "new");
        assert_eq!(proposal.evidence_items.len(), 2);

        repository
            .propose(ProposalDecision::Accept {
                key: proposal.key.clone(),
                schedule: "manual".to_owned(),
            })
            .unwrap();
        let connection = repository.open_existing().unwrap();
        let predecessor: String = connection
            .query_row(
                "SELECT uid FROM backlog WHERE proposal_key=?1",
                params![proposal.key],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "UPDATE backlog SET status='implemented', closed_at=datetime('now'), resolution_evidence='fresh proof' WHERE uid=?1",
                params![predecessor],
            )
            .unwrap();

        assert!(repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .iter()
            .all(|item| item.key != proposal.key));
        let suppressed = repository
            .propose(ProposalDecision::PreviewSuppressed)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == proposal.key)
            .unwrap();
        assert_eq!(suppressed.lifecycle_state, "suppressed");
        assert!(suppressed
            .lifecycle_explanation
            .unwrap()
            .contains("no uncovered evidence"));

        record_proposal_friction(&repository, "Lifecycle regression fixture");
        let regression = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == proposal.key)
            .unwrap();
        assert_eq!(regression.lifecycle_state, "regression");
        assert_eq!(
            regression.predecessor_uid.as_deref(),
            Some(predecessor.as_str())
        );
        assert_eq!(regression.evidence_items.len(), 1);

        repository
            .propose(ProposalDecision::Accept {
                key: proposal.key.clone(),
                schedule: "traces:5".to_owned(),
            })
            .unwrap();
        let recurrence: (String, String, Option<String>, Option<String>, Option<i64>) = connection
            .query_row(
                "SELECT uid, occurrence_kind, predecessor_uid, outcome_schedule_kind, outcome_after_traces FROM backlog WHERE proposal_key=?1 ORDER BY id DESC LIMIT 1",
                params![proposal.key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_ne!(recurrence.0, predecessor);
        assert_eq!(recurrence.1, "regression");
        assert_eq!(recurrence.2.as_deref(), Some(predecessor.as_str()));
        assert_eq!(recurrence.3.as_deref(), Some("trace_count"));
        assert_eq!(recurrence.4, Some(5));
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM proposal_evidence_link WHERE backlog_uid=?1",
                    params![recurrence.0],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn proposal_recurrence_rejection_creates_reconsideration_idempotently() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        for _ in 0..2 {
            record_proposal_friction(&repository, "Lifecycle reconsideration fixture");
        }
        let proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("Lifecycle reconsideration fixture"))
            .unwrap();
        repository
            .propose(ProposalDecision::Reject {
                key: proposal.key.clone(),
                reason: "not useful yet".to_owned(),
            })
            .unwrap();
        record_proposal_friction(&repository, "Lifecycle reconsideration fixture");
        let candidate = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == proposal.key)
            .unwrap();
        assert_eq!(candidate.lifecycle_state, "reconsideration");
        let rejected = repository
            .propose(ProposalDecision::Reject {
                key: proposal.key.clone(),
                reason: "still not useful".to_owned(),
            })
            .unwrap();
        assert!(rejected
            .message
            .unwrap()
            .contains("Rejected reconsideration"));
        let unchanged = repository
            .propose(ProposalDecision::Reject {
                key: proposal.key.clone(),
                reason: "still not useful".to_owned(),
            })
            .unwrap();
        assert!(unchanged.message.unwrap().contains("unchanged"));

        let connection = repository.open_existing().unwrap();
        let latest: (String, Option<String>, Option<String>, Option<String>) = connection
            .query_row(
                "SELECT occurrence_kind, predecessor_uid, outcome_schedule_kind, closed_at FROM backlog WHERE proposal_key=?1 ORDER BY id DESC LIMIT 1",
                params![proposal.key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(latest.0, "reconsideration");
        assert!(latest.1.is_some());
        assert_eq!(latest.2, None);
        assert!(latest.3.is_some());
    }

    #[test]
    fn proposal_recurrence_classifies_pending_accepted_and_legacy_rows() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        for _ in 0..2 {
            record_proposal_friction(&repository, "Lifecycle pending fixture");
        }
        let proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("Lifecycle pending fixture"))
            .unwrap();
        let connection = repository.open_existing().unwrap();
        connection
            .execute(
                "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, status) VALUES (?1, ?2, 'original', ?3, 'proposed')",
                params![stable_uid("blg", "pending fixture"), proposal.key, proposal.title],
            )
            .unwrap();
        let pending = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == proposal.key)
            .unwrap();
        assert_eq!(pending.lifecycle_state, "pending");
        assert!(pending.committed_backlog_id.is_some());
        connection
            .execute(
                "UPDATE backlog SET status='accepted', accepted_at=datetime('now'), outcome_schedule_kind='manual' WHERE proposal_key=?1",
                params![proposal.key],
            )
            .unwrap();
        let accepted = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == proposal.key)
            .unwrap();
        assert_eq!(accepted.lifecycle_state, "accepted");

        for _ in 0..2 {
            record_proposal_friction(&repository, "Lifecycle legacy fixture");
        }
        let legacy_proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("Lifecycle legacy fixture"))
            .unwrap();
        connection
            .execute(
                "INSERT INTO backlog (title, status) VALUES (?1, 'implemented')",
                params![legacy_proposal.title],
            )
            .unwrap();
        let legacy = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == legacy_proposal.key)
            .unwrap();
        assert_eq!(legacy.lifecycle_state, "legacy-unclassified");
        assert!(legacy
            .lifecycle_explanation
            .unwrap()
            .contains("US-080 reconciliation"));
    }

    #[test]
    fn proposal_recurrence_changeset_rebuild_preserves_lineage_and_evidence() {
        let (_temp_dir, repository) = isolated_test_repository();
        let repository = repository.with_run_id("run_proposal_recurrence_replay");
        repository.init().unwrap();
        for _ in 0..2 {
            record_proposal_friction(&repository, "Lifecycle replay fixture");
        }
        let proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("Lifecycle replay fixture"))
            .unwrap();
        repository
            .propose(ProposalDecision::Accept {
                key: proposal.key.clone(),
                schedule: "manual".to_owned(),
            })
            .unwrap();
        let mut connection = repository.open_existing().unwrap();
        let predecessor: String = connection
            .query_row(
                "SELECT uid FROM backlog WHERE proposal_key=?1",
                params![proposal.key],
                |row| row.get(0),
            )
            .unwrap();
        repository
            .with_logged_write(&mut connection, |transaction| {
                transaction.execute(
                    "UPDATE backlog SET status='implemented', closed_at=datetime('now'), resolution_evidence='replay proof' WHERE uid=?1",
                    params![predecessor],
                )?;
                Ok((
                    (),
                    vec![json!({
                        "op": "backlog.complete",
                        "version": 1,
                        "uid": predecessor,
                        "payload": {
                            "story_id": "US-REPLAY",
                            "resolution_evidence": "replay proof",
                            "trace_baseline": null,
                        }
                    })],
                ))
            })
            .unwrap();
        record_proposal_friction(&repository, "Lifecycle replay fixture");
        repository
            .propose(ProposalDecision::Accept {
                key: proposal.key.clone(),
                schedule: "traces:4".to_owned(),
            })
            .unwrap();

        let changeset = repository
            .repo_root
            .join(".harness/changesets/run_proposal_recurrence_replay.changeset.jsonl");
        let replay = SqliteHarnessRepository::new(
            repository.repo_root.clone(),
            repository.repo_root.join("replay.db"),
            repository.schema_dir.clone(),
        );
        replay.init().unwrap();
        replay.apply_changeset(&changeset).unwrap();
        let replay_connection = replay.open_existing().unwrap();
        let rows: i64 = replay_connection
            .query_row(
                "SELECT COUNT(*) FROM backlog WHERE proposal_key=?1",
                params![proposal.key],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2);
        let recurrence: (String, Option<String>, i64) = replay_connection
            .query_row(
                "SELECT backlog.occurrence_kind, backlog.predecessor_uid,
                        (SELECT COUNT(*) FROM proposal_evidence_link WHERE backlog_uid=backlog.uid)
                 FROM backlog WHERE proposal_key=?1 ORDER BY id DESC LIMIT 1",
                params![proposal.key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(recurrence.0, "regression");
        assert_eq!(recurrence.1.as_deref(), Some(predecessor.as_str()));
        assert_eq!(recurrence.2, 1);
    }

    #[test]
    fn improvement_health_outcomes_append_replay_and_open_work_is_refused() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let repository = repository.with_run_id("run_improvement_health_outcome");
        let connection = repository.open_existing().unwrap();
        let implemented_uid = "blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        connection.execute(
            "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, status, predicted_impact, actual_outcome, outcome_schedule_kind)
             VALUES (?1, 'improvement.proposal:v1:test', 'original', 'Measured improvement', 'implemented', 'less friction', 'legacy field unchanged', 'manual');",
            params![implemented_uid],
        ).unwrap();
        let implemented_id = connection.last_insert_rowid();
        connection.execute(
            "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, status, outcome_schedule_kind)
             VALUES ('blg_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'improvement.proposal:v1:open', 'original', 'Open improvement', 'accepted', 'manual');",
            [],
        ).unwrap();
        let open_id = connection.last_insert_rowid();

        let first = repository
            .record_backlog_outcome(BacklogOutcomeInput {
                id: implemented_id,
                status: "confirmed".to_owned(),
                outcome: "Friction decreased".to_owned(),
                evidence: Some("five clean traces".to_owned()),
            })
            .unwrap();
        let second = repository
            .record_backlog_outcome(BacklogOutcomeInput {
                id: implemented_id,
                status: "ineffective".to_owned(),
                outcome: "The initial gain was too small".to_owned(),
                evidence: Some("benchmark comparison".to_owned()),
            })
            .unwrap();
        let third = repository
            .record_backlog_outcome(BacklogOutcomeInput {
                id: implemented_id,
                status: "reverted".to_owned(),
                outcome: "The issue returned".to_owned(),
                evidence: None,
            })
            .unwrap();
        assert_eq!((first.ordinal, second.ordinal, third.ordinal), (1, 2, 3));
        assert_eq!(
            repository
                .record_backlog_outcome(BacklogOutcomeInput {
                    id: open_id,
                    status: "confirmed".to_owned(),
                    outcome: "too early".to_owned(),
                    evidence: None,
                })
                .unwrap_err()
                .to_string(),
            format!(
                "backlog outcome record: backlog item '{open_id}' must be an implemented keyed occurrence"
            )
        );

        let connection = repository.open_existing().unwrap();
        let state: (i64, String, String) = connection
            .query_row(
            "SELECT COUNT(*), MAX(CASE WHEN ordinal=3 THEN backlog_outcome_observation.status END), backlog.actual_outcome
             FROM backlog_outcome_observation
             JOIN backlog ON backlog.uid=backlog_outcome_observation.backlog_uid
             WHERE backlog_uid=?1;",
                params![implemented_uid],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            state,
            (
                3,
                "reverted".to_owned(),
                "legacy field unchanged".to_owned()
            )
        );
        assert!(repository
            .audit()
            .unwrap()
            .backlog_without_outcomes
            .is_empty());

        let changeset = repository.changeset_path("run_improvement_health_outcome");
        let replay = SqliteHarnessRepository::new(
            repository.repo_root.clone(),
            repository.repo_root.join("replay.db"),
            repository.schema_dir.clone(),
        );
        replay.init().unwrap();
        replay.open_existing().unwrap().execute(
            "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, status, predicted_impact, actual_outcome, outcome_schedule_kind)
             VALUES (?1, 'improvement.proposal:v1:test', 'original', 'Measured improvement', 'implemented', 'less friction', 'legacy field unchanged', 'manual');",
            params![implemented_uid],
        ).unwrap();
        replay.apply_changeset(&changeset).unwrap();
        let replay_count: i64 = replay
            .open_existing()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM backlog_outcome_observation WHERE backlog_uid=?1;",
                params![implemented_uid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(replay_count, 3);
    }

    #[test]
    fn improvement_health_derives_schedule_states_and_is_read_only() {
        let (_temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let connection = repository.open_existing().unwrap();
        for (uid, key, title, kind, due, after, baseline) in [
            (
                "blg_11111111111111111111111111111111",
                "manual",
                "Manual review",
                "manual",
                None,
                None,
                None,
            ),
            (
                "blg_22222222222222222222222222222222",
                "future",
                "Future review",
                "due_at",
                Some("2999-01-01T00:00:00Z"),
                None,
                None,
            ),
            (
                "blg_33333333333333333333333333333333",
                "past",
                "Due review",
                "due_at",
                Some("2000-01-01T00:00:00Z"),
                None,
                None,
            ),
            (
                "blg_44444444444444444444444444444444",
                "traces",
                "Trace review",
                "trace_count",
                None,
                Some(2),
                Some(0),
            ),
            (
                "blg_55555555555555555555555555555555",
                "error",
                "Broken baseline",
                "trace_count",
                None,
                Some(2),
                Some(9),
            ),
        ] {
            connection.execute(
                "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, status, predicted_impact, outcome_schedule_kind, outcome_due_at, outcome_after_traces, outcome_baseline_trace_count)
                 VALUES (?1, ?2, 'original', ?3, 'implemented', 'health fixture', ?4, ?5, ?6, ?7);",
                params![uid, format!("improvement.proposal:v1:{key}"), title, kind, due, after, baseline],
            ).unwrap();
        }
        connection.execute(
            "INSERT INTO backlog (uid, proposal_key, occurrence_kind, title, status, predicted_impact)
             VALUES ('blg_66666666666666666666666666666666', 'improvement.proposal:v1:legacy-plan', 'original', 'Missing plan', 'implemented', 'health fixture');",
            [],
        ).unwrap();
        for index in 1..=2 {
            connection
                .execute(
                    "INSERT INTO trace (uid, task_summary) VALUES (?1, 'health trace');",
                    params![format!("trc_{index:032x}")],
                )
                .unwrap();
        }
        connection.execute(
            "INSERT INTO backlog_outcome_observation (uid, backlog_uid, ordinal, status, outcome, observed_at)
             VALUES ('obs_77777777777777777777777777777777', 'blg_11111111111111111111111111111111', 1, 'legacy_recorded', 'preserved old outcome', '2026-01-01 00:00:00');",
            [],
        ).unwrap();
        drop(connection);

        let before = fs::read(&repository.db_path).unwrap();
        let health = repository.query_improvement_health().unwrap();
        let health_again = repository.query_improvement_health().unwrap();
        let after = fs::read(&repository.db_path).unwrap();
        assert_eq!(health, health_again);
        assert_eq!(before, after);
        let states = health
            .items
            .iter()
            .filter(|item| item.category == "outcome_review")
            .map(|item| (item.title.as_str(), item.state.as_str()))
            .collect::<Vec<_>>();
        assert!(states.contains(&("Manual review", "legacy_recorded")));
        assert!(states.contains(&("Future review", "scheduled_not_due")));
        assert!(states.contains(&("Due review", "due")));
        assert!(states.contains(&("Trace review", "due")));
        assert!(states.contains(&("Broken baseline", "schedule_error")));
        assert!(states.contains(&("Missing plan", "awaiting_observation_plan")));
        let legacy = health
            .items
            .iter()
            .find(|item| item.title == "Manual review")
            .unwrap();
        assert_eq!(legacy.outcome, "preserved old outcome");
    }

    #[test]
    fn story_backlog_trace_and_queries_work() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();

        repository
            .add_story(StoryAddInput {
                id: "US-T".to_owned(),
                title: "Test story".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: None,
                notes: None,
            })
            .unwrap();
        repository
            .update_story(StoryUpdateInput {
                id: "US-T".to_owned(),
                contract_doc: None,
                status: Some("in_progress".to_owned()),
                evidence: Some("unit test".to_owned()),
                unit: Some(BoolFlag(1)),
                integration: None,
                e2e: None,
                platform: None,
                verify_command: None,
            })
            .unwrap();
        assert_eq!(repository.query_matrix().unwrap()[0].unit, 1);

        let backlog_id = repository
            .add_backlog(BacklogAddInput {
                title: "Improve CLI".to_owned(),
                discovered_while: None,
                current_pain: Some("manual SQL".to_owned()),
                suggestion: None,
                risk: Some(RiskLane::HighRisk),
                predicted_impact: None,
                notes: None,
            })
            .unwrap();
        repository
            .close_backlog(BacklogCloseInput {
                id: backlog_id,
                status: "implemented".to_owned(),
                actual_outcome: Some("done".to_owned()),
            })
            .unwrap();
        assert_eq!(
            repository.query_backlog(BacklogFilter::All).unwrap()[0]
                .actual_outcome
                .as_deref(),
            Some("done")
        );

        let trace_id = repository
            .record_trace(TraceInput {
                task_summary: "Test trace".to_owned(),
                intake_id: None,
                story_id: Some("US-T".to_owned()),
                agent: Some("test".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: Some("none".to_owned()),
                notes: None,
                actions: CsvList::from_optional(Some("one,two".to_owned())),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(None),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
        assert_eq!(trace_id, 1);
        assert_eq!(
            repository.query_traces().unwrap()[0].task_summary,
            "Test trace"
        );
        assert_eq!(
            repository.query_friction().unwrap()[0].harness_friction,
            "none"
        );
    }

    #[test]
    fn friction_query_includes_intake_context_and_filters_null_friction() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        let intake_id = repository
            .record_intake(IntakeInput {
                input_type: InputType::ChangeRequest,
                summary: "Friction query context".to_owned(),
                risk_lane: RiskLane::Normal,
                risk_flags: CsvList::from_optional(None),
                affected_docs: CsvList::from_optional(None),
                story_id: None,
                notes: None,
            })
            .unwrap();
        repository
            .record_trace(TraceInput {
                task_summary: "Trace without friction".to_owned(),
                intake_id: Some(intake_id),
                story_id: None,
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: None,
                notes: None,
                actions: CsvList::from_optional(None),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(None),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
        repository
            .record_trace(TraceInput {
                task_summary: "Trace with linked friction".to_owned(),
                intake_id: Some(intake_id),
                story_id: None,
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: Some("Linked friction".to_owned()),
                notes: None,
                actions: CsvList::from_optional(None),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(None),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
        repository
            .record_trace(TraceInput {
                task_summary: "Trace with unlinked friction".to_owned(),
                intake_id: None,
                story_id: None,
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: Some("Unlinked friction".to_owned()),
                notes: None,
                actions: CsvList::from_optional(None),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(None),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();

        let friction = repository.query_friction().unwrap();

        assert_eq!(friction.len(), 2);
        assert_eq!(friction[0].risk_lane, None);
        assert_eq!(friction[0].input_type, None);
        assert_eq!(friction[1].risk_lane.as_deref(), Some("normal"));
        assert_eq!(friction[1].input_type.as_deref(), Some("change_request"));
    }

    #[test]
    fn import_brownfield_seeds_markdown_state_idempotently() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        fs::create_dir_all(repo_root.join("docs/decisions")).unwrap();
        fs::write(
            repo_root.join("docs/TEST_MATRIX.md"),
            r#"# Test Matrix

| Story | Contract | Unit | Integration | E2E | Platform | Status | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| US-010 | docs/product/tasks.md | yes | pending | no | mac smoke | implemented | cargo test |
"#,
        )
        .unwrap();
        fs::write(
            repo_root.join("docs/decisions/0007-test-decision.md"),
            r#"# Test Decision

## Status

Accepted
"#,
        )
        .unwrap();
        fs::write(
            repo_root.join("docs/HARNESS_BACKLOG.md"),
            r#"# Harness Backlog

## Items

### Title

Import existing docs

### Discovered While

Testing brownfield import

### Current Pain

Existing Harness v0 repos have markdown truth.

### Suggested Improvement

Seed the durable database.

### Risk

normal

### Status

accepted

### Title

Keep installer checksum

### Discovered While

Testing release install

### Current Pain

Downloads need verification.

### Suggested Improvement

Verify sha256 files.

### Risk

high-risk

### Status

implemented
"#,
        )
        .unwrap();

        let source_repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf();
        let repository = SqliteHarnessRepository::new(
            repo_root.clone(),
            temp_dir.path().join("harness.db"),
            source_repo_root.join("scripts/schema"),
        );
        repository.init().unwrap();

        let first = repository.import_brownfield().unwrap();
        let second = repository.import_brownfield().unwrap();

        assert_eq!(
            first,
            BrownfieldImportResult {
                stories: 1,
                decisions: 1,
                backlog_items: 2,
            }
        );
        assert_eq!(second.backlog_items, 2);

        let matrix = repository.query_matrix().unwrap();
        assert_eq!(matrix[0].id, "US-010");
        assert_eq!(matrix[0].title, "docs/product/tasks.md");
        assert_eq!(matrix[0].status, "implemented");
        assert_eq!(matrix[0].unit, 1);
        assert_eq!(matrix[0].integration, 0);
        assert_eq!(matrix[0].platform, 1);

        let decisions = repository.query_decisions().unwrap();
        assert_eq!(decisions[0].id, "0007-test-decision");
        assert_eq!(decisions[0].status, "accepted");

        let backlog = repository.query_backlog(BacklogFilter::All).unwrap();
        assert_eq!(backlog.len(), 2);
        assert!(backlog
            .iter()
            .any(|item| item.title == "Import existing docs"
                && item.status == "accepted"
                && item.risk.as_deref() == Some("normal")));
        assert!(backlog
            .iter()
            .any(|item| item.title == "Keep installer checksum"
                && item.status == "implemented"
                && item.risk.as_deref() == Some("high_risk")));
    }

    #[test]
    fn filters_open_and_closed_backlog_items() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();

        let proposed_id = repository
            .add_backlog(BacklogAddInput {
                title: "Proposed item".to_owned(),
                discovered_while: None,
                current_pain: None,
                suggestion: None,
                risk: Some(RiskLane::Tiny),
                predicted_impact: Some("Should improve trace review.".to_owned()),
                notes: None,
            })
            .unwrap();
        let implemented_id = repository
            .add_backlog(BacklogAddInput {
                title: "Implemented item".to_owned(),
                discovered_while: None,
                current_pain: None,
                suggestion: None,
                risk: Some(RiskLane::Normal),
                predicted_impact: Some("Should reduce missing proof.".to_owned()),
                notes: None,
            })
            .unwrap();
        repository
            .close_backlog(BacklogCloseInput {
                id: implemented_id,
                status: "implemented".to_owned(),
                actual_outcome: Some("Proof gaps were found earlier.".to_owned()),
            })
            .unwrap();

        let all = repository.query_backlog(BacklogFilter::All).unwrap();
        let open = repository.query_backlog(BacklogFilter::Open).unwrap();
        let closed = repository.query_backlog(BacklogFilter::Closed).unwrap();

        assert_eq!(all.len(), 2);
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].id, proposed_id);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].id, implemented_id);
        assert_eq!(
            closed[0].actual_outcome.as_deref(),
            Some("Proof gaps were found earlier.")
        );
    }

    #[test]
    fn scores_latest_and_specific_trace_with_lane_lookup() {
        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        let intake_id = repository
            .record_intake(IntakeInput {
                input_type: InputType::HarnessImprovement,
                summary: "High risk trace quality test".to_owned(),
                risk_lane: RiskLane::HighRisk,
                risk_flags: CsvList::from_optional(None),
                affected_docs: CsvList::from_optional(None),
                story_id: None,
                notes: None,
            })
            .unwrap();
        let first_trace = repository
            .record_trace(TraceInput {
                task_summary: "Minimal trace test".to_owned(),
                intake_id: None,
                story_id: None,
                agent: None,
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: None,
                notes: None,
                actions: CsvList::from_optional(None),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(None),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
        repository
            .record_trace(TraceInput {
                task_summary: "Standard trace linked to high risk intake".to_owned(),
                intake_id: Some(intake_id),
                story_id: None,
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: Some("none".to_owned()),
                notes: None,
                actions: CsvList::from_optional(Some("read,patched".to_owned())),
                files_read: CsvList::from_optional(Some("PHASE3.md".to_owned())),
                files_changed: CsvList::from_optional(Some(
                    "crates/harness-cli/src/domain.rs".to_owned(),
                )),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();

        let latest = repository.score_trace(None).unwrap();
        assert_eq!(latest.achieved, TraceQualityTier::Standard);
        assert_eq!(latest.required, Some(TraceQualityTier::Detailed));
        assert!(!latest.meets_requirement);
        assert!(latest
            .missing_detailed
            .iter()
            .any(|field| field.starts_with("decisions_made")));

        let specific = repository.score_trace(Some(first_trace)).unwrap();
        assert_eq!(specific.trace_id, first_trace);
        assert_eq!(specific.achieved, TraceQualityTier::Minimal);
        assert_eq!(specific.required, None);
        assert!(specific.meets_requirement);
    }

    #[test]
    fn review_finding_rfc3339_unicode_and_exact_rejection_reason() {
        let negative = parse_observation_schedule("due:2099-01-01T12:00:00-05:00").unwrap();
        assert_eq!(negative.0, "due_at");
        assert_eq!(negative.1.as_deref(), Some("2099-01-01T17:00:00+00:00"));
        assert!(parse_observation_schedule("due:2099-01-01 12:00:00").is_err());

        let unicode = format!("{}🙂bbbb", "0".repeat(68));
        assert!(short_title(&unicode).ends_with("..."));
        assert_ne!(
            normalize_token("データベースが遅い"),
            normalize_token("認証が失敗")
        );

        let (_temp_dir, repository) = test_repository();
        repository.init().unwrap();
        for _ in 0..2 {
            record_proposal_friction(&repository, "review rejection prefix fixture");
        }
        let proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("review rejection prefix fixture"))
            .unwrap();
        repository
            .propose(ProposalDecision::Reject {
                key: proposal.key.clone(),
                reason: "not useful yet".to_owned(),
            })
            .unwrap();
        assert_eq!(
            repository
                .open_existing()
                .unwrap()
                .query_row(
                    "SELECT rejection_reason FROM backlog WHERE proposal_key=?1",
                    params![proposal.key],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "not useful yet"
        );
        let different = repository.propose(ProposalDecision::Reject {
            key: proposal.key,
            reason: "not useful".to_owned(),
        });
        assert!(matches!(
            different,
            Err(HarnessInfraError::ProposalDecision(message))
                if message.contains("different reason")
        ));

        for _ in 0..2 {
            record_proposal_friction(&repository, "review negative offset acceptance");
        }
        let due_proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("review negative offset acceptance"))
            .unwrap();
        let accepted = repository
            .propose(ProposalDecision::Accept {
                key: due_proposal.key,
                schedule: "due:2099-01-01T12:00:00-05:00".to_owned(),
            })
            .unwrap();
        assert!(accepted.message.unwrap().contains("Accepted proposal"));
    }

    #[test]
    fn review_finding_audit_decisions_require_stable_evidence_and_replay_replacement() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-AUDIT-REVIEW".to_owned(),
                title: "Original audit title".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some(passing_command().to_owned()),
                notes: None,
            })
            .unwrap();
        repository
            .open_existing()
            .unwrap()
            .execute(
                "UPDATE story SET status='implemented' WHERE id='US-AUDIT-REVIEW'",
                [],
            )
            .unwrap();

        let proposal = repository
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("unverified story"))
            .unwrap();
        let refused = repository.propose(ProposalDecision::Reject {
            key: proposal.key,
            reason: "reviewed".to_owned(),
        });
        assert!(matches!(
            refused,
            Err(HarnessInfraError::ProposalDecision(message))
                if message.contains("audit --record-evidence")
        ));

        let logged = repository.with_run_id("run_review_audit_replacement");
        logged.audit_record_evidence().unwrap();
        let recorded = logged
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("unverified story"))
            .unwrap();
        assert!(recorded
            .evidence_items
            .iter()
            .all(|item| item.source_kind == "audit"));
        logged
            .propose(ProposalDecision::Reject {
                key: recorded.key,
                reason: "reviewed stable evidence".to_owned(),
            })
            .unwrap();
        let connection = logged.open_existing().unwrap();
        connection
            .execute(
                "UPDATE story SET title='Changed audit title' WHERE id='US-AUDIT-REVIEW'",
                [],
            )
            .unwrap();
        logged.audit_record_evidence().unwrap();
        let changeset = logged.changeset_path("run_review_audit_replacement");
        let contents = fs::read_to_string(&changeset).unwrap();
        assert!(contents.contains("audit.evidence.clear"));
        let live_total: i64 = connection
            .query_row("SELECT COUNT(*) FROM audit_evidence_episode", [], |row| {
                row.get(0)
            })
            .unwrap();
        let live_active: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM audit_evidence_episode WHERE cleared_at IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let replay_root = temp_dir.path().join("audit-replay");
        fs::create_dir_all(&replay_root).unwrap();
        let replay = SqliteHarnessRepository::new(
            replay_root.clone(),
            replay_root.join("harness.db"),
            logged.schema_dir.clone(),
        );
        replay.init().unwrap();
        assert!(replay.apply_changeset(&changeset).unwrap().applied);
        let replay_connection = replay.open_existing().unwrap();
        assert_eq!(
            replay_connection
                .query_row("SELECT COUNT(*) FROM audit_evidence_episode", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            live_total
        );
        assert_eq!(
            replay_connection
                .query_row(
                    "SELECT COUNT(*) FROM audit_evidence_episode WHERE cleared_at IS NULL",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            live_active
        );
    }

    #[test]
    fn review_finding_resolver_link_order_is_precise_and_replayable() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        add_completion_story(&repository, "US-LINK-ORDER", Some(passing_command()));
        let connection = repository.open_existing().unwrap();
        connection.execute("INSERT INTO intake (uid, input_type, summary, risk_lane, story_id) VALUES ('ink_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'harness_improvement', 'review', 'high_risk', 'US-LINK-ORDER')", []).unwrap();
        connection.execute("INSERT INTO backlog (uid, title, status) VALUES ('blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'review resolver', 'accepted')", []).unwrap();
        let backlog_id = connection.last_insert_rowid();
        connection.execute("INSERT INTO trace (uid, recorded_at_unix_ns, created_at, intake_uid, task_summary, story_id, actions_taken, files_changed, outcome) VALUES ('trc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 100, '2026-01-01 00:00:00', 'ink_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'before link', 'US-LINK-ORDER', '[\"work\"]', '[\"src.rs\"]', 'completed')", []).unwrap();
        connection.execute("INSERT INTO story_backlog_link (story_id, backlog_uid, relationship, linked_at, linked_at_unix_ns) VALUES ('US-LINK-ORDER', 'blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'resolves', '2026-01-01 00:00:00', 200)", []).unwrap();
        assert!(matches!(
            repository.complete_story("US-LINK-ORDER"),
            Err(HarnessInfraError::StoryCompletion(message))
                if message.contains("after the newest resolver link")
        ));

        let logged = repository.with_run_id("run_review_link_replay");
        connection
            .execute(
                "DELETE FROM story_backlog_link WHERE story_id='US-LINK-ORDER'",
                [],
            )
            .unwrap();
        logged
            .link_story_backlog(StoryBacklogLinkInput {
                story_id: "US-LINK-ORDER".to_owned(),
                backlog_id,
                relationship: "resolves".to_owned(),
            })
            .unwrap();
        let live: (String, i64) = connection
            .query_row("SELECT linked_at, linked_at_unix_ns FROM story_backlog_link WHERE story_id='US-LINK-ORDER'", [], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();

        let replay_root = temp_dir.path().join("link-replay");
        fs::create_dir_all(&replay_root).unwrap();
        let replay = SqliteHarnessRepository::new(
            replay_root.clone(),
            replay_root.join("harness.db"),
            logged.schema_dir.clone(),
        );
        replay.init().unwrap();
        add_completion_story(&replay, "US-LINK-ORDER", Some(passing_command()));
        let replay_connection = replay.open_existing().unwrap();
        replay_connection.execute("INSERT INTO backlog (uid, title, status) VALUES ('blg_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'review resolver', 'accepted')", []).unwrap();
        replay
            .apply_changeset(&logged.changeset_path("run_review_link_replay"))
            .unwrap();
        let rebuilt: (String, i64) = replay_connection
            .query_row("SELECT linked_at, linked_at_unix_ns FROM story_backlog_link WHERE story_id='US-LINK-ORDER'", [], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        assert_eq!(rebuilt, live);
    }

    #[test]
    fn proof_audit_mixed_resolver_semantic_history_matches_after_replay() {
        let (temp_dir, live) = isolated_test_repository();
        live.init().unwrap();
        let replay_root = temp_dir.path().join("mixed-replay");
        fs::create_dir_all(&replay_root).unwrap();
        let replay = SqliteHarnessRepository::new(
            replay_root.clone(),
            replay_root.join("harness.db"),
            live.schema_dir.clone(),
        );
        replay.init().unwrap();

        for repository in [&live, &replay] {
            add_completion_story(repository, "US-MIXED-ORDER", Some(passing_command()));
            let connection = repository.open_existing().unwrap();
            connection.execute("INSERT INTO intake (uid, input_type, summary, risk_lane, story_id) VALUES ('ink_99999999999999999999999999999999', 'harness_improvement', 'mixed order', 'high_risk', 'US-MIXED-ORDER')", []).unwrap();
            connection.execute("INSERT INTO backlog (uid, title, status) VALUES ('blg_88888888888888888888888888888888', 'legacy resolver', 'accepted'), ('blg_99999999999999999999999999999999', 'precise resolver', 'accepted')", []).unwrap();
        }

        let before_changeset = temp_dir.path().join("mixed-before.changeset.jsonl");
        fs::write(&before_changeset, r#"{"op":"changeset.header","version":1,"run_id":"run_mixed_before","base_schema_version":12}
{"op":"story.backlog.link","version":1,"id":"US-MIXED-ORDER","payload":{"backlog_uid":"blg_88888888888888888888888888888888","relationship":"resolves","linked_at":"2025-12-31 23:59:59"}}
{"op":"story.backlog.link","version":2,"id":"US-MIXED-ORDER","payload":{"backlog_uid":"blg_99999999999999999999999999999999","relationship":"resolves","linked_at":"2026-01-01 00:00:00","linked_at_unix_ns":200}}
{"op":"trace.add","version":2,"uid":"trc_88888888888888888888888888888888","payload":{"recorded_at_unix_ns":150,"created_at":"2026-01-01 00:00:00","intake_uid":"ink_99999999999999999999999999999999","task_summary":"before newest link","story_id":"US-MIXED-ORDER","actions_taken":"[\"work\"]","files_changed":"[\"src.rs\"]","outcome":"completed"}}
"#).unwrap();
        let after_changeset = temp_dir.path().join("mixed-after.changeset.jsonl");
        fs::write(&after_changeset, r#"{"op":"changeset.header","version":1,"run_id":"run_mixed_after","base_schema_version":12}
{"op":"trace.add","version":2,"uid":"trc_99999999999999999999999999999999","payload":{"recorded_at_unix_ns":300,"created_at":"2026-01-01 00:00:00","intake_uid":"ink_99999999999999999999999999999999","task_summary":"after newest link","story_id":"US-MIXED-ORDER","actions_taken":"[\"work\"]","files_changed":"[\"src.rs\"]","outcome":"completed"}}
"#).unwrap();

        for repository in [&live, &replay] {
            repository.apply_changeset(&before_changeset).unwrap();
            let before_result = repository.complete_story("US-MIXED-ORDER");
            assert!(
                matches!(
                    &before_result,
                    Err(HarnessInfraError::StoryCompletion(message))
                        if message.contains("after the newest resolver link")
                ),
                "unexpected pre-link completion result: {before_result:?}"
            );
            repository.apply_changeset(&after_changeset).unwrap();
        }

        let live_result = live.complete_story("US-MIXED-ORDER").unwrap();
        let replay_result = replay.complete_story("US-MIXED-ORDER").unwrap();
        assert_eq!(live_result.result, replay_result.result);
        assert_eq!(
            live_result.closed_backlog_ids,
            replay_result.closed_backlog_ids
        );
        assert_eq!(
            live_result.implementation_trace_uid,
            replay_result.implementation_trace_uid
        );
        assert_eq!(
            live_result.implementation_trace_uid.as_deref(),
            Some("trc_99999999999999999999999999999999")
        );
    }

    #[test]
    fn review_finding_live_rebuild_recurrence_parity() {
        let (temp_dir, repository) = isolated_test_repository();
        repository.init().unwrap();
        let logged = repository.with_run_id("run_review_recurrence_parity");
        for _ in 0..2 {
            record_proposal_friction(&logged, "review recurrence parity fixture");
        }
        let proposal = logged
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.title.contains("review recurrence parity fixture"))
            .unwrap();
        logged
            .propose(ProposalDecision::Accept {
                key: proposal.key.clone(),
                schedule: "manual".to_owned(),
            })
            .unwrap();
        add_completion_story(&logged, "US-REVIEW-RECURRENCE", Some(passing_command()));
        let intake_id = logged
            .record_intake(IntakeInput {
                input_type: InputType::HarnessImprovement,
                summary: "review recurrence resolver".to_owned(),
                risk_lane: RiskLane::HighRisk,
                risk_flags: CsvList::from_optional(None),
                affected_docs: CsvList::from_optional(None),
                story_id: Some("US-REVIEW-RECURRENCE".to_owned()),
                notes: None,
            })
            .unwrap();
        let connection = logged.open_existing().unwrap();
        let backlog_id: i64 = connection
            .query_row(
                "SELECT id FROM backlog WHERE proposal_key=?1",
                params![proposal.key],
                |row| row.get(0),
            )
            .unwrap();
        logged
            .link_story_backlog(StoryBacklogLinkInput {
                story_id: "US-REVIEW-RECURRENCE".to_owned(),
                backlog_id,
                relationship: "resolves".to_owned(),
            })
            .unwrap();
        logged
            .record_trace(TraceInput {
                task_summary: "review recurrence implementation".to_owned(),
                intake_id: Some(intake_id),
                story_id: Some("US-REVIEW-RECURRENCE".to_owned()),
                agent: Some("codex".to_owned()),
                outcome: Some("completed".to_owned()),
                duration_seconds: None,
                token_estimate: None,
                friction: None,
                notes: None,
                actions: CsvList::from_optional(Some("implemented".to_owned())),
                files_read: CsvList::from_optional(None),
                files_changed: CsvList::from_optional(Some("src.rs".to_owned())),
                decisions: CsvList::from_optional(None),
                errors: CsvList::from_optional(None),
            })
            .unwrap();
        logged.complete_story("US-REVIEW-RECURRENCE").unwrap();
        record_proposal_friction(&logged, "review recurrence parity fixture");

        let live = logged
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == proposal.key)
            .unwrap();
        assert_eq!(live.lifecycle_state, "regression");

        let replay_root = temp_dir.path().join("recurrence-replay");
        fs::create_dir_all(&replay_root).unwrap();
        let replay = SqliteHarnessRepository::new(
            replay_root.clone(),
            replay_root.join("harness.db"),
            logged.schema_dir.clone(),
        );
        replay.init().unwrap();
        replay
            .apply_changeset(&logged.changeset_path("run_review_recurrence_parity"))
            .unwrap();
        let rebuilt = replay
            .propose(ProposalDecision::Preview)
            .unwrap()
            .proposals
            .into_iter()
            .find(|item| item.key == proposal.key)
            .unwrap();
        assert_eq!(rebuilt.lifecycle_state, "regression");
        assert_eq!(rebuilt.evidence_items, live.evidence_items);
        let live_order: (String, i64) = connection
            .query_row(
                "SELECT uid, recorded_at_unix_ns FROM trace WHERE task_summary='review recurrence implementation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let rebuilt_order: (String, i64) = replay
            .open_existing()
            .unwrap()
            .query_row(
                "SELECT uid, recorded_at_unix_ns FROM trace WHERE task_summary='review recurrence implementation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(rebuilt_order, live_order);
    }

    #[test]
    fn query_sql_keeps_select_cte_pragma_and_explain_reads_available() {
        let (_temp, repository) = isolated_test_repository();
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-SQL-READ".to_owned(),
                title: "Read-only SQL".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some(passing_command().to_owned()),
                notes: None,
            })
            .unwrap();

        let selected = repository
            .query_sql("SELECT id, title FROM story WHERE id='US-SQL-READ';")
            .unwrap();
        assert_eq!(selected.headers, ["id", "title"]);
        assert_eq!(
            selected.rows,
            [["US-SQL-READ".to_owned(), "Read-only SQL".to_owned()]]
        );

        let cte = repository
            .query_sql(
                "WITH selected AS (
                     SELECT id FROM story WHERE id='US-SQL-READ'
                 )
                 SELECT id FROM selected;",
            )
            .unwrap();
        assert_eq!(cte.headers, ["id"]);
        assert_eq!(cte.rows, [["US-SQL-READ".to_owned()]]);

        let table_info = repository.query_sql("PRAGMA table_info(story);").unwrap();
        assert_eq!(
            table_info.headers,
            ["cid", "name", "type", "notnull", "dflt_value", "pk"]
        );
        assert!(table_info.rows.iter().any(|row| row[1] == "id"));
        assert_eq!(
            repository.query_sql("PRAGMA foreign_keys;").unwrap().rows,
            [["1".to_owned()]]
        );

        let explain = repository
            .query_sql("EXPLAIN SELECT title FROM story WHERE id='US-SQL-READ';")
            .unwrap();
        assert!(!explain.rows.is_empty());
        assert_eq!(
            repository
                .open_existing()
                .unwrap()
                .query_row(
                    "SELECT title FROM story WHERE id='US-SQL-READ';",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "Read-only SQL"
        );
    }

    #[test]
    fn query_sql_is_physically_read_only_across_statement_shapes() {
        fn assert_denied(error: HarnessInfraError, sql: &str) {
            assert!(
                matches!(error, HarnessInfraError::QuerySqlWriteDenied),
                "expected read-only denial for {sql}, got {error}"
            );
        }

        let (_temp, repository) = isolated_test_repository();
        let repository = repository.with_run_id("run_query_sql_read_only");
        repository.init().unwrap();
        repository
            .add_story(StoryAddInput {
                id: "US-SQL-SENTINEL".to_owned(),
                title: "Mutation sentinel".to_owned(),
                risk_lane: RiskLane::Normal,
                contract_doc: None,
                verify_command: Some(passing_command().to_owned()),
                notes: None,
            })
            .unwrap();
        let changeset = repository.changeset_path("run_query_sql_read_only");
        let operation_log_before = fs::read_to_string(&changeset).unwrap();

        let mutations = [
            "DELETE FROM story WHERE id='US-SQL-SENTINEL';",
            "UPDATE story SET title='mutated' WHERE id='US-SQL-SENTINEL' RETURNING id;",
            "WITH doomed(id) AS (
                 SELECT id FROM story WHERE id='US-SQL-SENTINEL'
             )
             DELETE FROM story WHERE id IN (SELECT id FROM doomed) RETURNING id;",
            "PRAGMA user_version = 999;",
            "CREATE TEMP TABLE query_sql_temp(value TEXT);",
        ];

        for sql in mutations {
            assert_denied(repository.query_sql(sql).unwrap_err(), sql);
            let connection = repository.open_existing().unwrap();
            let unchanged: (String, i64) = connection
                .query_row(
                    "SELECT title, (SELECT user_version FROM pragma_user_version)
                     FROM story WHERE id='US-SQL-SENTINEL';",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(unchanged, ("Mutation sentinel".to_owned(), 0));
            assert_eq!(
                fs::read_to_string(&changeset).unwrap(),
                operation_log_before
            );
        }

        for sql in [
            "PRAGMA query_only=OFF;",
            "PRAGMA query_only=OFF;
             DELETE FROM story WHERE id='US-SQL-SENTINEL';",
            "PRAGMA journal_mode=DELETE;",
            "PRAGMA wal_checkpoint(TRUNCATE);",
        ] {
            assert_denied(repository.query_sql(sql).unwrap_err(), sql);
        }
        assert_eq!(fs::read_to_string(changeset).unwrap(), operation_log_before);
        assert_eq!(
            repository
                .query_sql("SELECT title FROM story WHERE id='US-SQL-SENTINEL';")
                .unwrap()
                .rows,
            [["Mutation sentinel".to_owned()]]
        );
    }

    #[test]
    fn query_sql_cannot_attach_or_checkpoint_external_state() {
        fn assert_denied(error: HarnessInfraError, sql: &str) {
            assert!(
                matches!(error, HarnessInfraError::QuerySqlWriteDenied),
                "expected read-only denial for {sql}, got {error}"
            );
        }

        let (temp, repository) = isolated_test_repository();
        repository.init().unwrap();

        let missing = temp.path().join("query-sql-created.db");
        let missing_sql = format!(
            "ATTACH DATABASE '{}' AS external;",
            missing.to_string_lossy().replace('\'', "''")
        );
        assert_denied(
            repository.query_sql(&missing_sql).unwrap_err(),
            &missing_sql,
        );
        assert!(!missing.exists());

        let existing = temp.path().join("query-sql-existing.db");
        let external = Connection::open(&existing).unwrap();
        external
            .execute_batch(
                "CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES('safe');",
            )
            .unwrap();
        drop(external);
        let external_before = fs::read(&existing).unwrap();
        for sql in [
            format!(
                "ATTACH DATABASE '{}' AS external;",
                existing.to_string_lossy().replace('\'', "''")
            ),
            format!(
                "ATTACH DATABASE '{}' AS external; DELETE FROM external.sentinel;",
                existing.to_string_lossy().replace('\'', "''")
            ),
        ] {
            assert_denied(repository.query_sql(&sql).unwrap_err(), &sql);
            assert_eq!(fs::read(&existing).unwrap(), external_before);
        }

        let writer = repository.open_existing().unwrap();
        assert_eq!(
            writer
                .query_row("PRAGMA journal_mode=WAL;", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        writer
            .execute(
                "INSERT INTO story (id,title,risk_lane) VALUES ('US-SQL-WAL','WAL sentinel','normal');",
                [],
            )
            .unwrap();
        let wal_path = repository.db_path.with_extension("db-wal");
        let database_before = fs::read(&repository.db_path).unwrap();
        let wal_before = fs::read(&wal_path).unwrap();
        for sql in [
            "PRAGMA wal_checkpoint(TRUNCATE);",
            "PRAGMA wal_checkpoint(TRUNCATE); SELECT 1;",
            "PRAGMA journal_mode=DELETE;",
        ] {
            assert_denied(repository.query_sql(sql).unwrap_err(), sql);
            assert_eq!(fs::read(&repository.db_path).unwrap(), database_before);
            assert_eq!(fs::read(&wal_path).unwrap(), wal_before);
        }
        assert_eq!(
            repository.query_sql("PRAGMA journal_mode;").unwrap().rows,
            [["wal".to_owned()]]
        );
        assert_eq!(fs::read(&repository.db_path).unwrap(), database_before);
        assert_eq!(fs::read(&wal_path).unwrap(), wal_before);
        drop(writer);
    }

    #[test]
    fn protocol_discovery_is_non_mutating() {
        let (_temp, repository) = isolated_test_repository();
        let missing = repository.discover_contract().unwrap();
        assert_eq!(missing.database_state, ContractDatabaseState::Missing);
        assert!(!repository.db_path.exists());
        let connection = Connection::open(&repository.db_path).unwrap();
        repository.apply_schema_v1(&connection).unwrap();
        drop(connection);
        let old = repository.discover_contract().unwrap();
        assert_eq!(old.database_state, ContractDatabaseState::NeedsMigration);
        assert_eq!(old.database_schema_version, Some(1));
        let connection = Connection::open(&repository.db_path).unwrap();
        assert_eq!(
            SqliteHarnessRepository::schema_version(&connection).unwrap(),
            1
        );
    }

    #[test]
    fn graph_cas_and_hierarchy_are_cycle_safe() {
        let (_temp, repository) = isolated_test_repository();
        repository.init().unwrap();
        for id in ["US-A", "US-B", "US-C"] {
            repository
                .add_story(StoryAddInput {
                    id: id.into(),
                    title: id.into(),
                    risk_lane: RiskLane::Normal,
                    contract_doc: None,
                    verify_command: Some("true".into()),
                    notes: None,
                })
                .unwrap();
        }
        repository
            .add_story_dependency(StoryDependencyInput {
                blocker: "US-A".into(),
                blocked: "US-B".into(),
            })
            .unwrap();
        repository
            .add_story_hierarchy(StoryHierarchyInput {
                parent: "US-A".into(),
                child: "US-B".into(),
            })
            .unwrap();
        repository
            .add_story_hierarchy(StoryHierarchyInput {
                parent: "US-B".into(),
                child: "US-C".into(),
            })
            .unwrap();
        assert!(matches!(
            repository
                .add_story_hierarchy(StoryHierarchyInput {
                    parent: "US-C".into(),
                    child: "US-A".into()
                })
                .unwrap_err(),
            HarnessInfraError::StoryHierarchyCycle(_, _)
        ));
        let first = repository.query_work_graph().unwrap();
        assert_eq!(first, repository.query_work_graph().unwrap());
        assert!(
            first
                .stories
                .iter()
                .find(|story| story.id == "US-A")
                .unwrap()
                .runnable
        );
        assert!(
            !first
                .stories
                .iter()
                .find(|story| story.id == "US-B")
                .unwrap()
                .runnable
        );
        assert!(matches!(
            repository
                .update_story_cas(StoryCasUpdateInput {
                    id: "US-B".into(),
                    status: "retired".into(),
                    expected_status: "planned".into(),
                    require_runnable: true
                })
                .unwrap_err(),
            HarnessInfraError::StoryNotRunnable(_)
        ));
        let changed = repository
            .update_story_cas(StoryCasUpdateInput {
                id: "US-A".into(),
                status: "in_progress".into(),
                expected_status: "planned".into(),
                require_runnable: true,
            })
            .unwrap();
        assert_eq!(
            (
                changed.before_status.as_str(),
                changed.after_status.as_str()
            ),
            ("planned", "in_progress")
        );
        assert!(matches!(
            repository
                .update_story_cas(StoryCasUpdateInput {
                    id: "US-A".into(),
                    status: "retired".into(),
                    expected_status: "planned".into(),
                    require_runnable: false
                })
                .unwrap_err(),
            HarnessInfraError::StoryStatusConflict { .. }
        ));
    }

    #[test]
    fn changeset_id_is_bound_to_exact_content_sha() {
        let (temp, repository) = isolated_test_repository();
        repository.init().unwrap();
        let path = temp.path().join("identity.changeset.jsonl");
        fs::write(&path, "{\"op\":\"changeset.header\",\"version\":1,\"run_id\":\"run_identity\",\"base_schema_version\":13}\n{\"op\":\"story.add\",\"version\":1,\"id\":\"US-IDENTITY\",\"payload\":{\"title\":\"Identity\",\"risk_lane\":\"normal\",\"contract_doc\":null,\"verify_command\":\"true\",\"notes\":null}}\n").unwrap();
        let status = repository.changeset_status(&path).unwrap();
        assert!(!status.applied);
        let applied = repository.apply_changeset(&path).unwrap();
        assert_eq!(applied.content_sha256, status.content_sha256);
        assert!(!repository.apply_changeset(&path).unwrap().applied);
        fs::write(&path, "{\"op\":\"changeset.header\",\"version\":1,\"run_id\":\"run_identity\",\"base_schema_version\":13}\n").unwrap();
        assert!(matches!(
            repository.changeset_status(&path).unwrap_err(),
            HarnessInfraError::ChangesetIdentityConflict { .. }
        ));
    }

    #[test]
    fn snapshot_contains_committed_wal_state() {
        let (temp, repository) = isolated_test_repository();
        repository.init().unwrap();
        let writer = repository.open_existing().unwrap();
        writer.execute("INSERT INTO story (id,title,risk_lane,verify_command) VALUES ('US-WAL','WAL','normal','true');", []).unwrap();
        assert!(repository.db_path.with_extension("db-wal").exists());
        let output = temp.path().join("snapshot.db");
        let result = repository.snapshot_db(&output).unwrap();
        let snapshot = Connection::open(&result.output).unwrap();
        assert_eq!(
            snapshot
                .query_row("SELECT COUNT(*) FROM story WHERE id='US-WAL';", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        assert_eq!(
            logical_database_sha256(&snapshot).unwrap(),
            result.source_logical_sha256
        );
        assert_eq!(
            sha256_bytes(&fs::read(&result.output).unwrap()),
            result.snapshot_file_sha256
        );
    }

    #[test]
    fn rollback_changeset_append_truncates_to_original_len() {
        // Reproduces the rollback path that fires when a transaction commits
        // fail after the changeset has been buffered but before it is durable.
        // The helper must restore the file to exactly its `original_len` and
        // leave the pre-existing bytes intact — otherwise a partially-rolled
        // back changeset could leave half-written lines that the replay path
        // later treats as committed operations.
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("rollback.changeset.jsonl");
        let pre_existing = "{\"op\":\"changeset.header\",\"version\":1,\"run_id\":\"run_prior\",\"base_schema_version\":13}\n";
        fs::write(&path, pre_existing).unwrap();
        let original_len = pre_existing.len() as u64;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        file.lock_exclusive().unwrap();
        // Simulate the operations that would have been buffered before the
        // commit failed. Bytes past `original_len` are what the rollback must
        // discard.
        let pending = "{\"op\":\"story.add\",\"version\":1,\"id\":\"US-ROLL\",\"payload\":{\"title\":\"Roll\",\"risk_lane\":\"normal\",\"contract_doc\":null,\"verify_command\":\"true\",\"notes\":null}}\n";
        writeln!(file, "{}", pending).unwrap();
        file.flush().unwrap();

        let mut append = ChangesetAppend {
            path: path.clone(),
            original_len,
            file: Some(file),
        };

        rollback_changeset_append(&mut append).unwrap();

        let after = fs::read(&path).unwrap();
        assert_eq!(
            after.len() as u64,
            original_len,
            "rollback must restore original length"
        );
        assert_eq!(
            std::str::from_utf8(&after).unwrap(),
            pre_existing,
            "rollback must leave pre-existing bytes intact",
        );
        // The locked descriptor is still held so a concurrent writer cannot
        // have raced between the truncation and the read above.
        assert!(append.file.is_some());
    }

    #[test]
    fn rollback_changeset_append_reacquires_lock_when_handle_missing() {
        // The defensive `file: None` branch is unreachable from production
        // code today (see the struct invariant) but exists so the helper can
        // still truncate if a future call site constructs a ChangesetAppend
        // without keeping the locked descriptor. This test pins that
        // behaviour: even when the helper reopens the file, it must hold an
        // exclusive lock for the entire rollback window and leave only the
        // pre-existing bytes behind.
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("rollback-no-handle.changeset.jsonl");
        let pre_existing = "{\"op\":\"changeset.header\",\"version\":1,\"run_id\":\"run_prior\",\"base_schema_version\":13}\n";
        fs::write(&path, pre_existing).unwrap();
        let original_len = pre_existing.len() as u64;

        // Drop the locked descriptor so the helper has to reopen.
        let mut append = ChangesetAppend {
            path: path.clone(),
            original_len,
            file: None,
        };
        // Stage bytes past the rollback boundary that the helper must
        // discard.
        let mut stage = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(
            stage,
            "{}",
            "{\"op\":\"story.add\",\"version\":1,\"id\":\"US-STAGE\",\"payload\":{\"title\":\"Stage\",\"risk_lane\":\"normal\",\"contract_doc\":null,\"verify_command\":\"true\",\"notes\":null}}"
        )
        .unwrap();
        stage.flush().unwrap();
        drop(stage);

        rollback_changeset_append(&mut append).unwrap();

        let after = fs::read(&path).unwrap();
        assert_eq!(after.len() as u64, original_len);
        assert_eq!(std::str::from_utf8(&after).unwrap(), pre_existing);
    }
}
