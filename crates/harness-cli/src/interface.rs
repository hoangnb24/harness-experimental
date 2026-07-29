use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand};
use thiserror::Error;

use crate::epoch_fence::{acquire_command_guard, EpochFenceError};

use crate::application::{
    BacklogAddInput, BacklogCloseInput, BacklogOutcomeInput, BrownfieldImportResult,
    ChangesetApplyResult, DbRebuildResult, DecisionAddInput, HarnessContext, HarnessService,
    ImprovementHealthResult, InitResult, IntakeInput, InterventionAddInput, InterventionFilter,
    LegacyReconcileResult, MigrateResult, QueryTable, StoryAddInput, StoryBacklogLinkInput,
    StoryCasUpdateInput, StoryDependencyInput, StoryHierarchyInput, StoryUpdateInput,
    ToolRegisterInput, TraceInput,
};
use crate::domain::{
    normalize_capability, parse_optional_integer, parse_tool_args, proof_display,
    validate_responsibility, validate_tool_kind, BacklogFilter, BacklogRecord, BoolFlag,
    ContextScoreResult, CsvList, DecisionRecord, FrictionRecord, HarnessStats, ImprovementProposal,
    InputType, IntakeRecord, InterventionRecord, RiskLane, StoryMatrixRecord, StoryVerifyAllResult,
    ToolEntry, TraceQualityTier, TraceRecord, TraceScoreResult, RISK_LANE_HELP,
};
use crate::infrastructure::{ProposalDecision, ProposalResult, ToolCheckResult};

#[derive(Parser, Debug)]
#[command(name = "harness-cli")]
#[command(about = "durable layer for the project harness", long_about = None)]
#[command(version)]
pub struct Cli {
    /// Permit an explicitly selected legacy lifecycle write to this source
    /// repository's default compatibility database.
    #[arg(long, global = true)]
    compatibility_write: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create the harness database if it does not already exist.
    Init,
    /// Apply schema migrations.
    Migrate,
    /// Seed or refresh the database from existing markdown state.
    Import(ImportArgs),
    /// Record a feature intake classification.
    Intake(IntakeArgs),
    /// Add or update a story.
    Story(StoryArgs),
    /// Add a decision or run its verification.
    Decision(DecisionArgs),
    /// Add or close a backlog item.
    Backlog(BacklogArgs),
    /// Register or remove external tools.
    Tool(ToolArgs),
    /// Record a human, review, CI, or agent intervention.
    Intervention(InterventionArgs),
    /// Record an agent execution trace.
    Trace(TraceArgs),
    /// Score a trace against the trace quality tiers.
    ScoreTrace(ScoreTraceArgs),
    /// Score trace context reads against CONTEXT_RULES.md.
    ScoreContext { trace_id: String },
    /// Run drift audit and entropy score.
    Audit(AuditArgs),
    /// Generate improvement proposals from observed patterns.
    Propose(ProposeArgs),
    /// Manage harness database changesets.
    Db(DbArgs),
    /// Query harness data.
    Query(QueryArgs),
}

#[derive(Args, Debug)]
struct AuditArgs {
    /// Explicitly persist audit evidence episode transitions.
    #[arg(long)]
    record_evidence: bool,
}

#[derive(Args, Debug)]
#[command(after_help = RISK_LANE_HELP)]
struct IntakeArgs {
    #[arg(long = "type")]
    input_type: String,
    #[arg(long)]
    summary: String,
    #[arg(long, value_name = "tiny|normal|high-risk")]
    lane: String,
    #[arg(long)]
    flags: Option<String>,
    #[arg(long)]
    docs: Option<String>,
    #[arg(long)]
    story: Option<String>,
    #[arg(long)]
    notes: Option<String>,
}

#[derive(Args, Debug)]
struct ImportArgs {
    #[command(subcommand)]
    source: ImportSource,
}

#[derive(Subcommand, Debug)]
enum ImportSource {
    /// Import TEST_MATRIX, decisions, and backlog markdown.
    Brownfield,
}

#[derive(Args, Debug)]
struct StoryArgs {
    #[command(subcommand)]
    action: StoryAction,
}

#[derive(Subcommand, Debug)]
enum StoryAction {
    #[command(after_help = RISK_LANE_HELP)]
    Add(StoryAddArgs),
    #[command(
        after_help = "Proof flags use numeric booleans: --unit 1 --integration 1 --e2e 0 --platform 0. Do not use yes/no. Status 'implemented' is completion-only; move active work to in_progress or changed, then run story complete <id>."
    )]
    Update(StoryUpdateArgs),
    /// Add or remove a dependency edge where blocker -> blocked.
    Dependency(StoryDependencyArgs),
    /// Add or remove a hierarchy edge where parent -> child.
    Hierarchy(StoryHierarchyArgs),
    /// Link a story to a stable Harness backlog occurrence.
    Backlog(StoryBacklogArgs),
    #[command(
        after_help = "story verify only accepts the story id. Configure proof with story add/update --verify, then record proof flags with story update."
    )]
    Verify {
        /// Story id to verify.
        id: String,
    },
    /// Run fresh proof and atomically mark a completion-eligible story implemented.
    Complete {
        /// Story id to complete.
        id: String,
        /// Emit the protocol-v1 machine envelope.
        #[arg(long)]
        json: bool,
    },
    /// Verify every story, skipping stories without verify_command.
    VerifyAll,
}

#[derive(Args, Debug)]
struct StoryDependencyArgs {
    #[command(subcommand)]
    action: StoryDependencyAction,
}

#[derive(Subcommand, Debug)]
enum StoryDependencyAction {
    /// Add a cycle-safe dependency edge.
    Add(StoryDependencyMutationArgs),
    /// Remove a dependency edge; a missing edge is unchanged.
    Remove(StoryDependencyMutationArgs),
}

#[derive(Args, Debug)]
struct StoryBacklogArgs {
    #[command(subcommand)]
    action: StoryBacklogAction,
}

#[derive(Subcommand, Debug)]
enum StoryBacklogAction {
    Link(StoryBacklogLinkArgs),
    Unlink(StoryBacklogUnlinkArgs),
    List(StoryBacklogListArgs),
}

#[derive(Args, Debug)]
struct StoryBacklogLinkArgs {
    #[arg(long)]
    story: String,
    #[arg(long)]
    backlog: String,
    #[arg(long, value_parser = ["resolves", "references"])]
    relationship: String,
}

#[derive(Args, Debug)]
struct StoryBacklogUnlinkArgs {
    #[arg(long)]
    story: String,
    #[arg(long)]
    backlog: String,
}

#[derive(Args, Debug)]
struct StoryBacklogListArgs {
    #[arg(long)]
    story: Option<String>,
    #[arg(long)]
    backlog: Option<String>,
}

#[derive(Args, Debug)]
struct StoryDependencyMutationArgs {
    /// The story that must complete first.
    #[arg(long)]
    blocker: String,
    /// The story blocked by --blocker.
    #[arg(long)]
    blocked: String,
    /// Emit the protocol-v1 machine envelope.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct StoryHierarchyArgs {
    #[command(subcommand)]
    action: StoryHierarchyAction,
}

#[derive(Subcommand, Debug)]
enum StoryHierarchyAction {
    /// Add a cycle-safe parent/child edge.
    Add(StoryHierarchyMutationArgs),
    /// Remove a parent/child edge; a missing edge is unchanged.
    Remove(StoryHierarchyMutationArgs),
}

#[derive(Args, Debug)]
struct StoryHierarchyMutationArgs {
    #[arg(long)]
    parent: String,
    #[arg(long)]
    child: String,
    /// Emit the protocol-v1 machine envelope.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct StoryAddArgs {
    #[arg(long)]
    id: String,
    #[arg(long)]
    title: String,
    #[arg(long, value_name = "tiny|normal|high-risk")]
    lane: String,
    #[arg(long)]
    contract: Option<String>,
    #[arg(long)]
    verify: Option<String>,
    #[arg(long)]
    notes: Option<String>,
    /// Emit the protocol-v1 machine envelope.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct StoryUpdateArgs {
    #[arg(long)]
    id: String,
    #[arg(long)]
    contract: Option<String>,
    #[arg(long)]
    /// Set a non-completion lifecycle status. Use `story complete` for implemented.
    status: Option<String>,
    #[arg(long)]
    evidence: Option<String>,
    #[arg(long, value_name = "0|1")]
    unit: Option<String>,
    #[arg(long, value_name = "0|1")]
    integration: Option<String>,
    #[arg(long, value_name = "0|1")]
    e2e: Option<String>,
    #[arg(long, value_name = "0|1")]
    platform: Option<String>,
    #[arg(long)]
    verify: Option<String>,
    /// Compare-and-set precondition checked in the write transaction.
    #[arg(long)]
    expected_status: Option<String>,
    /// Require generic runnable state in the same write transaction.
    #[arg(long)]
    require_runnable: bool,
    /// Emit the protocol-v1 machine envelope.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct DecisionArgs {
    #[command(subcommand)]
    action: DecisionAction,
}

#[derive(Subcommand, Debug)]
enum DecisionAction {
    Add(DecisionAddArgs),
    Verify { id: String },
}

#[derive(Args, Debug)]
struct DecisionAddArgs {
    #[arg(long)]
    id: String,
    #[arg(long)]
    title: String,
    #[arg(long, default_value = "accepted")]
    status: String,
    #[arg(long)]
    doc: Option<String>,
    #[arg(long)]
    verify: Option<String>,
    #[arg(long)]
    predicted: Option<String>,
    #[arg(long)]
    notes: Option<String>,
}

#[derive(Args, Debug)]
struct BacklogArgs {
    #[command(subcommand)]
    action: BacklogAction,
}

#[derive(Subcommand, Debug)]
enum BacklogAction {
    #[command(after_help = RISK_LANE_HELP)]
    Add(BacklogAddArgs),
    Close(BacklogCloseArgs),
    /// Append a measured outcome for an implemented improvement occurrence.
    Outcome(BacklogOutcomeArgs),
    /// Preview or apply conservative legacy lifecycle identity backfill.
    Reconcile(BacklogReconcileArgs),
}

#[derive(Args, Debug)]
struct BacklogReconcileArgs {
    #[arg(long, value_parser = ["backfill-lifecycle-identity"])]
    action: String,
    #[arg(long, conflicts_with = "apply")]
    dry_run: bool,
    #[arg(long, conflicts_with = "dry_run")]
    apply: bool,
}

#[derive(Args, Debug)]
struct BacklogOutcomeArgs {
    #[command(subcommand)]
    action: BacklogOutcomeAction,
}

#[derive(Subcommand, Debug)]
enum BacklogOutcomeAction {
    Record(BacklogOutcomeRecordArgs),
}

#[derive(Args, Debug)]
struct BacklogOutcomeRecordArgs {
    #[arg(long)]
    id: String,
    #[arg(long, value_parser = ["confirmed", "ineffective", "reverted"])]
    status: String,
    #[arg(long)]
    outcome: String,
    #[arg(long)]
    evidence: Option<String>,
}

#[derive(Args, Debug)]
struct BacklogAddArgs {
    #[arg(long)]
    title: String,
    #[arg(long = "while")]
    discovered_while: Option<String>,
    #[arg(long)]
    pain: Option<String>,
    #[arg(long)]
    suggestion: Option<String>,
    #[arg(long, value_name = "tiny|normal|high-risk")]
    risk: Option<String>,
    #[arg(long)]
    predicted: Option<String>,
    #[arg(long)]
    notes: Option<String>,
}

#[derive(Args, Debug)]
struct BacklogCloseArgs {
    #[arg(long)]
    id: String,
    #[arg(long, default_value = "implemented")]
    status: String,
    #[arg(long)]
    outcome: Option<String>,
}

#[derive(Args, Debug)]
struct ToolArgs {
    #[command(subcommand)]
    action: ToolAction,
}

#[derive(Subcommand, Debug)]
enum ToolAction {
    Register(ToolRegisterArgs),
    /// Scan registered tools and persist present/missing/unknown status.
    Check(ToolCheckArgs),
    Remove {
        #[arg(long)]
        name: String,
    },
}

#[derive(Args, Debug)]
struct ToolRegisterArgs {
    #[arg(long)]
    name: String,
    #[arg(long)]
    command: String,
    #[arg(long)]
    description: String,
    #[arg(long)]
    responsibility: String,
    #[arg(long)]
    args: Option<String>,
    #[arg(long)]
    force: bool,
    /// How the tool is reached and probed: cli, binary, mcp, skill, http.
    #[arg(long, default_value = "cli")]
    kind: String,
    /// Workflow purpose a step looks the tool up by (kebab-case).
    #[arg(long)]
    capability: Option<String>,
    /// Declarative path/URL `tool check` resolves to decide presence.
    #[arg(long)]
    scan: Option<String>,
}

#[derive(Args, Debug)]
struct ToolCheckArgs {
    /// Check one tool by name; omit to check every registered tool.
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct InterventionArgs {
    #[command(subcommand)]
    action: InterventionAction,
}

#[derive(Subcommand, Debug)]
enum InterventionAction {
    Add(InterventionAddArgs),
}

#[derive(Args, Debug)]
struct InterventionAddArgs {
    #[arg(long)]
    trace: Option<String>,
    #[arg(long)]
    story: Option<String>,
    #[arg(long = "type")]
    intervention_type: String,
    #[arg(long)]
    description: String,
    #[arg(long)]
    source: String,
    #[arg(long)]
    impact: Option<String>,
}

#[derive(Args, Debug)]
struct TraceArgs {
    #[arg(long)]
    summary: String,
    #[arg(long)]
    intake: Option<String>,
    #[arg(long)]
    story: Option<String>,
    #[arg(long)]
    agent: Option<String>,
    #[arg(long)]
    outcome: Option<String>,
    #[arg(long)]
    duration: Option<String>,
    #[arg(long)]
    tokens: Option<String>,
    #[arg(long)]
    friction: Option<String>,
    #[arg(long)]
    actions: Option<String>,
    #[arg(long = "read")]
    files_read: Option<String>,
    #[arg(long = "changed")]
    files_changed: Option<String>,
    #[arg(long)]
    decisions: Option<String>,
    #[arg(long)]
    errors: Option<String>,
    #[arg(long)]
    notes: Option<String>,
}

#[derive(Args, Debug)]
struct ScoreTraceArgs {
    /// Score a specific trace id. Defaults to the latest trace.
    #[arg(long)]
    id: Option<String>,
}

#[derive(Args, Debug)]
struct ProposeArgs {
    /// Removed bulk writer. Select one key with --accept or --reject instead.
    #[arg(long)]
    commit: bool,
    #[arg(long, conflicts_with = "reject")]
    accept: Option<String>,
    #[arg(long, conflicts_with = "accept")]
    reject: Option<String>,
    #[arg(long)]
    outcome_manual: bool,
    #[arg(long)]
    outcome_due: Option<String>,
    #[arg(long)]
    outcome_after_traces: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    /// Include handled occurrences whose current evidence is fully covered.
    #[arg(long, conflicts_with_all = ["accept", "reject"])]
    show_suppressed: bool,
}

#[derive(Args, Debug)]
struct DbArgs {
    #[command(subcommand)]
    action: DbAction,
}

#[derive(Subcommand, Debug)]
enum DbAction {
    Changeset(ChangesetArgs),
    /// Create an atomic SQLite online-backup snapshot.
    Snapshot(SnapshotArgs),
    /// Rebuild a fresh harness database from committed changesets.
    Rebuild {
        #[arg(long = "from")]
        from: PathBuf,
    },
}

#[derive(Args, Debug)]
struct ChangesetArgs {
    #[command(subcommand)]
    action: ChangesetAction,
}

#[derive(Subcommand, Debug)]
enum ChangesetAction {
    /// Apply one semantic changeset file idempotently.
    Apply {
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Inspect one semantic changeset without changing database state.
    Status {
        path: PathBuf,
        #[arg(long, action = clap::ArgAction::SetTrue, required = true)]
        json: bool,
    },
}

#[derive(Args, Debug)]
struct SnapshotArgs {
    #[arg(long)]
    output: PathBuf,
    #[arg(long, action = clap::ArgAction::SetTrue, required = true)]
    json: bool,
}

#[derive(Args, Debug)]
struct QueryArgs {
    #[command(subcommand)]
    view: QueryView,
}

#[derive(Args, Debug, Default)]
struct MatrixQueryArgs {
    /// Render proof flags as CLI input values, 1 and 0, instead of yes and no.
    #[arg(long)]
    numeric: bool,
    /// Show only unfinished stories: planned, in_progress, or changed.
    #[arg(long)]
    active: bool,
    /// Show only stories currently runnable under the protocol-v1 rule.
    #[arg(long)]
    runnable: bool,
    /// Show only one exact story ID. Combines with the other filters.
    #[arg(long, value_name = "ID")]
    story: Option<String>,
    /// Omit the potentially long evidence column.
    #[arg(long)]
    summary: bool,
}

#[derive(Args, Debug)]
struct BacklogQueryArgs {
    /// Show only proposed and accepted backlog items.
    #[arg(long, conflicts_with = "closed")]
    open: bool,
    /// Show only implemented and rejected backlog items.
    #[arg(long)]
    closed: bool,
    /// Also render relationships for one backlog occurrence.
    #[arg(long)]
    id: Option<String>,
}

#[derive(Subcommand, Debug)]
enum QueryView {
    /// Discover protocol capabilities and database compatibility without writes.
    Contract(MachineQueryArgs),
    /// Read stable story records.
    Stories(MachineQueryArgs),
    /// Read stories, dependencies, and hierarchy from one database revision.
    WorkGraph(MachineQueryArgs),
    /// Test matrix.
    Matrix(MatrixQueryArgs),
    /// Story dependency edges, optionally filtered to one story.
    Dependencies(DependenciesQueryArgs),
    /// Story hierarchy edges, optionally filtered to one story.
    Hierarchy(HierarchyQueryArgs),
    /// Harness improvement proposals.
    Backlog(BacklogQueryArgs),
    /// Decision records.
    Decisions,
    /// Recent intake classifications.
    Intakes,
    /// Recent traces.
    Traces,
    /// Traces with harness friction.
    Friction,
    /// Machine-readable and registered tool manifest.
    Tools(ToolsQueryArgs),
    /// Intervention records.
    Interventions(InterventionsQueryArgs),
    /// Summary counts.
    Stats,
    /// Read-only daily view of proposal, implementation, outcome, and recurrence health.
    ImprovementHealth,
    /// Run one read-only SQL statement.
    Sql { query: Vec<String> },
}

#[derive(Args, Debug)]
struct DependenciesQueryArgs {
    /// Show edges where this story is either the blocker or blocked story.
    #[arg(long)]
    story: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct HierarchyQueryArgs {
    #[arg(long)]
    story: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
struct MachineQueryArgs {
    #[arg(long, action = clap::ArgAction::SetTrue, required = true)]
    json: bool,
}

#[derive(Args, Debug)]
struct ToolsQueryArgs {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    summary: bool,
    #[arg(long)]
    responsibility: Option<String>,
    /// Filter to tools that provide this capability.
    #[arg(long)]
    capability: Option<String>,
    /// Filter to tools with this scanned status: present, missing, unknown.
    #[arg(long)]
    status: Option<String>,
}

#[derive(Args, Debug)]
struct InterventionsQueryArgs {
    #[arg(long)]
    trace: Option<String>,
    #[arg(long)]
    story: Option<String>,
    #[arg(long = "type")]
    intervention_type: Option<String>,
}

#[derive(Debug, Error)]
pub enum InterfaceError {
    #[error("{0}")]
    InvalidArgument(String),
    #[error("{0}")]
    ParseHarnessValue(#[from] crate::domain::ParseHarnessValueError),
    #[error("{0}")]
    ToolValidation(#[from] crate::domain::ToolValidationError),
    #[error("{0}")]
    Infrastructure(#[from] crate::infrastructure::HarnessInfraError),
    #[error("could not determine current directory: {0}")]
    CurrentDir(std::io::Error),
    #[error("query sql requires a SQL statement")]
    EmptySql,
    #[error("machine output exceeds the 16 MiB protocol limit")]
    MachineOutputTooLarge,
    #[error("json serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("stdout write failed: {0}")]
    Output(std::io::Error),
    #[error("path cannot be represented as UTF-8 for protocol JSON")]
    NonUtf8Path,
    #[error(
        "upstream SQLite lifecycle write '{operation}' is frozen for new repository work; use Git-native plans and decisions for current authority. Rerun with --compatibility-write only when maintaining preserved compatibility state"
    )]
    LegacyLifecycleWriteFrozen { operation: &'static str },
    #[error("{0}")]
    EpochFence(#[from] EpochFenceError),
}

const PROTOCOL_VERSION: u32 = 1;
const MAX_MACHINE_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

impl Cli {
    fn mutates_state(&self) -> bool {
        match &self.command {
            Command::Init
            | Command::Migrate
            | Command::Import(_)
            | Command::Intake(_)
            | Command::Intervention(_)
            | Command::Trace(_) => true,
            Command::Story(args) => !matches!(
                &args.action,
                StoryAction::Backlog(StoryBacklogArgs {
                    action: StoryBacklogAction::List(_)
                })
            ),
            Command::Decision(_) | Command::Tool(_) => true,
            Command::Backlog(args) => !matches!(
                &args.action,
                BacklogAction::Reconcile(BacklogReconcileArgs { apply: false, .. })
            ),
            Command::Audit(args) => args.record_evidence,
            Command::Propose(args) => args.commit || args.accept.is_some() || args.reject.is_some(),
            Command::Db(args) => matches!(
                &args.action,
                DbAction::Changeset(ChangesetArgs {
                    action: ChangesetAction::Apply { .. }
                }) | DbAction::Rebuild { .. }
            ),
            Command::ScoreTrace(_) | Command::ScoreContext { .. } | Command::Query(_) => false,
        }
    }

    fn legacy_lifecycle_write(&self) -> Option<&'static str> {
        if self.machine_mode() || !self.mutates_state() {
            return None;
        }

        match &self.command {
            Command::Import(_) => Some("import brownfield"),
            Command::Intake(_) => Some("intake"),
            Command::Story(_) => Some("story"),
            Command::Decision(_) => Some("decision"),
            Command::Backlog(_) => Some("backlog"),
            Command::Tool(_) => Some("tool"),
            Command::Intervention(_) => Some("intervention"),
            Command::Trace(_) => Some("trace"),
            Command::Audit(_) => Some("audit --record-evidence"),
            Command::Propose(_) => Some("propose decision"),
            Command::Init
            | Command::Migrate
            | Command::ScoreTrace(_)
            | Command::ScoreContext { .. }
            | Command::Db(_)
            | Command::Query(_) => None,
        }
    }

    pub fn machine_mode(&self) -> bool {
        match &self.command {
            Command::Story(args) => match &args.action {
                StoryAction::Add(args) => args.json,
                StoryAction::Update(args) => args.json,
                StoryAction::Dependency(args) => match &args.action {
                    StoryDependencyAction::Add(args) | StoryDependencyAction::Remove(args) => {
                        args.json
                    }
                },
                StoryAction::Hierarchy(args) => match &args.action {
                    StoryHierarchyAction::Add(args) | StoryHierarchyAction::Remove(args) => {
                        args.json
                    }
                },
                StoryAction::Complete { json, .. } => *json,
                _ => false,
            },
            Command::Db(args) => match &args.action {
                DbAction::Snapshot(args) => args.json,
                DbAction::Changeset(args) => match &args.action {
                    ChangesetAction::Apply { json, .. } | ChangesetAction::Status { json, .. } => {
                        *json
                    }
                },
                DbAction::Rebuild { .. } => false,
            },
            Command::Query(args) => match &args.view {
                QueryView::Contract(args)
                | QueryView::Stories(args)
                | QueryView::WorkGraph(args) => args.json,
                QueryView::Dependencies(args) => args.json,
                QueryView::Hierarchy(args) => args.json,
                _ => false,
            },
            _ => false,
        }
    }

    pub fn machine_operation(&self) -> &'static str {
        match &self.command {
            Command::Story(args) => match &args.action {
                StoryAction::Add(_) => "story.add",
                StoryAction::Update(_) => "story.update",
                StoryAction::Dependency(args) => match &args.action {
                    StoryDependencyAction::Add(_) => "story.dependency.add",
                    StoryDependencyAction::Remove(_) => "story.dependency.remove",
                },
                StoryAction::Hierarchy(args) => match &args.action {
                    StoryHierarchyAction::Add(_) => "story.hierarchy.add",
                    StoryHierarchyAction::Remove(_) => "story.hierarchy.remove",
                },
                StoryAction::Complete { .. } => "story.complete",
                _ => "story",
            },
            Command::Db(args) => match &args.action {
                DbAction::Snapshot(_) => "db.snapshot",
                DbAction::Changeset(args) => match &args.action {
                    ChangesetAction::Apply { .. } => "db.changeset.apply",
                    ChangesetAction::Status { .. } => "db.changeset.status",
                },
                DbAction::Rebuild { .. } => "db.rebuild",
            },
            Command::Query(args) => match &args.view {
                QueryView::Contract(_) => "query.contract",
                QueryView::Stories(_) => "query.stories",
                QueryView::WorkGraph(_) => "query.work-graph",
                QueryView::Dependencies(_) => "query.dependencies",
                QueryView::Hierarchy(_) => "query.hierarchy",
                _ => "query",
            },
            _ => "harness-cli",
        }
    }
}

fn request_id() -> String {
    if let Ok(value) = env::var("HARNESS_REQUEST_ID") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.chars().take(128).collect();
        }
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("req-{}-{nanos}", std::process::id())
}

fn write_machine_document(value: &serde_json::Value) -> Result<(), InterfaceError> {
    let mut output = serde_json::to_vec(value)?;
    if output.len() + 1 > MAX_MACHINE_OUTPUT_BYTES {
        return Err(InterfaceError::MachineOutputTooLarge);
    }
    output.push(b'\n');
    std::io::stdout()
        .lock()
        .write_all(&output)
        .map_err(InterfaceError::Output)
}

fn print_machine_success(operation: &str, result: serde_json::Value) -> Result<(), InterfaceError> {
    write_machine_document(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "operation": operation,
        "request_id": request_id(),
        "result": result,
    }))
}

pub fn emit_machine_error(operation: &str, error: &InterfaceError) -> i32 {
    let (code, retryable, exit_code) = match error {
        InterfaceError::InvalidArgument(_)
        | InterfaceError::ParseHarnessValue(_)
        | InterfaceError::ToolValidation(_)
        | InterfaceError::EmptySql => ("INVALID_ARGUMENT", false, 2),
        InterfaceError::Infrastructure(
            crate::infrastructure::HarnessInfraError::StoryImplementedRequiresCompletion(_),
        ) => ("INVALID_ARGUMENT", false, 2),
        InterfaceError::Infrastructure(
            crate::infrastructure::HarnessInfraError::StoryNotFound(_)
            | crate::infrastructure::HarnessInfraError::ToolNotFound(_)
            | crate::infrastructure::HarnessInfraError::BacklogNotFound(_)
            | crate::infrastructure::HarnessInfraError::TraceNotFound(_),
        ) => ("NOT_FOUND", false, 3),
        InterfaceError::Infrastructure(
            crate::infrastructure::HarnessInfraError::MissingStoryVerifyCommand(_)
            | crate::infrastructure::HarnessInfraError::StoryCompletion(_),
        ) => ("VERIFICATION_FAILED", false, 4),
        InterfaceError::Infrastructure(
            crate::infrastructure::HarnessInfraError::StoryDependencySelf(_)
            | crate::infrastructure::HarnessInfraError::StoryDependencyCycle(_, _)
            | crate::infrastructure::HarnessInfraError::StoryHierarchySelf(_)
            | crate::infrastructure::HarnessInfraError::StoryHierarchyCycle(_, _)
            | crate::infrastructure::HarnessInfraError::StoryStatusConflict { .. }
            | crate::infrastructure::HarnessInfraError::StoryNotRunnable(_)
            | crate::infrastructure::HarnessInfraError::ChangesetIdentityConflict { .. }
            | crate::infrastructure::HarnessInfraError::EntityRevisionConflict { .. }
            | crate::infrastructure::HarnessInfraError::SnapshotOutputExists(_),
        ) => ("CONFLICT", false, 3),
        InterfaceError::Infrastructure(
            crate::infrastructure::HarnessInfraError::MissingDatabase(_)
            | crate::infrastructure::HarnessInfraError::InvalidChangeset(_),
        ) => ("COMPATIBILITY_ERROR", false, 2),
        InterfaceError::MachineOutputTooLarge => ("OUTPUT_LIMIT_EXCEEDED", false, 5),
        InterfaceError::NonUtf8Path => ("PATH_NOT_UTF8", false, 2),
        _ => ("INTERNAL_ERROR", false, 5),
    };
    let details = match error {
        InterfaceError::Infrastructure(
            crate::infrastructure::HarnessInfraError::EntityRevisionConflict {
                changeset_id,
                entity_kind,
                entity_id,
                expected_revision,
                actual_revision,
            },
        ) => serde_json::json!({
            "changeset_id": changeset_id,
            "entity_kind": entity_kind,
            "entity_id": entity_id,
            "expected_revision": expected_revision,
            "actual_revision": actual_revision,
        }),
        _ => serde_json::json!({}),
    };
    let envelope = serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "operation": operation,
        "request_id": request_id(),
        "error": {
            "code": code,
            "message": error.to_string(),
            "retryable": retryable,
            "details": details,
        },
    });
    if let Err(write_error) = write_machine_document(&envelope) {
        eprintln!("error: {write_error}");
    }
    exit_code
}

pub fn emit_parse_error(message: &str) -> i32 {
    let envelope = serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "operation": "cli.parse",
        "request_id": request_id(),
        "error": {
            "code": "INVALID_ARGUMENT",
            "message": message,
            "retryable": false,
            "details": {},
        },
    });
    if let Err(error) = write_machine_document(&envelope) {
        eprintln!("error: {error}");
    }
    2
}

pub fn run(cli: Cli) -> Result<(), InterfaceError> {
    let context = resolve_context()?;
    if let Some(notice) = enforce_control_plane_freeze(&cli, &context)? {
        eprintln!("{notice}");
    }
    let _epoch_write_guard = acquire_command_guard(&context.repo_root, cli.mutates_state())?;
    let service = HarnessService::new(context);

    match cli.command {
        Command::Init => print_init_result(service.init()?),
        Command::Migrate => print_migrate_result(service.migrate()?),
        Command::Import(args) => match args.source {
            ImportSource::Brownfield => {
                print_brownfield_import_result(service.import_brownfield()?)
            }
        },
        Command::Intake(args) => {
            let id = service.record_intake(IntakeInput {
                input_type: InputType::from_str(&args.input_type)?,
                summary: args.summary,
                risk_lane: RiskLane::from_str(&args.lane)?,
                risk_flags: CsvList::try_from_optional(args.flags)?,
                affected_docs: CsvList::try_from_optional(args.docs)?,
                story_id: args.story,
                notes: args.notes,
            })?;
            println!("Intake #{id} recorded.");
        }
        Command::Story(args) => match args.action {
            StoryAction::Add(args) => {
                service.add_story(StoryAddInput {
                    id: args.id.clone(),
                    title: args.title,
                    risk_lane: RiskLane::from_str(&args.lane)?,
                    contract_doc: args.contract,
                    verify_command: args.verify,
                    notes: args.notes,
                })?;
                if args.json {
                    let story = service
                        .query_orchestration_stories()?
                        .into_iter()
                        .find(|story| story.id == args.id)
                        .ok_or_else(|| {
                            InterfaceError::InvalidArgument(
                                "created story was not readable".to_owned(),
                            )
                        })?;
                    print_machine_success(
                        "story.add",
                        serde_json::json!({"changed": true, "story": story}),
                    )?;
                } else {
                    println!("Story {} added.", args.id);
                }
            }
            StoryAction::Update(args) => {
                if args.json {
                    if args.contract.is_some()
                        || args.evidence.is_some()
                        || args.unit.is_some()
                        || args.integration.is_some()
                        || args.e2e.is_some()
                        || args.platform.is_some()
                        || args.verify.is_some()
                    {
                        return Err(InterfaceError::InvalidArgument(
                            "story update --json is a status CAS operation and cannot combine contract/proof/evidence/verify fields".to_owned(),
                        ));
                    }
                    let status = args.status.ok_or_else(|| {
                        InterfaceError::InvalidArgument(
                            "story update --json requires --status".to_owned(),
                        )
                    })?;
                    let expected_status = args.expected_status.ok_or_else(|| {
                        InterfaceError::InvalidArgument(
                            "story update --json requires --expected-status".to_owned(),
                        )
                    })?;
                    let result = service.update_story_cas(StoryCasUpdateInput {
                        id: args.id,
                        status,
                        expected_status,
                        require_runnable: args.require_runnable,
                    })?;
                    print_machine_success("story.update", serde_json::to_value(result)?)?;
                } else {
                    if args.expected_status.is_some() || args.require_runnable {
                        return Err(InterfaceError::InvalidArgument(
                            "--expected-status and --require-runnable require --json".to_owned(),
                        ));
                    }
                    service.update_story(StoryUpdateInput {
                        id: args.id.clone(),
                        contract_doc: args.contract,
                        status: args.status,
                        evidence: args.evidence,
                        unit: parse_optional_bool("story update: --unit", args.unit)?,
                        integration: parse_optional_bool(
                            "story update: --integration",
                            args.integration,
                        )?,
                        e2e: parse_optional_bool("story update: --e2e", args.e2e)?,
                        platform: parse_optional_bool("story update: --platform", args.platform)?,
                        verify_command: args.verify,
                    })?;
                    println!("Story {} updated.", args.id);
                }
            }
            StoryAction::Dependency(args) => match args.action {
                StoryDependencyAction::Add(args) => {
                    let changed = service.add_story_dependency(StoryDependencyInput {
                        blocker: args.blocker.clone(),
                        blocked: args.blocked.clone(),
                    })?;
                    if args.json {
                        print_machine_success(
                            "story.dependency.add",
                            serde_json::json!({
                                "blocker": args.blocker, "blocked": args.blocked, "changed": changed
                            }),
                        )?;
                    } else {
                        println!(
                            "Story dependency {} -> {} {}.",
                            args.blocker,
                            args.blocked,
                            if changed { "added" } else { "unchanged" }
                        );
                    }
                }
                StoryDependencyAction::Remove(args) => {
                    let changed = service.remove_story_dependency(StoryDependencyInput {
                        blocker: args.blocker.clone(),
                        blocked: args.blocked.clone(),
                    })?;
                    if args.json {
                        print_machine_success(
                            "story.dependency.remove",
                            serde_json::json!({
                                "blocker": args.blocker, "blocked": args.blocked, "changed": changed
                            }),
                        )?;
                    } else {
                        println!(
                            "Story dependency {} -> {} {}.",
                            args.blocker,
                            args.blocked,
                            if changed { "removed" } else { "unchanged" }
                        );
                    }
                }
            },
            StoryAction::Hierarchy(args) => match args.action {
                StoryHierarchyAction::Add(args) => {
                    let changed = service.add_story_hierarchy(StoryHierarchyInput {
                        parent: args.parent.clone(),
                        child: args.child.clone(),
                    })?;
                    if args.json {
                        print_machine_success(
                            "story.hierarchy.add",
                            serde_json::json!({
                                "parent": args.parent, "child": args.child, "changed": changed
                            }),
                        )?;
                    } else {
                        println!(
                            "Story hierarchy {} -> {} {}.",
                            args.parent,
                            args.child,
                            if changed { "added" } else { "unchanged" }
                        );
                    }
                }
                StoryHierarchyAction::Remove(args) => {
                    let changed = service.remove_story_hierarchy(StoryHierarchyInput {
                        parent: args.parent.clone(),
                        child: args.child.clone(),
                    })?;
                    if args.json {
                        print_machine_success(
                            "story.hierarchy.remove",
                            serde_json::json!({
                                "parent": args.parent, "child": args.child, "changed": changed
                            }),
                        )?;
                    } else {
                        println!(
                            "Story hierarchy {} -> {} {}.",
                            args.parent,
                            args.child,
                            if changed { "removed" } else { "unchanged" }
                        );
                    }
                }
            },
            StoryAction::Backlog(args) => match args.action {
                StoryBacklogAction::Link(args) => {
                    let backlog_id = parse_optional_integer(
                        "story backlog link: --backlog",
                        Some(args.backlog),
                    )?
                    .expect("value provided");
                    let changed = service.link_story_backlog(StoryBacklogLinkInput {
                        story_id: args.story.clone(),
                        backlog_id,
                        relationship: args.relationship.clone(),
                    })?;
                    println!(
                        "Story backlog link {} -> #{} ({}) {}.",
                        args.story,
                        backlog_id,
                        args.relationship,
                        if changed { "updated" } else { "unchanged" }
                    );
                }
                StoryBacklogAction::Unlink(args) => {
                    let backlog_id = parse_optional_integer(
                        "story backlog unlink: --backlog",
                        Some(args.backlog),
                    )?
                    .expect("value provided");
                    let changed = service.unlink_story_backlog(&args.story, backlog_id)?;
                    println!(
                        "Story backlog link {} -> #{} {}.",
                        args.story,
                        backlog_id,
                        if changed { "removed" } else { "unchanged" }
                    );
                }
                StoryBacklogAction::List(args) => {
                    let backlog_id =
                        parse_optional_integer("story backlog list: --backlog", args.backlog)?;
                    print_story_backlog_links(
                        &service.query_story_backlog_links(args.story.as_deref(), backlog_id)?,
                    );
                }
            },
            StoryAction::Verify { id } => {
                let result = service.verify_story(&id)?;
                println!("Running: {}", result.command);
                print!("{}", result.stdout);
                print!("{}", result.stderr);
                println!("Story {id} verification: {}", result.result);
                if result.result == "fail" {
                    std::process::exit(1);
                }
            }
            StoryAction::Complete { id, json } => {
                let result = service.complete_story(&id)?;
                if result.result == "fail" {
                    return Err(InterfaceError::Infrastructure(
                        crate::infrastructure::HarnessInfraError::StoryCompletion(format!(
                            "verification command failed for story {id}: {}",
                            result.command
                        )),
                    ));
                }
                if json {
                    return print_machine_success(
                        "story.complete",
                        serde_json::json!({
                            "id": id, "command": result.command,
                            "stdout": result.stdout, "stderr": result.stderr,
                            "result": result.result, "intake_uid": result.intake_uid,
                            "implementation_trace_uid": result.implementation_trace_uid,
                            "closed_backlog_ids": result.closed_backlog_ids,
                            "already_closed_backlog_ids": result.already_closed_backlog_ids,
                            "referenced_backlog_ids": result.referenced_backlog_ids,
                        }),
                    );
                }
                println!("Running: {}", result.command);
                print!("{}", result.stdout);
                print!("{}", result.stderr);
                if let Some(intake_uid) = &result.intake_uid {
                    println!("Completion intake: {intake_uid}");
                }
                if let Some(trace_uid) = &result.implementation_trace_uid {
                    println!("Implementation trace: {trace_uid}");
                }
                println!(
                    "Story {id} completion: {}; closed backlog: {}; already closed: {}; references: {}",
                    result.result,
                    result
                        .closed_backlog_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    result
                        .already_closed_backlog_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    result
                        .referenced_backlog_ids
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if result.result == "fail" {
                    std::process::exit(1);
                }
            }
            StoryAction::VerifyAll => {
                let result = service.verify_all_stories()?;
                print_story_verify_all(&result);
                if result.failed() > 0 {
                    std::process::exit(1);
                }
            }
        },
        Command::Decision(args) => match args.action {
            DecisionAction::Add(args) => {
                service.add_decision(DecisionAddInput {
                    id: args.id.clone(),
                    title: args.title,
                    status: args.status,
                    doc_path: args.doc,
                    verify_command: args.verify,
                    predicted_impact: args.predicted,
                    notes: args.notes,
                })?;
                println!("Decision {} added.", args.id);
            }
            DecisionAction::Verify { id } => {
                let result = service.verify_decision(&id)?;
                println!("Running: {}", result.command);
                println!("Decision {id} verification: {}", result.result);
                if result.result == "fail" {
                    std::process::exit(1);
                }
            }
        },
        Command::Backlog(args) => match args.action {
            BacklogAction::Add(args) => {
                let id = service.add_backlog(BacklogAddInput {
                    title: args.title,
                    discovered_while: args.discovered_while,
                    current_pain: args.pain,
                    suggestion: args.suggestion,
                    risk: args
                        .risk
                        .map(|value| RiskLane::from_str(&value))
                        .transpose()?,
                    predicted_impact: args.predicted,
                    notes: args.notes,
                })?;
                println!("Backlog #{id} added.");
            }
            BacklogAction::Close(args) => {
                let id = parse_optional_integer("backlog close: --id", Some(args.id))?
                    .expect("value provided");
                let status = args.status;
                service.close_backlog(BacklogCloseInput {
                    id,
                    status: status.clone(),
                    actual_outcome: args.outcome,
                })?;
                println!("Backlog #{id} closed as {status}.");
            }
            BacklogAction::Outcome(args) => match args.action {
                BacklogOutcomeAction::Record(args) => {
                    let id = parse_optional_integer("backlog outcome record: --id", Some(args.id))?
                        .expect("value provided");
                    let observation = service.record_backlog_outcome(BacklogOutcomeInput {
                        id,
                        status: args.status,
                        outcome: args.outcome,
                        evidence: args.evidence,
                    })?;
                    println!(
                        "Backlog #{} outcome observation {} recorded as {} at {}.",
                        observation.backlog_id,
                        observation.ordinal,
                        observation.status,
                        observation.observed_at
                    );
                }
            },
            BacklogAction::Reconcile(args) => {
                if !args.dry_run && !args.apply {
                    return Err(InterfaceError::InvalidArgument(
                        "backlog reconcile requires exactly one of --dry-run or --apply".to_owned(),
                    ));
                }
                debug_assert_eq!(args.action, "backfill-lifecycle-identity");
                print_legacy_reconcile(&service.reconcile_legacy_improvements(args.apply)?);
            }
        },
        Command::Tool(args) => match args.action {
            ToolAction::Register(args) => {
                let kind = validate_tool_kind(&args.kind)?;
                let capability = args
                    .capability
                    .as_deref()
                    .map(normalize_capability)
                    .transpose()?;
                service.register_tool(ToolRegisterInput {
                    name: args.name.clone(),
                    command: args.command,
                    description: args.description,
                    responsibility: validate_responsibility(&args.responsibility)?,
                    args: parse_tool_args(args.args)?,
                    force: args.force,
                    kind,
                    capability,
                    scan_target: args.scan,
                })?;
                println!("Tool {} registered.", args.name);
            }
            ToolAction::Check(args) => {
                let results = service.check_tools(args.name)?;
                if args.json {
                    print_tool_check_json(&results);
                } else {
                    print_tool_check_summary(&results);
                }
            }
            ToolAction::Remove { name } => {
                service.remove_tool(&name)?;
                println!("Tool {name} removed.");
            }
        },
        Command::Intervention(args) => match args.action {
            InterventionAction::Add(args) => {
                let id = service.add_intervention(InterventionAddInput {
                    trace_id: parse_optional_integer("intervention add: --trace", args.trace)?,
                    story_id: args.story,
                    intervention_type: args.intervention_type,
                    description: args.description,
                    source: args.source,
                    impact: args.impact,
                })?;
                println!("Intervention #{id} recorded.");
            }
        },
        Command::Trace(args) => {
            let story_id = args.story.clone();
            let id = service.record_trace(TraceInput {
                task_summary: args.summary,
                intake_id: parse_optional_integer("trace: --intake", args.intake)?,
                story_id: args.story,
                agent: args.agent,
                outcome: args.outcome,
                duration_seconds: parse_optional_integer("trace: --duration", args.duration)?,
                token_estimate: parse_optional_integer("trace: --tokens", args.tokens)?,
                friction: args.friction,
                notes: args.notes,
                actions: CsvList::try_from_optional(args.actions)?,
                files_read: CsvList::try_from_optional(args.files_read)?,
                files_changed: CsvList::try_from_optional(args.files_changed)?,
                decisions: CsvList::try_from_optional(args.decisions)?,
                errors: CsvList::try_from_optional(args.errors)?,
            })?;
            println!("Trace #{id} recorded.");
            let result = service.score_trace(Some(id))?;
            print_trace_score(&result, false);
            println!("Reminder: Record any human corrections with: harness-cli intervention add");
            if let Some(story_id) = story_id {
                print_story_verify_warning(&service, &story_id)?;
            }
        }
        Command::ScoreTrace(args) => {
            let id = parse_optional_integer("score-trace: --id", args.id)?;
            let result = service.score_trace(id)?;
            print_trace_score(&result, id.is_none());
            if !result.meets_requirement {
                std::process::exit(1);
            }
        }
        Command::ScoreContext { trace_id } => {
            let id = parse_optional_integer("score-context: trace-id", Some(trace_id))?
                .expect("value provided");
            print_context_score(&service.score_context(id)?);
        }
        Command::Audit(args) => {
            let result = if args.record_evidence {
                service.audit_record_evidence()?
            } else {
                service.audit()?
            };
            print_audit(&result)
        }
        Command::Propose(args) => {
            if args.commit {
                eprintln!("use propose --accept <proposal-key> or propose --reject <proposal-key>");
                std::process::exit(2);
            }
            let decision = match (args.accept, args.reject) {
                (None, None) => {
                    if args.outcome_manual
                        || args.outcome_due.is_some()
                        || args.outcome_after_traces.is_some()
                        || args.reason.is_some()
                    {
                        return Err(InterfaceError::InvalidArgument(
                            "propose decision flags require --accept or --reject".to_owned(),
                        ));
                    }
                    if args.show_suppressed {
                        ProposalDecision::PreviewSuppressed
                    } else {
                        ProposalDecision::Preview
                    }
                }
                (Some(key), None) => {
                    let schedules = usize::from(args.outcome_manual)
                        + usize::from(args.outcome_due.is_some())
                        + usize::from(args.outcome_after_traces.is_some());
                    if schedules != 1 || args.reason.is_some() {
                        return Err(InterfaceError::InvalidArgument(
                            "propose --accept requires exactly one outcome schedule".to_owned(),
                        ));
                    }
                    let schedule = if args.outcome_manual {
                        "manual".to_owned()
                    } else if let Some(value) = args.outcome_due {
                        format!("due:{value}")
                    } else {
                        format!("traces:{}", args.outcome_after_traces.unwrap_or_default())
                    };
                    ProposalDecision::Accept { key, schedule }
                }
                (None, Some(key)) => {
                    if args.outcome_manual
                        || args.outcome_due.is_some()
                        || args.outcome_after_traces.is_some()
                    {
                        return Err(InterfaceError::InvalidArgument(
                            "propose --reject does not accept an outcome schedule".to_owned(),
                        ));
                    }
                    ProposalDecision::Reject {
                        key,
                        reason: args.reason.ok_or_else(|| {
                            InterfaceError::InvalidArgument(
                                "propose --reject requires --reason".to_owned(),
                            )
                        })?,
                    }
                }
                _ => unreachable!(),
            };
            print_proposal_result(&service.propose(decision)?)
        }
        Command::Db(args) => match args.action {
            DbAction::Changeset(args) => match args.action {
                ChangesetAction::Apply { path, json } => {
                    let result = service.apply_changeset(&path)?;
                    if json {
                        print_machine_success("db.changeset.apply", serde_json::to_value(result)?)?
                    } else {
                        print_changeset_apply_result(result)
                    }
                }
                ChangesetAction::Status { path, .. } => print_machine_success(
                    "db.changeset.status",
                    serde_json::to_value(service.changeset_status(&path)?)?,
                )?,
            },
            DbAction::Snapshot(args) => {
                if args.output.to_str().is_none() {
                    return Err(InterfaceError::NonUtf8Path);
                }
                print_machine_success(
                    "db.snapshot",
                    serde_json::to_value(service.snapshot_db(&args.output)?)?,
                )?
            }
            DbAction::Rebuild { from } => print_db_rebuild_result(service.rebuild_db(&from)?),
        },
        Command::Query(args) => match args.view {
            QueryView::Contract(_) => print_machine_success(
                "query.contract",
                serde_json::to_value(service.discover_contract()?)?,
            )?,
            QueryView::Stories(_) => print_machine_success(
                "query.stories",
                serde_json::json!({"stories": service.query_orchestration_stories()?}),
            )?,
            QueryView::WorkGraph(_) => print_machine_success(
                "query.work-graph",
                serde_json::to_value(service.query_work_graph()?)?,
            )?,
            QueryView::Matrix(args) => {
                let records = filter_matrix(service.query_matrix()?, &args);
                print_matrix(&records, args.numeric, args.summary)
            }
            QueryView::Dependencies(args) => {
                let dependencies = service.query_story_dependencies(args.story.as_deref())?;
                if args.json {
                    print_machine_success(
                        "query.dependencies",
                        serde_json::json!({"dependencies": dependencies}),
                    )?;
                } else {
                    print_dependencies(&dependencies)
                }
            }
            QueryView::Hierarchy(args) => {
                let hierarchy = service.query_story_hierarchy(args.story.as_deref())?;
                if args.json {
                    print_machine_success(
                        "query.hierarchy",
                        serde_json::json!({"hierarchy": hierarchy}),
                    )?;
                } else {
                    for edge in hierarchy {
                        println!("{} -> {}", edge.parent, edge.child);
                    }
                }
            }
            QueryView::Backlog(args) => {
                let filter = backlog_filter(&args);
                let id = parse_optional_integer("query backlog: --id", args.id)?;
                if let Some(id) = id {
                    print_query_table(&service.query_sql(&format!(
                        "SELECT backlog.id, backlog.uid, backlog.status, backlog.proposal_key, backlog.predecessor_uid, backlog.resolution_evidence, (SELECT story_id FROM story_backlog_link WHERE backlog_uid=backlog.uid AND relationship='resolves') AS resolver, COALESCE((SELECT group_concat(story_id, ', ') FROM story_backlog_link WHERE backlog_uid=backlog.uid AND relationship='references'), '') AS reference_stories FROM backlog WHERE backlog.id={id};"
                    ))?);
                } else {
                    print_backlog(&service.query_backlog(filter)?);
                }
            }
            QueryView::Decisions => print_decisions(&service.query_decisions()?),
            QueryView::Intakes => print_intakes(&service.query_intakes()?),
            QueryView::Traces => print_traces(&service.query_traces()?),
            QueryView::Friction => print_friction(&service.query_friction()?),
            QueryView::Tools(args) => {
                let responsibility = args
                    .responsibility
                    .map(|value| validate_responsibility(&value))
                    .transpose()?;
                let capability = args
                    .capability
                    .as_deref()
                    .map(normalize_capability)
                    .transpose()?;
                let mut tools = service.query_tools(responsibility, capability)?;
                if let Some(status) = args.status.as_deref() {
                    let normalized = status.trim().to_lowercase();
                    tools.retain(|tool| tool.status == normalized);
                }
                if args.json {
                    print_tools_json(&tools);
                } else {
                    print_tools_summary(&tools);
                }
            }
            QueryView::Interventions(args) => {
                let trace_id = parse_optional_integer("query interventions: --trace", args.trace)?;
                print_interventions(&service.query_interventions(InterventionFilter {
                    trace_id,
                    story_id: args.story,
                    intervention_type: args.intervention_type,
                })?);
            }
            QueryView::Stats => print_stats(&service.query_stats()?),
            QueryView::ImprovementHealth => {
                print_improvement_health(&service.query_improvement_health()?)
            }
            QueryView::Sql { query } => {
                if query.is_empty() {
                    return Err(InterfaceError::EmptySql);
                }
                print_query_table(&service.query_sql(&query.join(" "))?);
            }
        },
    }

    Ok(())
}

fn enforce_control_plane_freeze(
    cli: &Cli,
    context: &HarnessContext,
) -> Result<Option<String>, InterfaceError> {
    let Some(operation) = cli.legacy_lifecycle_write() else {
        return Ok(None);
    };
    let is_upstream_source = context.repo_root.join("Cargo.toml").is_file()
        && context
            .repo_root
            .join("crates/harness-cli/Cargo.toml")
            .is_file();
    // Short-circuit before canonicalize so non-upstream callers (the common
    // case) skip two realpath syscalls per invocation. `is_upstream_source` is
    // pure metadata work — it inspects two Cargo.toml files.
    if !is_upstream_source {
        return Ok(None);
    }

    let default_db = context.repo_root.join("harness.db");
    // Resolve relative paths against the repo root so the freeze sentinel
    // cannot be bypassed by supplying `harness.db`, `./harness.db`, or any
    // other string that refers to the same target database. `Path::join`
    // replaces the receiver when its argument is absolute, so the same call
    // covers both relative and absolute `db_path`. Best-effort canonicalize
    // closes the symlink bypass when the file already exists.
    let resolved_db_path = context.repo_root.join(&context.db_path);
    let canonical_resolved = std::fs::canonicalize(&resolved_db_path).unwrap_or(resolved_db_path);
    let canonical_default = std::fs::canonicalize(&default_db).unwrap_or(default_db);
    if canonical_resolved != canonical_default {
        return Ok(None);
    }

    if !cli.compatibility_write {
        return Err(InterfaceError::LegacyLifecycleWriteFrozen { operation });
    }

    Ok(Some(format!(
        "warning: executing explicitly selected compatibility write '{operation}' against the upstream legacy database; Git-native plans and decisions remain current authority."
    )))
}

fn print_trace_score(result: &TraceScoreResult, latest: bool) {
    if latest {
        println!("Trace #{} (latest):", result.trace_id);
    } else {
        println!("Trace #{}:", result.trace_id);
    }
    println!(
        "  Tier achieved: {} ({}/3)",
        result.achieved.label(),
        result.achieved.score()
    );

    match (&result.risk_lane, result.required) {
        (Some(lane), Some(required)) => {
            println!(
                "  Lane: {} -> required tier: {} ({}/3)",
                lane,
                required.label(),
                required.score()
            );
            if result.meets_requirement {
                println!("  MEETS REQUIREMENT");
            } else {
                println!("  BELOW REQUIREMENT");
            }
        }
        _ => {
            println!("  Lane: unknown (no linked intake)");
        }
    }

    print_missing_fields(
        "minimal",
        TraceQualityTier::Minimal,
        &result.missing_minimal,
    );
    print_missing_fields(
        "standard",
        TraceQualityTier::Standard,
        &result.missing_standard,
    );
    print_missing_fields(
        "detailed",
        TraceQualityTier::Detailed,
        &result.missing_detailed,
    );
}

fn print_story_verify_all(result: &StoryVerifyAllResult) {
    for item in &result.items {
        match item.result.as_str() {
            "skipped" => println!("Story {}: skipped (no verify_command)", item.id),
            status => {
                println!("Story {}: {status}", item.id);
                if !item.stdout.is_empty() {
                    print!("{}", item.stdout);
                }
                if !item.stderr.is_empty() {
                    print!("{}", item.stderr);
                }
            }
        }
    }
    println!(
        "{} stories verified: {} passed, {} failed, {} skipped (no verify_command)",
        result.items.len(),
        result.passed(),
        result.failed(),
        result.skipped()
    );
}

fn print_context_score(result: &ContextScoreResult) {
    println!(
        "Trace #{} | Lane: {} | Phase: {}",
        result.trace_id, result.lane, result.phase
    );
    println!();
    let must_met = result.must.iter().filter(|item| item.met).count();
    println!("Must-read compliance: {must_met}/{}", result.must.len());
    for item in &result.must {
        println!(
            "  {} {} ({})",
            if item.met { "OK" } else { "MISSING" },
            item.label,
            item.target
        );
    }
    let should_met = result.should.iter().filter(|item| item.met).count();
    println!(
        "Should-read compliance: {should_met}/{}",
        result.should.len()
    );
    for item in &result.should {
        println!(
            "  {} {} ({})",
            if item.met { "OK" } else { "MISSING" },
            item.label,
            item.target
        );
    }
    println!("Over-reading: {} item(s)", result.over_read.len());
    for item in &result.over_read {
        println!("  - {item}");
    }
}

fn print_audit(result: &crate::domain::AuditResult) {
    println!("=== Harness Drift Audit ===");
    print_audit_category(
        "Orphaned stories (planned/in-progress, no traces)",
        &result.orphaned_stories,
    );
    print_audit_category("Unverified stories", &result.unverified_stories);
    print_audit_category("Unverified decisions", &result.unverified_decisions);
    print_audit_category(
        "Open backlog without outcomes",
        &result.backlog_without_outcomes,
    );
    print_audit_category("Stale stories", &result.stale_stories);
    print_audit_category("Broken tools", &result.broken_tools);
    println!(
        "Entropy score: {}/100 (lower is better)",
        result.entropy_score()
    );
}

fn print_audit_category(label: &str, findings: &[crate::domain::AuditFinding]) {
    println!();
    println!("{label}: {}", findings.len());
    for finding in findings {
        println!("  - {}: {}", finding.id, finding.title);
    }
}

fn print_proposals(proposals: &[ImprovementProposal]) {
    println!("=== Improvement Proposals ===");
    if proposals.is_empty() {
        println!("No proposals generated.");
        return;
    }
    for (index, proposal) in proposals.iter().enumerate() {
        println!();
        println!(
            "Proposal {} ({} confidence):",
            index + 1,
            proposal.confidence
        );
        println!("  Key: {}", proposal.key);
        println!("  Lifecycle: {}", proposal.lifecycle_state);
        if let Some(explanation) = &proposal.lifecycle_explanation {
            println!("  Lifecycle detail: {explanation}");
        }
        println!("  Title: {}", proposal.title);
        println!("  Component: {}", proposal.component);
        println!("  Evidence: {}", proposal.evidence);
        println!("  Predicted impact: {}", proposal.predicted_impact);
        println!("  Risk: {}", proposal.risk);
        println!("  Suggested action: {}", proposal.suggested_action);
        println!("  Validation: {}", proposal.validation_plan);
        if let Some(id) = proposal.committed_backlog_id {
            println!("  Backlog item: #{id}");
        }
    }
    println!();
    println!(
        "{} proposals generated. Use --accept <proposal-key> or --reject <proposal-key> to make one explicit decision.",
        proposals.len()
    );
}

fn print_proposal_result(result: &ProposalResult) {
    print_proposals(&result.proposals);
    if let Some(message) = &result.message {
        println!("{message}");
    }
}

fn print_changeset_apply_result(result: ChangesetApplyResult) {
    if result.applied {
        println!(
            "Changeset {} applied ({} operation(s)).",
            result.id, result.operations
        );
    } else {
        println!("Changeset {} already applied; skipped.", result.id);
    }
}

fn print_db_rebuild_result(result: DbRebuildResult) {
    println!("Rebuilt database at {}", result.db_path.display());
    println!(
        "Applied {} changeset(s), {} operation(s).",
        result.changesets, result.operations
    );
}

fn print_story_verify_warning(
    service: &HarnessService,
    story_id: &str,
) -> Result<(), InterfaceError> {
    let status = service.story_verify_status(story_id)?;
    let has_command = status
        .verify_command
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    if has_command && status.last_verified_result.as_deref() != Some("pass") {
        println!();
        println!(
            "Warning: Story {} has verify_command but verification has not passed.",
            status.id
        );
        println!("Run: harness-cli story verify {}", status.id);
    }
    Ok(())
}

fn print_missing_fields(label: &str, tier: TraceQualityTier, fields: &[String]) {
    if fields.is_empty() {
        return;
    }
    println!();
    println!("  Missing for {label}:");
    for field in fields {
        println!("    - {field}");
    }
    if tier == TraceQualityTier::Detailed {
        println!();
    }
}

fn backlog_filter(args: &BacklogQueryArgs) -> BacklogFilter {
    if args.open {
        BacklogFilter::Open
    } else if args.closed {
        BacklogFilter::Closed
    } else {
        BacklogFilter::All
    }
}

fn print_brownfield_import_result(result: BrownfieldImportResult) {
    println!("Brownfield import complete.");
    println!("Stories imported or updated: {}", result.stories);
    println!("Decisions imported or updated: {}", result.decisions);
    println!("Backlog items discovered: {}", result.backlog_items);
}

fn parse_optional_bool(
    label: &str,
    value: Option<String>,
) -> Result<Option<BoolFlag>, InterfaceError> {
    value
        .map(|inner| BoolFlag::parse(label, &inner))
        .transpose()
        .map_err(InterfaceError::from)
}

fn print_init_result(result: InitResult) {
    match result {
        InitResult::Created { db_path } => {
            println!("Creating harness database at {}", db_path.display());
            println!("Schema applied.");
        }
        InitResult::Existing { db_path, version } => {
            println!("Database already exists at {}", db_path.display());
            println!("Current schema version: {version}");
        }
        InitResult::MigratedExisting { db_path } => {
            println!("Database already exists at {}", db_path.display());
            println!("No schema version found. Applying schema.");
            println!("Schema applied.");
        }
    }
}

fn print_migrate_result(result: MigrateResult) {
    println!("Current schema version: {}", result.current_version);
    if result.applied.is_empty() {
        println!("Already up to date.");
    } else {
        for version in &result.applied {
            println!("Applying migration {version}...");
        }
        println!("Applied {} migration(s).", result.applied.len());
    }
}

fn resolve_context() -> Result<HarnessContext, InterfaceError> {
    let repo_root = match env::var_os("HARNESS_REPO_ROOT") {
        Some(path) => PathBuf::from(path),
        None => env::current_dir().map_err(InterfaceError::CurrentDir)?,
    };
    let db_path = resolve_db_path(
        &repo_root,
        env::var_os("HARNESS_DB_PATH").map(PathBuf::from),
        env::var_os("HARNESS_DB").map(PathBuf::from),
    );

    let schema_dir = repo_root.join("scripts/schema");

    Ok(HarnessContext {
        repo_root,
        db_path,
        schema_dir,
    })
}

fn resolve_db_path(
    repo_root: &std::path::Path,
    harness_db_path: Option<PathBuf>,
    legacy_harness_db: Option<PathBuf>,
) -> PathBuf {
    harness_db_path
        .or(legacy_harness_db)
        .unwrap_or_else(|| repo_root.join("harness.db"))
}

fn filter_matrix(
    records: Vec<StoryMatrixRecord>,
    args: &MatrixQueryArgs,
) -> Vec<StoryMatrixRecord> {
    records
        .into_iter()
        .filter(|record| {
            args.story.as_deref().is_none_or(|story| record.id == story)
                && (!args.active
                    || matches!(
                        record.status.as_str(),
                        "planned" | "in_progress" | "changed"
                    ))
                && (!args.runnable || record.runnable)
        })
        .collect()
}

fn matrix_table(
    records: &[StoryMatrixRecord],
    numeric: bool,
    summary: bool,
) -> (Vec<&'static str>, Vec<Vec<String>>) {
    let rows = records
        .iter()
        .map(|record| {
            if summary {
                vec![
                    record.id.clone(),
                    record.title.clone(),
                    record.risk_lane.clone(),
                    record.status.clone(),
                    if record.runnable { "yes" } else { "no" }.to_owned(),
                    proof_display(record.unit, numeric),
                    proof_display(record.integration, numeric),
                    proof_display(record.e2e, numeric),
                    proof_display(record.platform, numeric),
                ]
            } else {
                vec![
                    record.id.clone(),
                    record.title.clone(),
                    record.status.clone(),
                    proof_display(record.unit, numeric),
                    proof_display(record.integration, numeric),
                    proof_display(record.e2e, numeric),
                    proof_display(record.platform, numeric),
                    record.evidence.clone().unwrap_or_default(),
                ]
            }
        })
        .collect::<Vec<_>>();

    let headers = if summary {
        vec![
            "id", "title", "lane", "status", "runnable", "unit", "integ", "e2e", "plat",
        ]
    } else {
        vec![
            "id", "title", "status", "unit", "integ", "e2e", "plat", "evidence",
        ]
    };
    (headers, rows)
}

fn print_matrix(records: &[StoryMatrixRecord], numeric: bool, summary: bool) {
    let (headers, rows) = matrix_table(records, numeric, summary);
    print_table(&headers, &rows);
}

fn print_dependencies(records: &[crate::application::StoryDependencyRecord]) {
    let rows = records
        .iter()
        .map(|record| vec![record.blocker.clone(), record.blocked.clone()])
        .collect::<Vec<_>>();
    print_table(&["blocker", "blocked"], &rows);
}

fn print_story_backlog_links(records: &[crate::application::StoryBacklogLinkRecord]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.story_id.clone(),
                record.backlog_id.to_string(),
                record.backlog_uid.clone(),
                record.relationship.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["story", "backlog", "backlog_uid", "relationship"], &rows);
}

fn print_backlog(records: &[BacklogRecord]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.id.to_string(),
                record.title.clone(),
                record.status.clone(),
                record.risk.clone().unwrap_or_default(),
                record.predicted_impact.clone().unwrap_or_default(),
                record.actual_outcome.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "id",
            "title",
            "status",
            "risk",
            "predicted_impact",
            "actual_outcome",
        ],
        &rows,
    );
}

fn print_legacy_reconcile(result: &LegacyReconcileResult) {
    let rows = result
        .records
        .iter()
        .map(|record| {
            vec![
                record.backlog_id.to_string(),
                record.classification.clone(),
                record.proposal_key.clone().unwrap_or_default(),
                record.changes.clone(),
                record.reason.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "backlog",
            "classification",
            "proposal_key",
            "changes",
            "reason",
        ],
        &rows,
    );
    println!(
        "Legacy reconciliation: mode={}, changed={}, trace={}",
        if result.applied { "apply" } else { "dry-run" },
        result.changed,
        result
            .trace_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_owned())
    );
}

fn print_decisions(records: &[DecisionRecord]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.id.clone(),
                record.title.clone(),
                record.status.clone(),
                record.last_verified_at.clone().unwrap_or_default(),
                record.last_verified_result.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "id",
            "title",
            "status",
            "last_verified_at",
            "last_verified_result",
        ],
        &rows,
    );
}

fn print_intakes(records: &[IntakeRecord]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.id.to_string(),
                record.created_at.clone(),
                record.input_type.clone(),
                record.risk_lane.clone(),
                record.summary.clone(),
            ]
        })
        .collect::<Vec<_>>();

    print_table(
        &["id", "created_at", "input_type", "risk_lane", "summary"],
        &rows,
    );
}

fn print_traces(records: &[TraceRecord]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.id.to_string(),
                record.created_at.clone(),
                record.outcome.clone().unwrap_or_default(),
                record.task_summary.clone(),
                record.harness_friction.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "id",
            "created_at",
            "outcome",
            "task_summary",
            "harness_friction",
        ],
        &rows,
    );
}

fn print_friction(records: &[FrictionRecord]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.id.to_string(),
                record.created_at.clone(),
                record.risk_lane.clone().unwrap_or_else(|| "-".to_owned()),
                record.input_type.clone().unwrap_or_else(|| "-".to_owned()),
                record.task_summary.clone(),
                record.harness_friction.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "id",
            "created_at",
            "risk_lane",
            "input_type",
            "task_summary",
            "harness_friction",
        ],
        &rows,
    );
}

fn print_tools_summary(records: &[ToolEntry]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.name.clone(),
                record.kind.clone(),
                record.capability.clone().unwrap_or_else(|| "-".to_owned()),
                record.responsibility.clone(),
                record.status.clone(),
                record.source.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "name",
            "kind",
            "capability",
            "responsibility",
            "status",
            "source",
        ],
        &rows,
    );
}

fn print_tools_json(records: &[ToolEntry]) {
    println!("[");
    for (index, record) in records.iter().enumerate() {
        let comma = if index + 1 == records.len() { "" } else { "," };
        println!("  {{");
        println!("    \"provider\": \"{}\",", json_escape(&record.provider));
        println!("    \"name\": \"{}\",", json_escape(&record.name));
        println!("    \"command\": \"{}\",", json_escape(&record.command));
        println!(
            "    \"description\": \"{}\",",
            json_escape(&record.description)
        );
        println!("    \"args\": [");
        for (arg_index, arg) in record.args.iter().enumerate() {
            let arg_comma = if arg_index + 1 == record.args.len() {
                ""
            } else {
                ","
            };
            println!(
                "      {{\"name\":\"{}\",\"type\":\"{}\",\"required\":{},\"help\":\"{}\"}}{}",
                json_escape(&arg.name),
                json_escape(&arg.arg_type),
                arg.required,
                json_escape(arg.help.as_deref().unwrap_or("")),
                arg_comma
            );
        }
        println!("    ],");
        println!(
            "    \"responsibility\": \"{}\",",
            json_escape(&record.responsibility)
        );
        println!("    \"source\": \"{}\",", json_escape(&record.source));
        println!("    \"since\": \"{}\",", json_escape(&record.since));
        println!("    \"kind\": \"{}\",", json_escape(&record.kind));
        println!(
            "    \"capability\": {},",
            json_optional(record.capability.as_deref())
        );
        println!(
            "    \"scan_target\": {},",
            json_optional(record.scan_target.as_deref())
        );
        println!("    \"status\": \"{}\",", json_escape(&record.status));
        println!(
            "    \"checked_at\": {}",
            json_optional(record.checked_at.as_deref())
        );
        println!("  }}{comma}");
    }
    println!("]");
}

fn print_tool_check_summary(records: &[ToolCheckResult]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.name.clone(),
                record.kind.clone(),
                record.capability.clone().unwrap_or_else(|| "-".to_owned()),
                record.status.clone(),
                record.detail.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["name", "kind", "capability", "status", "detail"], &rows);
}

fn print_tool_check_json(records: &[ToolCheckResult]) {
    println!("[");
    for (index, record) in records.iter().enumerate() {
        let comma = if index + 1 == records.len() { "" } else { "," };
        println!("  {{");
        println!("    \"name\": \"{}\",", json_escape(&record.name));
        println!("    \"kind\": \"{}\",", json_escape(&record.kind));
        println!(
            "    \"capability\": {},",
            json_optional(record.capability.as_deref())
        );
        println!("    \"status\": \"{}\",", json_escape(&record.status));
        println!("    \"detail\": \"{}\"", json_escape(&record.detail));
        println!("  }}{comma}");
    }
    println!("]");
}

fn json_optional(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_owned(),
    }
}

fn print_interventions(records: &[InterventionRecord]) {
    let rows = records
        .iter()
        .map(|record| {
            vec![
                record.id.to_string(),
                record.created_at.clone(),
                record
                    .trace_id
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                record.story_id.clone().unwrap_or_default(),
                record.intervention_type.clone(),
                record.source.clone(),
                record.description.clone(),
                record.impact.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "id",
            "created_at",
            "trace",
            "story",
            "type",
            "source",
            "description",
            "impact",
        ],
        &rows,
    );
}

fn json_escape(value: &str) -> String {
    // Delegated to serde_json so the control-character handling stays in sync
    // with RFC 8259 — including the generic `\u00xx` fallback for bytes below
    // 0x20 that the previous hand-rolled encoder had to spell out one by one.
    // `to_string` wraps the escaped body in quotes; strip them so the call
    // sites that print `\"field\": {}` keep working without rewriting every
    // print site. Serializing a `&str` is infallible, so the unwrap is safe.
    serde_json::to_string(value)
        .expect("serializing a Rust &str to JSON is infallible")
        .trim_start_matches('"')
        .trim_end_matches('"')
        .to_owned()
}

fn print_stats(stats: &HarnessStats) {
    println!("=== Harness Stats ===");
    print_table(
        &["intakes", "stories", "decisions", "backlog_items", "traces"],
        &[vec![
            stats.intakes.to_string(),
            stats.stories.to_string(),
            stats.decisions.to_string(),
            stats.backlog_items.to_string(),
            stats.traces.to_string(),
        ]],
    );
}

fn print_improvement_health(result: &ImprovementHealthResult) {
    println!("=== Daily Improvement Health ===");
    println!("Audit entropy: {}/100", result.entropy_score);
    println!("Actionable drift: {}", result.actionable_drift);
    let rows = result
        .items
        .iter()
        .map(|item| {
            vec![
                item.category.clone(),
                item.id.clone(),
                item.title.clone(),
                item.state.clone(),
                item.schedule.clone(),
                item.outcome.clone(),
                item.evidence.clone(),
                item.next_action.clone(),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "category",
            "id",
            "title",
            "state",
            "schedule",
            "outcome",
            "evidence",
            "next_action",
        ],
        &rows,
    );
}

fn print_query_table(table: &QueryTable) {
    let headers = table.headers.iter().map(String::as_str).collect::<Vec<_>>();
    print_table(&headers, &table.rows);
}

fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let widths = headers
        .iter()
        .enumerate()
        .map(|(index, header)| {
            rows.iter()
                .filter_map(|row| row.get(index))
                .map(String::len)
                .chain(std::iter::once(header.len()))
                .max()
                .unwrap_or(header.len())
        })
        .collect::<Vec<_>>();

    print_row(
        &headers
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>(),
        &widths,
    );
    print_row(
        &widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>(),
        &widths,
    );
    for row in rows {
        print_row(row, &widths);
    }
}

fn print_row(values: &[String], widths: &[usize]) {
    for (index, width) in widths.iter().enumerate() {
        if index > 0 {
            print!("  ");
        }
        let value = values.get(index).map(String::as_str).unwrap_or("");
        print!("{value:<width$}");
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use std::path::Path;

    fn matrix_record(
        id: &str,
        status: &str,
        runnable: bool,
        evidence: Option<&str>,
    ) -> StoryMatrixRecord {
        StoryMatrixRecord {
            id: id.to_owned(),
            title: format!("{id} title"),
            risk_lane: "normal".to_owned(),
            status: status.to_owned(),
            unit: 1,
            integration: 0,
            e2e: 1,
            platform: 0,
            evidence: evidence.map(str::to_owned),
            runnable,
        }
    }

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    fn context(repo_root: &Path, db_path: &Path) -> HarnessContext {
        HarnessContext {
            repo_root: repo_root.to_path_buf(),
            db_path: db_path.to_path_buf(),
            schema_dir: repo_root.join("scripts/schema"),
        }
    }

    #[test]
    fn phase4_freeze_targets_only_upstream_default_lifecycle_writes() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("crates/harness-cli")).unwrap();
        std::fs::write(root.path().join("Cargo.toml"), "[workspace]\n").unwrap();
        std::fs::write(
            root.path().join("crates/harness-cli/Cargo.toml"),
            "[package]\nname='harness-cli'\nversion='0.0.0'\n",
        )
        .unwrap();
        let default_context = context(root.path(), &root.path().join("harness.db"));

        let intake = Cli::try_parse_from([
            "harness-cli",
            "intake",
            "--type",
            "change request",
            "--summary",
            "legacy write",
            "--lane",
            "normal",
        ])
        .unwrap();
        let error = enforce_control_plane_freeze(&intake, &default_context).unwrap_err();
        assert!(matches!(
            error,
            InterfaceError::LegacyLifecycleWriteFrozen {
                operation: "intake"
            }
        ));

        let explicit_intake = Cli::try_parse_from([
            "harness-cli",
            "--compatibility-write",
            "intake",
            "--type",
            "change request",
            "--summary",
            "legacy write",
            "--lane",
            "normal",
        ])
        .unwrap();
        let notice = enforce_control_plane_freeze(&explicit_intake, &default_context)
            .unwrap()
            .unwrap();
        assert!(notice.contains("explicitly selected compatibility write 'intake'"));

        let query = Cli::try_parse_from(["harness-cli", "query", "intakes"]).unwrap();
        assert!(enforce_control_plane_freeze(&query, &default_context)
            .unwrap()
            .is_none());

        let isolated = context(root.path(), &root.path().join("fixture.db"));
        assert!(enforce_control_plane_freeze(&intake, &isolated)
            .unwrap()
            .is_none());
    }

    #[test]
    fn phase4_warning_does_not_change_machine_or_maintenance_boundaries() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("crates/harness-cli")).unwrap();
        std::fs::write(root.path().join("Cargo.toml"), "[workspace]\n").unwrap();
        std::fs::write(
            root.path().join("crates/harness-cli/Cargo.toml"),
            "[package]\nname='harness-cli'\nversion='0.0.0'\n",
        )
        .unwrap();
        let source = context(root.path(), &root.path().join("harness.db"));

        let machine_story = Cli::try_parse_from([
            "harness-cli",
            "story",
            "add",
            "--id",
            "US-200",
            "--title",
            "Machine story",
            "--lane",
            "normal",
            "--json",
        ])
        .unwrap();
        assert!(enforce_control_plane_freeze(&machine_story, &source)
            .unwrap()
            .is_none());

        for arguments in [
            vec!["harness-cli", "init"],
            vec!["harness-cli", "migrate"],
            vec![
                "harness-cli",
                "db",
                "rebuild",
                "--from",
                ".harness/changesets",
            ],
            vec!["harness-cli", "audit"],
            vec!["harness-cli", "propose"],
        ] {
            let cli = Cli::try_parse_from(arguments).unwrap();
            assert!(enforce_control_plane_freeze(&cli, &source)
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn story_dependency_command_parses_typed_edges() {
        let cli = Cli::try_parse_from([
            "harness-cli",
            "story",
            "dependency",
            "add",
            "--blocker",
            "US-073",
            "--blocked",
            "US-074",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Story(StoryArgs {
                action: StoryAction::Dependency(StoryDependencyArgs {
                    action: StoryDependencyAction::Add(StoryDependencyMutationArgs { blocker, blocked, .. })
                })
            }) if blocker == "US-073" && blocked == "US-074"
        ));

        let cli = Cli::try_parse_from([
            "harness-cli",
            "story",
            "dependency",
            "remove",
            "--blocker",
            "US-073",
            "--blocked",
            "US-074",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Story(StoryArgs {
                action: StoryAction::Dependency(StoryDependencyArgs {
                    action: StoryDependencyAction::Remove(StoryDependencyMutationArgs { blocker, blocked, .. })
                })
            }) if blocker == "US-073" && blocked == "US-074"
        ));

        let cli =
            Cli::try_parse_from(["harness-cli", "query", "dependencies", "--story", "US-074"])
                .unwrap();
        assert!(matches!(
            cli.command,
            Command::Query(QueryArgs {
                view: QueryView::Dependencies(DependenciesQueryArgs { story: Some(story), .. })
            }) if story == "US-074"
        ));
    }

    #[test]
    fn improvement_health_commands_parse_typed_outcomes() {
        let cli = Cli::try_parse_from([
            "harness-cli",
            "backlog",
            "outcome",
            "record",
            "--id",
            "42",
            "--status",
            "confirmed",
            "--outcome",
            "Friction decreased",
            "--evidence",
            "five traces",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Backlog(BacklogArgs {
                action: BacklogAction::Outcome(BacklogOutcomeArgs {
                    action: BacklogOutcomeAction::Record(BacklogOutcomeRecordArgs { status, .. })
                })
            }) if status == "confirmed"
        ));

        let cli = Cli::try_parse_from(["harness-cli", "query", "improvement-health"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Query(QueryArgs {
                view: QueryView::ImprovementHealth
            })
        ));
    }

    #[test]
    fn matrix_query_parses_additive_filters() {
        let cli = Cli::try_parse_from([
            "harness-cli",
            "query",
            "matrix",
            "--active",
            "--runnable",
            "--story",
            "US-079",
            "--summary",
            "--numeric",
        ])
        .unwrap();

        assert!(matches!(
            cli.command,
            Command::Query(QueryArgs {
                view: QueryView::Matrix(MatrixQueryArgs {
                    active: true,
                    runnable: true,
                    story: Some(story),
                    summary: true,
                    numeric: true,
                })
            }) if story == "US-079"
        ));
    }

    #[test]
    fn matrix_filters_combine_with_and_semantics() {
        let active = filter_matrix(
            vec![
                matrix_record("US-A", "planned", true, Some("ready")),
                matrix_record("US-B", "planned", false, Some("blocked")),
                matrix_record("US-C", "in_progress", false, None),
                matrix_record("US-D", "implemented", false, Some("done")),
                matrix_record("US-E", "changed", false, Some("changed")),
                matrix_record("US-F", "retired", false, Some("retired")),
            ],
            &MatrixQueryArgs {
                active: true,
                ..MatrixQueryArgs::default()
            },
        );
        assert_eq!(
            active
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            vec!["US-A", "US-B", "US-C", "US-E"]
        );

        let runnable = filter_matrix(
            vec![
                matrix_record("US-A", "planned", true, Some("ready")),
                matrix_record("US-B", "planned", false, Some("blocked")),
            ],
            &MatrixQueryArgs {
                runnable: true,
                ..MatrixQueryArgs::default()
            },
        );
        assert_eq!(runnable.len(), 1);
        assert_eq!(runnable[0].id, "US-A");

        let exact_story = filter_matrix(
            vec![
                matrix_record("US-A", "planned", true, None),
                matrix_record("US-D", "implemented", false, None),
            ],
            &MatrixQueryArgs {
                story: Some("US-D".to_owned()),
                ..MatrixQueryArgs::default()
            },
        );
        assert_eq!(exact_story.len(), 1);
        assert_eq!(exact_story[0].id, "US-D");

        let combined = filter_matrix(
            vec![
                matrix_record("US-A", "planned", true, Some("ready")),
                matrix_record("US-B", "planned", false, Some("blocked")),
                matrix_record("US-C", "in_progress", false, None),
                matrix_record("US-D", "implemented", false, Some("done")),
            ],
            &MatrixQueryArgs {
                active: true,
                runnable: true,
                story: Some("US-A".to_owned()),
                ..MatrixQueryArgs::default()
            },
        );
        assert_eq!(combined.len(), 1);
        assert_eq!(combined[0].id, "US-A");

        let excluded = filter_matrix(
            vec![matrix_record("US-B", "planned", false, None)],
            &MatrixQueryArgs {
                active: true,
                runnable: true,
                story: Some("US-B".to_owned()),
                ..MatrixQueryArgs::default()
            },
        );
        assert!(excluded.is_empty());
    }

    #[test]
    fn matrix_default_and_numeric_output_shape_remain_unchanged() {
        let record = matrix_record("US-A", "planned", true, Some("proof details"));

        let (headers, rows) = matrix_table(std::slice::from_ref(&record), false, false);
        assert_eq!(
            headers,
            vec!["id", "title", "status", "unit", "integ", "e2e", "plat", "evidence"]
        );
        assert_eq!(
            rows,
            vec![vec![
                "US-A",
                "US-A title",
                "planned",
                "yes",
                "no",
                "yes",
                "no",
                "proof details",
            ]]
        );

        let (_, numeric_rows) = matrix_table(std::slice::from_ref(&record), true, false);
        assert_eq!(&numeric_rows[0][3..7], ["1", "0", "1", "0"]);

        let (summary_headers, summary_rows) = matrix_table(&[record], false, true);
        assert_eq!(
            summary_headers,
            vec!["id", "title", "lane", "status", "runnable", "unit", "integ", "e2e", "plat"]
        );
        assert_eq!(summary_rows[0].len(), 9);
        assert_eq!(&summary_rows[0][2..5], ["normal", "planned", "yes"]);
        assert!(!summary_rows[0].iter().any(|value| value == "proof details"));
    }

    #[test]
    fn legacy_reconciliation_command_requires_an_explicit_mode() {
        let cli = Cli::try_parse_from([
            "harness-cli",
            "backlog",
            "reconcile",
            "--action",
            "backfill-lifecycle-identity",
            "--dry-run",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Backlog(BacklogArgs {
                action: BacklogAction::Reconcile(BacklogReconcileArgs {
                    dry_run: true,
                    apply: false,
                    ..
                })
            })
        ));
        assert!(Cli::try_parse_from([
            "harness-cli",
            "backlog",
            "reconcile",
            "--action",
            "backfill-lifecycle-identity",
            "--dry-run",
            "--apply",
        ])
        .is_err());
    }

    #[test]
    fn harness_db_path_overrides_legacy_harness_db() {
        let db_path = resolve_db_path(
            Path::new("/repo"),
            Some(PathBuf::from("/isolated/harness.db")),
            Some(PathBuf::from("/legacy/harness.db")),
        );

        assert_eq!(db_path, PathBuf::from("/isolated/harness.db"));
    }

    #[test]
    fn legacy_harness_db_remains_fallback() {
        let db_path = resolve_db_path(
            Path::new("/repo"),
            None,
            Some(PathBuf::from("/legacy/harness.db")),
        );

        assert_eq!(db_path, PathBuf::from("/legacy/harness.db"));
    }

    #[test]
    fn database_path_defaults_to_repo_root_harness_db() {
        let db_path = resolve_db_path(Path::new("/repo"), None, None);

        assert_eq!(db_path, PathBuf::from("/repo/harness.db"));
    }

    #[test]
    fn story_help_documents_proof_command_shape() {
        let mut command = Cli::command();
        let story = command.find_subcommand_mut("story").unwrap();

        let update_help = story
            .find_subcommand_mut("update")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(update_help.contains("--unit <0|1>"));
        assert!(update_help.contains("--integration <0|1>"));
        assert!(update_help.contains("Proof flags use numeric booleans"));

        let verify_help = story
            .find_subcommand_mut("verify")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(verify_help.contains("story verify only accepts the story id"));
        assert!(verify_help.contains("Configure proof with story add/update --verify"));
    }

    #[test]
    fn command_help_documents_lane_values_and_version() {
        let mut command = Cli::command();
        assert!(command.render_long_help().to_string().contains("--version"));

        let intake_help = command
            .find_subcommand_mut("intake")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(intake_help.contains("--lane <tiny|normal|high-risk>"));
        assert!(intake_help.contains("Use tiny instead of low"));

        let story_add_help = command
            .find_subcommand_mut("story")
            .unwrap()
            .find_subcommand_mut("add")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(story_add_help.contains("--lane <tiny|normal|high-risk>"));

        let backlog_add_help = command
            .find_subcommand_mut("backlog")
            .unwrap()
            .find_subcommand_mut("add")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(backlog_add_help.contains("--risk <tiny|normal|high-risk>"));
        assert!(backlog_add_help.contains("Accepted lanes"));

        let matrix_help = command
            .find_subcommand_mut("query")
            .unwrap()
            .find_subcommand_mut("matrix")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(matrix_help.contains("--numeric"));
        assert!(matrix_help.contains("--active"));
        assert!(matrix_help.contains("--runnable"));
        assert!(matrix_help.contains("--story <ID>"));
        assert!(matrix_help.contains("--summary"));
    }
}
