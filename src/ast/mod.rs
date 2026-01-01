//! AST node definitions for SQL statements
//!
//! This module defines the abstract syntax tree for Snowflake SQL with
//! holes for Jinja expressions.

/// A complete SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Merge(MergeStatement),
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    AlterTable(AlterTableStatement),
    DropTable(DropTableStatement),
    DropView(DropViewStatement),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SelectStatement {
    pub with_clause: Option<WithClause>,
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub joins: Vec<JoinClause>,
    pub pivot: Option<PivotClause>,
    pub unpivot: Option<UnpivotClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<HavingClause>,
    pub qualify: Option<QualifyClause>,
    pub window: Option<WindowClause>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<LimitClause>,
    pub union: Option<Box<SetOperation>>,
    pub semicolon: bool,
}

/// WITH clause (CTEs)
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub ctes: Vec<CommonTableExpression>,
}

/// A single CTE definition
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpression {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<SelectStatement>,
}

/// A column in SELECT list
#[derive(Debug, Clone, PartialEq)]
pub struct SelectColumn {
    pub expr: Expression,
    pub alias: Option<String>,
    pub explicit_as: bool,  // Track if AS keyword was explicit
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub table: TableReference,
}

/// A table reference
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    Table {
        name: String,
        alias: Option<String>,
        time_travel: Option<TimeTravelClause>,
        changes: Option<ChangesClause>,
        sample: Option<SampleClause>,
        pivot: Option<PivotClause>,
        unpivot: Option<UnpivotClause>,
        match_recognize: Option<MatchRecognizeClause>,
    },
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
        explicit_as: bool,
    },
    JinjaRef(String),
    Flatten(FlattenClause),
    Lateral(Box<TableReference>),
    TableFunction {
        name: String,
        args: Vec<(Option<String>, Expression)>,  // (param_name, value)
        table_wrapper: bool,  // true if TABLE(func(...)) syntax was used
        alias: Option<String>,  // Optional alias (e.g., FLATTEN(...) f)
    },
    Values(ValuesClause),
    /// Stage reference (e.g., @stage_name, @stage_name/path, @~, @%table_name)
    Stage {
        name: String,        // Stage name (including ~ or %table_name if present)
        path: Option<String>, // Optional path after stage name
        alias: Option<String>,
    },
}

/// JOIN clause
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableReference,
    pub condition: Option<Expression>,
    pub explicit_inner: bool,  // Track if INNER was explicitly written
    pub is_comma_join: bool,   // Track if this was FROM a, b syntax (implicit cross join)
}

/// JOIN types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// WHERE clause
#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Expression,
}

/// GROUP BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct GroupByClause {
    pub expressions: Vec<Expression>,
}

/// HAVING clause
#[derive(Debug, Clone, PartialEq)]
pub struct HavingClause {
    pub condition: Expression,
}

/// QUALIFY clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub struct QualifyClause {
    pub condition: Expression,
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    pub items: Vec<OrderByItem>,
}

/// A single ORDER BY item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expression,
    pub direction: Option<SortDirection>,
    pub nulls: Option<NullsOrder>,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// NULLS ordering
#[derive(Debug, Clone, PartialEq)]
pub enum NullsOrder {
    First,
    Last,
}

/// LIMIT clause
#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub count: Expression,
    pub offset: Option<Expression>,
}

/// Set operations (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub op_type: SetOperationType,
    pub all: bool,
    pub query: SelectStatement,
}

/// Set operation types
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

/// Expression node
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(Literal),
    Identifier(String),
    QualifiedIdentifier(Vec<String>),
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expression>,
        within_group: Option<Vec<OrderByItem>>,  // WITHIN GROUP (ORDER BY ...)
        over: Option<WindowSpec>,
    },
    Case(CaseExpression),
    Subquery(Box<SelectStatement>),
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expression>,
        subquery: Box<SelectStatement>,
        negated: bool,
    },
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    },
    IsNull {
        expr: Box<Expression>,
        negated: bool,
    },
    /// IS [NOT] TRUE
    IsTrue {
        expr: Box<Expression>,
        negated: bool,
    },
    /// IS [NOT] FALSE
    IsFalse {
        expr: Box<Expression>,
        negated: bool,
    },
    /// IS [NOT] DISTINCT FROM other_expr
    IsDistinctFrom {
        expr: Box<Expression>,
        other: Box<Expression>,
        negated: bool,
    },
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
        shorthand: bool,  // true for :: syntax, false for CAST(x AS type)
        try_cast: bool,   // true for TRY_CAST, false for CAST
    },
    Extract {
        field: String,
        expr: Box<Expression>,
    },
    JinjaExpression(String),
    JinjaBlock(String),
    Parenthesized(Box<Expression>),
    SemiStructuredAccess {
        expr: Box<Expression>,
        path: String,
    },
    ArrayAccess {
        expr: Box<Expression>,
        index: Box<Expression>,
    },
    Star,
    QualifiedStar(String),
    Exists {
        subquery: Box<SelectStatement>,
    },
    /// Positional column reference ($1, $2, etc.) used in staged file queries
    PositionalColumn(u32),
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Date(String),
    Timestamp(String),
}

/// Binary operators
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Like,
    ILike,
    Concat,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

/// CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpression {
    pub operand: Option<Box<Expression>>,
    pub when_clauses: Vec<WhenClause>,
    pub else_clause: Option<Box<Expression>>,
}

/// WHEN clause in CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expression,
    pub result: Expression,
}

/// Window specification for window functions
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Option<Vec<Expression>>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub frame: Option<WindowFrame>,
    pub window_name: Option<String>,  // Reference to a named window from WINDOW clause
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub unit: WindowFrameUnit,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

/// Window frame units
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameUnit {
    Rows,
    Range,
    Groups,
}

/// Window frame bound value (numeric or interval)
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBoundValue {
    Numeric(u64),
    Interval { value: String, unit: String },
}

/// Window frame bounds
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(FrameBoundValue),
    Following(FrameBoundValue),
}

/// Data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Boolean,
    Int,                              // INT synonym
    Integer,                          // INTEGER synonym
    BigInt,
    Float,
    Double,
    Decimal(Option<u8>, Option<u8>),
    Number(Option<u8>, Option<u8>),   // Snowflake's preferred numeric type
    Varchar(Option<u32>),
    String(Option<u32>),              // STRING synonym for VARCHAR
    Text,                             // TEXT synonym for VARCHAR
    Char(Option<u32>),
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Variant,
    Object,
    Array,
}

/// FLATTEN clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub struct FlattenClause {
    pub input: Box<Expression>,
    pub path: Option<String>,
    pub outer: bool,
    pub recursive: bool,
    pub mode: Option<String>,
}

/// INSERT statement stub
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
}

/// INSERT source
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    Values(Vec<Vec<Expression>>),
    Query(Box<SelectStatement>),
}

/// UPDATE statement stub
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub alias: Option<String>,
    pub assignments: Vec<(String, Expression)>,
    pub from: Option<FromClause>,
    pub where_clause: Option<WhereClause>,
}

/// DELETE statement stub
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub alias: Option<String>,
    pub using: Option<FromClause>,
    pub where_clause: Option<WhereClause>,
}

/// MERGE statement stub
#[derive(Debug, Clone, PartialEq)]
pub struct MergeStatement {
    pub target: String,
    pub target_alias: Option<String>,
    pub source: TableReference,
    pub condition: Expression,
    pub clauses: Vec<MergeClause>,
}

/// MERGE clause
#[derive(Debug, Clone, PartialEq)]
pub enum MergeClause {
    WhenMatched {
        condition: Option<Expression>,
        action: MergeAction,
    },
    WhenNotMatched {
        condition: Option<Expression>,
        action: MergeAction,
    },
}

/// MERGE action
#[derive(Debug, Clone, PartialEq)]
pub enum MergeAction {
    Update(Vec<(String, Expression)>),
    Delete,
    Insert {
        columns: Option<Vec<String>>,
        values: Vec<Expression>,
    },
}

/// CREATE TABLE statement stub
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub as_query: Option<Box<SelectStatement>>,
    pub clone_source: Option<String>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Expression>,
}

/// CREATE VIEW statement stub
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStatement {
    pub or_replace: bool,
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub copy_grants: bool,
    pub query: Box<SelectStatement>,
}

/// ALTER TABLE statement stub
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub name: String,
    pub action: AlterTableAction,
}

/// ALTER TABLE actions
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    AddColumn(ColumnDefinition),
    DropColumn(String),
    RenameColumn { old: String, new: String },
    AlterColumn { name: String, data_type: DataType },
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub if_exists: bool,
    pub name: String,
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStatement {
    pub if_exists: bool,
    pub name: String,
}

/// SQL comment
#[derive(Debug, Clone, PartialEq)]
pub struct Comment {
    pub text: String,
    pub style: CommentStyle,
}

/// Comment styles
#[derive(Debug, Clone, PartialEq)]
pub enum CommentStyle {
    SingleLine,  // -- comment
    MultiLine,   // /* comment */
}

/// PIVOT clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub struct PivotClause {
    pub aggregate_functions: Vec<(Expression, Option<String>)>,  // (expr, alias)
    pub for_column: String,
    pub in_values: Vec<(Expression, Option<String>)>,  // (value, alias)
    pub alias: Option<String>,  // Optional table alias after PIVOT
}

/// UNPIVOT clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub struct UnpivotClause {
    pub value_column: String,
    pub name_column: String,
    pub columns: Vec<String>,
    pub include_nulls: bool,
    pub alias: Option<String>,  // Optional table alias after UNPIVOT
}

/// SAMPLE/TABLESAMPLE clause
#[derive(Debug, Clone, PartialEq)]
pub struct SampleClause {
    pub method: SampleMethod,
    pub size: SampleSize,
    pub seed: Option<i64>,
    pub tablesample: bool,  // true if TABLESAMPLE was used, false for SAMPLE
}

/// Sample method
#[derive(Debug, Clone, PartialEq)]
pub enum SampleMethod {
    Default,
    Bernoulli,
    System,
    Block,
}

/// Sample size specification
#[derive(Debug, Clone, PartialEq)]
pub enum SampleSize {
    Percent(f64),
    Rows(i64),
}

/// MATCH_RECOGNIZE clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub struct MatchRecognizeClause {
    pub partition_by: Option<Vec<Expression>>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub measures: Vec<(Expression, String)>,
    pub rows_per_match: Option<RowsPerMatch>,
    pub after_match_skip: Option<AfterMatchSkip>,
    pub pattern: String,
    pub define: Vec<(String, Expression)>,
}

/// Rows per match option
#[derive(Debug, Clone, PartialEq)]
pub enum RowsPerMatch {
    OneRow,
    AllRows,
}

/// After match skip option
#[derive(Debug, Clone, PartialEq)]
pub enum AfterMatchSkip {
    PastLastRow,
    ToNextRow,
    ToFirst(String),
    ToLast(String),
}

/// WINDOW clause for named windows
#[derive(Debug, Clone, PartialEq)]
pub struct WindowClause {
    pub definitions: Vec<WindowDefinition>,
}

/// Named window definition
#[derive(Debug, Clone, PartialEq)]
pub struct WindowDefinition {
    pub name: String,
    pub spec: WindowSpec,
}

/// Time travel clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub enum TimeTravelClause {
    At(TimeTravelPoint),
    Before(TimeTravelPoint),
}

/// Time travel point specification
#[derive(Debug, Clone, PartialEq)]
pub enum TimeTravelPoint {
    Timestamp(Expression),
    Offset(Expression),
    Statement(String),
}

/// CHANGES clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub struct ChangesClause {
    pub information: ChangesInformation,
    pub at_or_before: TimeTravelClause,
}

/// Changes information type
#[derive(Debug, Clone, PartialEq)]
pub enum ChangesInformation {
    Default,
    AppendOnly,
}

/// Interval literal
#[derive(Debug, Clone, PartialEq)]
pub struct IntervalLiteral {
    pub value: String,
    pub unit: String,
}

/// VALUES clause (Snowflake-specific)
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesClause {
    pub rows: Vec<Vec<Expression>>,
    pub alias: Option<ValuesAlias>,
}

/// VALUES alias with column names
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesAlias {
    pub table_alias: String,
    pub column_aliases: Vec<String>,
}
