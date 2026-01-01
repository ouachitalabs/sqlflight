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
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<HavingClause>,
    pub qualify: Option<QualifyClause>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<LimitClause>,
    pub union: Option<Box<SetOperation>>,
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
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub table: TableReference,
}

/// A table reference
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    Table { name: String, alias: Option<String> },
    Subquery { query: Box<SelectStatement>, alias: String },
    JinjaRef(String),
    Flatten(FlattenClause),
    Lateral(Box<TableReference>),
}

/// JOIN clause
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableReference,
    pub condition: Option<Expression>,
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
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
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

/// Window frame bounds
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    Preceding(Option<u64>),
    Following(Option<u64>),
}

/// Data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    BigInt,
    Float,
    Double,
    Decimal(Option<u8>, Option<u8>),
    Varchar(Option<u32>),
    Char(Option<u32>),
    Text,
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
