export {
  aggregate,
  joinSelectors,
  keyExpression,
  literal,
  parameterReference,
  selectors,
} from "./expression.ts";
export type {
  AggregateValue,
  BooleanExpression,
  DeclaredKey,
  ExpressionValue,
  KeyFieldsOf,
  Reference,
  Selectors,
  TypedAggregateExpression,
  TypedExpression,
} from "./expression.ts";
export {
  changes,
  compilePlan,
  defineView,
  from,
  parameter,
  reducer,
  scope,
  source,
  view,
} from "./relation.ts";
export type {
  ChangeStreamDeclaration,
  EvolveBranch,
  EvolveBranchBuilder,
  GroupedBuilder,
  JoinSpec,
  KeyedRelation,
  ParameterDeclaration,
  ReducerDeclaration,
  ReducerSpec,
  RelationBuilder,
  RelationExpression,
  Scope,
  SourceCollection,
  SourceDeclaration,
  SourceSpec,
  TopSpec,
  ViewDeclaration,
  ViewSpec,
} from "./relation.ts";
export {
  checkPlan,
  collectPlanIssues,
  PlanCheckFailed,
  PlanIssue,
  PlanIssueCode,
} from "./check.ts";
export type {
  CheckedPlan,
  PlanIssue as PlanIssueType,
  PlanIssueCode as PlanIssueCodeType,
} from "./check.ts";
export { encodePlan, planHash } from "./plan.ts";
export type * from "@streamsy/views-ir";
