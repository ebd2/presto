# Presto planner and optimizer tests

Testing planner and optimizer functionality generally requires generating a
query plan and then validating it against a set of expectations.  Expectations
should be expressed concisely and be resilient to changes in the objects Presto
uses to represent the plan and the code that generates the plan.

Unfortunately, that list includes the following:

* The parser
* The analyzer
* The planner
* Some or all of the optimizers
* The plan objects themselves

The plan testing framework attempts to solve the problem of specifiying
expectations about query plans in tests.

## Quickstart

TestPlanMatchingFramework.java contains examples of basic use of the plan
testing framework. If you'd like to write new tests, see
PlanMatchPattern.java to see patterns that are already well-supported by the
framework.

### PlanMatchPatterns

Expectations are expressed using `PlanMatchPatterns`. A `PlanMatchPattern`
roughly corresponds to a `PlanNode` in that it has zero or more
`PlanMatchPatterns` as sources. A complete test generally expresses its
expectations about an actual plan as a tree of `PlanMatchPatterns` that is
structurally similar to the actual plan.

A `PlanMatchPattern` by itself contains no expectations about the contents or
even type of the node that it's meant to match. Instead, expectations are added
to `PlanMatchPatterns` using `Matchers`.

### Matchers

`Matchers` are applied to `PlanNodes` to determine if the node they're applied
to meets some expectation expressed in the `Matcher`.

*A `Matcher` failing to match against a PlanNode it is applied to does not
intrinsically mean that the plan does not match the expectations expressed by
the test.* This is because the plan testing framework uses a
pattern-matching-like algorithm to allow tests to express their expectations
flexibly with regard to changes in the structure of the tree by using the
`anyTree()` pattern.  The `anyTree()` pattern looks for its child patterns in
any of the children of the node it's applied to.

Adding matchers to a `PlanMatchPattern` is usually done using one of the static
methods in PlanMatchPattern.java.

### Aliases

At a high level, Presto plans represent data through `Symbols`. A
`TableScanNode`, for instance, generates one symbol per column in the table as
its output.  `ProjectNodes` can apply a calculation to some input `Symbol` and
generate a new `Symbol` as output.

If you are writing tests, you may care about what is happening to data in the
actual plan. The simplest possible example of this would be asserting that a
`SELECT name FROM people` query has an `OutputNode` with an output `Symbol`
that actually represents the data in the `name` column of the `people` table.

In order for the plan testing framework to be independent of how Presto
allocates `Symbols`, `Symbols` are given an alias where they originate, and
must be referenced by that alias further up the plan. The value being assigned
to a `Symbol` can either be matched against something that is independent of a
`Symbol` (e.g., a `ColumnHandle` in a `TableScanNode`) or a value that is
dependent on a previously aliased symbol (e.g., an `Expression` in a
`ProjectNode`). Matching against the right side of the assignment ensures that
the plan-matching framework is independent of the implementation of the
`SymbolAllocator`.

## Testing planner code

Planner code should be tested by comparing the unoptimized plan to a set of
expectations. For various reasons related to how `Symbols` are handled in
unoptimized plans, this is frequently impossible. The minimum set of optimizers
that needs to be applied for the plan-matching framework to function is likely
to be some subset of the following:

- `UnaliasSymbolReferences`
- `PruneIdentityProjections`
- `PruneUnreferencedOutputs`

`TestQuantifiedComparison` illustrates unit testing planner code.

## Testing optimizer code

Optimizer code will generally need to be subjected to two types of tests:

1. Unit tests on the minimum set of optimizers needed for the optimizer to
   function.
2. A test using the full set of optimizers that ensures that the optimizer
   continues to function after the introduction of new optimizers or reordering
   of existing optimizers.

`TestMergeWindows` illustrates both types of tests and goes further into the
rationale for both types of tests.

## Internals

If you need to extend the plan-matching framework, you will probably need to
take a look at the internals to better understand how the pattern-matching-like
algorithm traverses the query plan and matches it against the expectations
expressed in the tests.

### The PlanMatchingVisitor

The `PlanMatchingVisitor` is responsible for visting the query plan and
ensuring that the actual plan conforms to the expectations expressed in the
test.

### PlanMatchPatterns in traversal

`PlanMatchPatterns` contain a list of source `PlanMatchPatterns` and a list of
`Matchers`. The `PlanMatchingVisitor` is responsible for traversing the actual
plan and making sure that the structure mirrors that of the tree of
`PlanMatchPattens`, and that the `Matchers` in the `PlanMatchPatterns` match
the nodes.

Conceptually, this is relatively simple, but the need for flexibility adds a
wrinkle. A `PlanMatchPattern` contains a field `anyTree`, which indicates that
it can be matched against one or more `PlanNodes` during traversal. As a
consequence, the source patterns in a `PlanMatchPattern` with `anyTree` set
can be matched after traversing an arbitrary number of unspecified nodes.

### Matchers in traversal

The `Matcher` interface is used to express expectations about specific nodes.
`Matchers` are applied in two steps when traversing the plans:

- As the matching algorithm passes downwards from the root of the tree it
  applies the `downMatches()` method, which is responsible for matching the
  shape of the plan and verifying that the types of the nodes the traversal
  passes through match the expectation in the test.
- After the traversal has reached the leaf nodes and is returning up the tree,
  it applies the `upMatches()` method, which is responsible for validating the
  fields in nodes and binding aliases to the `Symbols` it matches against.

### PlanMatchingStates

Using `anyTree()` lets test express expectations about the actual plan
flexibly.  During traversal, the plan-matching framework tracks potential
matches using `PlanMatchingStates`. Because visiting any `PlanNode` can result
in finding multiple possible matches, matching doesn't fail until the plan
matching framework visits a node and gets an empty list of possible matches.
This is why a `Matcher` failing to match against a node does not necessarily
mean that the plan as a whole doesn't match the expectation expressed in the
test.
