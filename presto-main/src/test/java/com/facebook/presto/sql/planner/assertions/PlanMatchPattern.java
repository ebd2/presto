/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.rewriteQualifiedNamesToSymbolReferences;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class PlanMatchPattern
{
    private final List<Matcher> matchers = new ArrayList<>();

    private final List<PlanMatchPattern> sourcePatterns;
    private boolean anyTree;

    public static PlanMatchPattern node(Class<? extends PlanNode> nodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new PlanNodeMatcher(nodeClass));
    }

    public static PlanMatchPattern any(PlanMatchPattern... sources)
    {
        return new PlanMatchPattern(ImmutableList.copyOf(sources));
    }

    /**
     * Matches to any tree of nodes with children matching to given source matchers.
     * anyNodeTree(tableScanNode("nation")) - will match to any plan which all leafs contain
     * any node containing table scan from nation table.
     */
    public static PlanMatchPattern anyTree(PlanMatchPattern... sources)
    {
        return any(sources).matchToAnyNodeTree();
    }

    public static PlanMatchPattern anyNot(Class<? extends PlanNode> excludeNodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new NotPlanNodeMatcher(excludeNodeClass));
    }

    public static PlanMatchPattern tableScan(String expectedTableName)
    {
        return node(TableScanNode.class).with(new TableScanMatcher(expectedTableName));
    }

    public static PlanMatchPattern tableScan(String expectedTableName, Map<String, String> columnReferences)
    {
        PlanMatchPattern result = tableScan(expectedTableName);
        return result.addColumnReferences(expectedTableName, columnReferences);
    }

    /*
     * Needs a different name because the signature of constrainedTableScan(String, List)
     * and tableScan(String, List) are the same after type erasure.
     */
    public static PlanMatchPattern constrainedTableScan(String expectedTableName, Map<String, Domain> constraint)
    {
        return node(TableScanNode.class).with(new TableScanMatcher(expectedTableName, constraint));
    }

    public static PlanMatchPattern constrainedTableScan(String expectedTableName, Map<String, Domain> constraint, Map<String, String> columnReferences)
    {
        PlanMatchPattern result = constrainedTableScan(expectedTableName, constraint);
        return result.addColumnReferences(expectedTableName, columnReferences);
    }

    private PlanMatchPattern addColumnReferences(String expectedTableName, Map<String, String> columnReferences)
    {
        columnReferences.entrySet().forEach(
                reference -> withAlias(reference.getKey(), columnReference(expectedTableName, reference.getValue())));
        return this;
    }

    public static PlanMatchPattern aggregate(Map<String, FunctionCallMaker> assignments, PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source);
        assignments.entrySet().forEach(
                assignment -> result.withAlias(assignment.getKey(), new AggregationFunctionMatcher(assignment.getValue())));
        return result;
    }

    public static PlanMatchPattern output(PlanMatchPattern source)
    {
        return node(OutputNode.class, source);
    }

    public static PlanMatchPattern output(ImmutableList<String> outputs, PlanMatchPattern source)
    {
        PlanMatchPattern result = output(source);
        result.withOutputs(outputs);
        return result;
    }

    public static PlanMatchPattern project(PlanMatchPattern source)
    {
        return node(ProjectNode.class, source);
    }

    public static PlanMatchPattern project(Map<String, ExpressionAssignment> assignments, PlanMatchPattern source)
    {
        PlanMatchPattern result = project(source);
        assignments.entrySet().forEach(
                assignment -> result.withAlias(assignment.getKey(), assignment.getValue()));
        return result;
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return node(SemiJoinNode.class, source, filtering).with(new SemiJoinMatcher(sourceSymbolAlias, filteringSymbolAlias, outputAlias));
    }

    public static PlanMatchPattern join(JoinNode.Type joinType, List<EquiMaker> expectedEquiCriteria, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(JoinNode.class, left, right).with(new JoinMatcher(joinType, expectedEquiCriteria));
    }

    private static class SymbolAlias
    {
        private final String alias;

        private SymbolAlias(String alias)
        {
            this.alias = requireNonNull(alias, "alias is null");
        }

        Symbol toSymbol(ExpressionAliases aliases)
        {
            return Symbol.from(aliases.get(alias));
        }
    }

    static class EquiMaker
    {
        SymbolAlias left;
        SymbolAlias right;

        private EquiMaker(SymbolAlias left, SymbolAlias right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        JoinNode.EquiJoinClause rehydrate(ExpressionAliases aliases)
        {
            return new JoinNode.EquiJoinClause(left.toSymbol(aliases), right.toSymbol(aliases));
        }
    }

    static class FunctionCallMaker
    {
        QualifiedName name;
        List<SymbolAlias> args;

        private FunctionCallMaker(QualifiedName name, List<SymbolAlias> args)
        {
            this.name = requireNonNull(name, "name is null");
            this.args = requireNonNull(args, "args is null");
        }

        FunctionCall rehydrate(ExpressionAliases aliases)
        {
            List<Expression> symbolArgs = args
                    .stream()
                    .map(arg -> arg.toSymbol(aliases).toSymbolReference())
                    .collect(toImmutableList());

            return new FunctionCall(name, symbolArgs);
        }
    }

    public static EquiMaker equiJoinClause(String left, String right)
    {
        return new EquiMaker(new SymbolAlias(left), new SymbolAlias(right));
    }

    public static SymbolAlias symbol(String alias)
    {
        return new SymbolAlias(alias);
    }

    public static PlanMatchPattern filter(String predicate, PlanMatchPattern source)
    {
        Expression expectedPredicate = rewriteQualifiedNamesToSymbolReferences(new SqlParser().createExpression(predicate));
        return node(FilterNode.class, source).with(new FilterMatcher(expectedPredicate));
    }

    public static PlanMatchPattern apply(List<String> correlationSymbolAliases, PlanMatchPattern inputPattern, PlanMatchPattern subqueryPattern)
    {
        return node(ApplyNode.class, inputPattern, subqueryPattern).with(new CorrelationMatcher(correlationSymbolAliases));
    }

    private PlanMatchPattern(List<PlanMatchPattern> sourcePatterns)
    {
        requireNonNull(sourcePatterns, "sourcePatterns are null");

        this.sourcePatterns = ImmutableList.copyOf(sourcePatterns);
    }

    List<PlanMatchingState> downMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        ImmutableList.Builder<PlanMatchingState> states = ImmutableList.builder();
        if (anyTree) {
            int sourcesCount = node.getSources().size();
            if (sourcesCount > 1) {
                states.add(new PlanMatchingState(nCopies(sourcesCount, this), expressionAliases));
            }
            else {
                states.add(new PlanMatchingState(ImmutableList.of(this), expressionAliases));
            }
        }
        if (node.getSources().size() == sourcePatterns.size() && matchers.stream().allMatch(it -> it.downMatches(node))) {
            states.add(new PlanMatchingState(sourcePatterns, expressionAliases));
        }
        return states.build();
    }

    boolean upMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        return matchers.stream().allMatch(it -> it.upMatches(node, session, metadata, expressionAliases));
    }

    public PlanMatchPattern with(Matcher matcher)
    {
        matchers.add(matcher);
        return this;
    }

    public PlanMatchPattern withAlias(String alias, RvalueMatcher matcher)
    {
        matchers.add(new Alias(alias, matcher));
        return this;
    }

    public static RvalueMatcher columnReference(String tableName, String columnName)
    {
        return new ColumnReference(tableName, columnName);
    }

    public static ExpressionAssignment expression(String expression)
    {
        return new ExpressionAssignment(expression);
    }

    public PlanMatchPattern withOutputs(ImmutableList<String> aliases)
    {
        matchers.add(new OutputMatcher(aliases));
        return this;
    }

    public PlanMatchPattern matchToAnyNodeTree()
    {
        anyTree = true;
        return this;
    }

    public boolean isTerminated()
    {
        return sourcePatterns.isEmpty();
    }

    public static FunctionCallMaker functionCall(String name, List<String> args)
    {
        List<SymbolAlias> symbolArgs = args
                .stream()
                .map(PlanMatchPattern::symbol)
                .collect(toImmutableList());

        return new FunctionCallMaker(QualifiedName.of(name), symbolArgs);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        toString(builder, 0);
        return builder.toString();
    }

    private void toString(StringBuilder builder, int indent)
    {
        checkState(matchers.stream().filter(PlanNodeMatcher.class::isInstance).count() <= 1);

        builder.append(indentString(indent));
        if (anyTree) {
            builder.append("anyTree");
        }
        else {
            builder.append("node");
        }

        Optional<PlanNodeMatcher> planNodeMatcher = matchers.stream()
                .filter(PlanNodeMatcher.class::isInstance)
                .map(PlanNodeMatcher.class::cast)
                .findFirst();

        if (planNodeMatcher.isPresent()) {
            builder.append("(").append(planNodeMatcher.get().getNodeClass().getSimpleName()).append(")");
        }

        List<Matcher> matchersToPrint = matchers.stream()
                .filter(matcher -> !(matcher instanceof PlanNodeMatcher))
                .collect(toImmutableList());

        builder.append("\n");

        if (matchersToPrint.size() + sourcePatterns.size() == 0) {
            return;
        }

        for (Matcher matcher : matchersToPrint) {
            builder.append(indentString(indent + 1)).append(matcher.toString()).append("\n");
        }

        for (PlanMatchPattern pattern : sourcePatterns) {
            pattern.toString(builder, indent + 1);
        }
    }

    private String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }
}
