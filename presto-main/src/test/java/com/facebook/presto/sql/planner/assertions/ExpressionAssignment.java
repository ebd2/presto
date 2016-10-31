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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.ExpressionUtils.rewriteQualifiedNamesToSymbolReferences;
import static com.facebook.presto.sql.planner.PlanPrinter.textLogicalPlan;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExpressionAssignment
    implements HackMatcher
{
    private final SqlParser parser = new SqlParser();
    private final Expression expression;

    public ExpressionAssignment(String expression)
    {
        this.expression = expression(requireNonNull(expression));
    }

    private Expression expression(String sql)
    {
        return rewriteQualifiedNamesToSymbolReferences(parser.createExpression(sql));
    }

    @Override
    public Optional<Symbol> getSymbol(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        Optional<Symbol> result = Optional.empty();
        ImmutableList.Builder<Expression> matchesBuilder = ImmutableList.builder();

        if (!(node instanceof ProjectNode)) {
            return result;
        }

        ProjectNode projectNode = (ProjectNode) node;
        ExpressionVerifier verifier = new ExpressionVerifier(expressionAliases);

        for (Map.Entry<Symbol, Expression> assignment : projectNode.getAssignments().entrySet()) {
            if (verifier.process(assignment.getValue(), expression)) {
                result = Optional.of(assignment.getKey());
                matchesBuilder.add(assignment.getValue());
            }
        }

        ImmutableList<Expression> matches = matchesBuilder.build();
        checkState(matches.size() < 2, "Ambiguous expression %s matches multiple assignments", expression,
                Joiner.on(", ").join((matches.stream().map(Expression::toString).collect(Collectors.toList()))));
        return result;
    }
}
