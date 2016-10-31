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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class PlanMatchingVisitor
        extends PlanVisitor<PlanMatchingContext, Boolean>
{
    private final Metadata metadata;
    private final Session session;

    PlanMatchingVisitor(Session session, Metadata metadata)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Boolean visitExchange(ExchangeNode node, PlanMatchingContext context)
    {
        checkState(node.getType() == ExchangeNode.Type.GATHER, "Only GATHER is supported");
        List<List<Symbol>> allInputs = node.getInputs();
        checkState(allInputs.size() == 1, "I don't know what to do with all these inputs");

        List<Symbol> inputs = allInputs.get(0);
        List<Symbol> outputs = node.getOutputSymbols();

        checkState(inputs.size() == outputs.size(), "Inputs/outputs size mismatch");

        ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
        for (int i = 0; i < inputs.size(); ++i) {
            assignments.put(outputs.get(i), inputs.get(i).toSymbolReference());
        }

        boolean result = super.visitExchange(node, context);
        if (result) {
            context.getExpressionAliases().updateAssignments(assignments.build());
        }
        return result;
    }

    @Override
    public Boolean visitProject(ProjectNode node, PlanMatchingContext context)
    {
        boolean result = super.visitProject(node, context);
        if (result) {
            context.getExpressionAliases().updateAssignments(node.getAssignments());
        }
        return result;
    }

    @Override
    protected Boolean visitPlan(PlanNode node, PlanMatchingContext context)
    {
        List<PlanMatchingState> states = context.getPattern().downMatches(node, session, metadata, context.getExpressionAliases());

        if (states.isEmpty()) {
            return false;
        }

        if (node.getSources().isEmpty()) {
            int terminatedUpMatchCount = 0;
            for (PlanMatchingState state : states) {
                if (!state.isTerminated()) {
                    continue;
                }
                if (context.getPattern().upMatches(node, session, metadata, context.getExpressionAliases())) {
                    terminatedUpMatchCount++;
                }
            }

            checkState(terminatedUpMatchCount < 2, format("Ambiguous shape match on leaf node %s", node));
            return terminatedUpMatchCount == 1;
        }

        for (PlanMatchingState state : states) {
            checkState(node.getSources().size() == state.getPatterns().size(), "Matchers count does not match count of sources");
            int i = 0;
            boolean sourcesMatch = true;
            ExpressionAliases stateAliases = new ExpressionAliases();
            for (PlanNode source : node.getSources()) {
                PlanMatchingContext sourceContext = state.createContext(i++);
                sourcesMatch = sourcesMatch && source.accept(this, sourceContext);
                if (!sourcesMatch) {
                    break;
                }
                stateAliases.putSourceAliases(sourceContext.getExpressionAliases());
            }
            if (sourcesMatch && context.getPattern().upMatches(node, session, metadata, stateAliases)) {
                context.getExpressionAliases().putSourceAliases(stateAliases);
                return true;
            }
        }
        return false;
    }

    private List<PlanMatchingState> filterTerminated(List<PlanMatchingState> states)
    {
        return states.stream()
                .filter(PlanMatchingState::isTerminated)
                .collect(toImmutableList());
    }
}
