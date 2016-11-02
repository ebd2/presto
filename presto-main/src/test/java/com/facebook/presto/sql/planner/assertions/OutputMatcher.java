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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class OutputMatcher
    implements Matcher
{
    private final List<String> aliases;

    public OutputMatcher(ImmutableList<String> aliases)
    {
        this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
    }

    @Override
    public boolean downMatches(PlanNode node)
    {
        return true;
    }

    @Override
    public boolean upMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        int i = 0;
        for (String alias : aliases) {
            Expression expression = expressionAliases.get(alias);
            boolean found = false;
            while (i < node.getOutputSymbols().size()) {
                Symbol outputSymbol = node.getOutputSymbols().get(i++);
                if (expression.equals(outputSymbol.toSymbolReference())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }
}
