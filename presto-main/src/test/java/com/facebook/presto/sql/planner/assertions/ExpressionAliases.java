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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExpressionAliases
{
    private final Map<String, Expression> map;

    public ExpressionAliases()
    {
        this.map = new HashMap<>();
    }

    public ExpressionAliases(ExpressionAliases expressionAliases)
    {
        requireNonNull(expressionAliases, "symbolAliases are null");
        this.map = new HashMap<>(expressionAliases.map);
    }

    public void put(String alias, Expression expression)
    {
        alias = alias(alias);
        checkState(!map.containsKey(alias), "Alias '%s' points to different expression '%s' and '%s'", alias, expression, map.get(alias));
        checkState(!map.values().contains(expression), "Expression '%s' is already pointed by different alias than '%s', check mapping for '%s'", expression, alias, map);
        map.put(alias, expression);
    }

    public void putSourceAliases(ExpressionAliases sourceAliases)
    {
        for (Map.Entry<String, Expression> alias : sourceAliases.map.entrySet()) {
            put(alias.getKey(), alias.getValue());
        }
    }

    public Expression get(String alias)
    {
        alias = alias(alias);
        Expression result = map.get(alias);
        requireNonNull(result, format("missing expression for alias %s", alias));
        return result;
    }

    private String alias(String alias)
    {
        return alias.toLowerCase().replace("(", "").replace(")", "").replace("\"", "");
    }

    public void updateAssignments(Map<Symbol, Expression> assignments)
    {
        ImmutableMap.Builder<String, Expression> mapUpdate = ImmutableMap.builder();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            for (Map.Entry<String, Expression> existingAlias : map.entrySet()) {
                if (assignment.getValue().equals(existingAlias.getValue())) {
                    mapUpdate.put(existingAlias.getKey(), assignment.getKey().toSymbolReference());
                }
            }
        }
        map.putAll(mapUpdate.build());
    }
}
