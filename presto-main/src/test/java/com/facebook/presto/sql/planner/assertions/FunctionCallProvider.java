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

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;

import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

class FunctionCallProvider
    implements ExpectedValueProvider<FunctionCall>
{
    private final QualifiedName name;
    private final List<SymbolAlias> args;

    FunctionCallProvider(QualifiedName name, List<SymbolAlias> args)
    {
        this.name = requireNonNull(name, "name is null");
        this.args = requireNonNull(args, "args is null");
    }

    public FunctionCall getExpectedValue(ExpressionAliases aliases)
    {
        List<Expression> symbolArgs = args
                .stream()
                .map(arg -> arg.toSymbol(aliases).toSymbolReference())
                .collect(toImmutableList());

        return new FunctionCall(name, symbolArgs);
    }
}
