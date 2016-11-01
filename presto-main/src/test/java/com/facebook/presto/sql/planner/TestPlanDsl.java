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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanDslTest;
import com.facebook.presto.sql.planner.assertions.HackMatcher;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregate;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.columnReference;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.symbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestPlanDsl extends BasePlanDslTest
{
    private final HackMatcher lineitemOrderkeyColumn;

    public TestPlanDsl()
    {
        super();

        lineitemOrderkeyColumn = columnReference("lineitem", "orderkey");
    }

    @Test
    public void testOutput()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn))
                .withOutputs(ImmutableList.of("ORDERKEY")));
    }

    @Test
    public void testOutputSameColumnMultipleTimes()
    {
        assertPlan("SELECT orderkey, orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "ORDERKEY"),
                        node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn)));
    }

    @Test
    public void testOutputTooFewOutputs()
    {
        assertNotPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "ORDERKEY"),
                        node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn)));
    }

    @Test
    public void testOutputSameColumnMultipleTimesWithOtherOutputs()
    {
        assertPlan("SELECT extendedprice, orderkey, discount, orderkey, linenumber FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "ORDERKEY"),
                        /*
                         * This is a project node, but this gives us a convenient way to verify that
                         * visitProject is correctly handled through an anyTree.
                         */
                        anyTree(
                                node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn))));
    }

    @Test
    public void testUnreferencedSymbolsDontNeedBinding()
    {
        assertPlan("SELECT orderkey, 2 FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        anyTree(
                                node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn))));
    }

    @Test
    public void testAliasConstantFromProject()
    {
        assertPlan("SELECT orderkey, 2 FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "TWO"),
                        project(ImmutableMap.of("TWO", expression("2")),
                                tableScan("lineitem").withAlias("ORDERKEY", lineitemOrderkeyColumn))));
    }

    @Test
    public void testTableScan()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))));
    }

    @Test
    public void testMagic()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders").withAlias("ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem").withAlias("LINEITEM_OK", columnReference("lineitem", "orderkey"))))));
    }

    @Test
    public void testAggregation()
    {
        assertPlan("SELECT COUNT(nationkey) FROM nation",
                output(ImmutableList.of("FINAL_COUNT"),
                        aggregate(ImmutableMap.of("FINAL_COUNT", functionCall("count", symbol("PARTIAL_COUNT"))),
                                any(
                                        aggregate(ImmutableMap.of("PARTIAL_COUNT", functionCall("count", symbol("NATIONKEY"))),
                                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".* doesn't have column .*")
    public void testBadColumn()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "NXCOLUMN"))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = "missing expression for alias .*")
    public void testBadAlias()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("NXALIAS"),
                        node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn)));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".*already bound to expression.*")
    public void testDuplicateAliases()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders").withAlias("ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem").withAlias("ORDERS_OK", columnReference("lineitem", "orderkey"))))));
    }

    @Test(expectedExceptions = { IllegalStateException.class }, expectedExceptionsMessageRegExp = ".*already bound in.*")
    public void testBindMultipleAliasesSameExpression()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "TWO"),
                        tableScan("lineitem")
                                .withAlias("FIRST", lineitemOrderkeyColumn)
                                .withAlias("SECOND", lineitemOrderkeyColumn)));
    }
}
