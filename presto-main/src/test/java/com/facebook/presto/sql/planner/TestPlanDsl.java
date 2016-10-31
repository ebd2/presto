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

import com.facebook.presto.sql.planner.assertions.HackMatcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
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
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class TestPlanDsl
{
    private final LocalQueryRunner queryRunner;
    private final HackMatcher lineitemOrderkeyColumn;

    public TestPlanDsl()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        lineitemOrderkeyColumn = columnReference("lineitem", "orderkey");
    }

    @Test
    public void testOutput()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn))
                .withOutput("ORDERKEY"));
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
                node(OutputNode.class,
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))
                .withOutput("ORDERKEY"));
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

    @Test(expectedExceptions = { NullPointerException.class })
    public void testBadColumn()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "NXCOLUMN")))
                .withOutput("NXALIAS"));
    }

    @Test(expectedExceptions = { NullPointerException.class })
    public void testBadAlias()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", lineitemOrderkeyColumn))
                        .withOutput("NXALIAS"));
    }

    @Test(expectedExceptions = { IllegalStateException.class })
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

    private void assertPlan(String sql, PlanMatchPattern pattern)
    {
        assertPlan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, pattern);
    }

    private void assertPlan(String sql, LogicalPlanner.Stage stage, PlanMatchPattern pattern)
    {
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, stage);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan plan(String sql)
    {
        return plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED);
    }

    private Plan plan(String sql, LogicalPlanner.Stage stage)
    {
        try {
            return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, stage));
        }
        catch (RuntimeException ex) {
            fail("Invalid SQL: " + sql, ex);
            return null; // make compiler happy
        }
    }
}
