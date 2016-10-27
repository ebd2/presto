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

import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.columnReference;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class TestPlanDsl
{
    private final LocalQueryRunner queryRunner;

    public TestPlanDsl()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testOutput()
    {
        assertPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "orderkey")))
                .withOutput("ORDERKEY"));
    }

    @Test
    public void testSymbolChange()
    {
        assertPlan("SELECT orderkey AS okeydokey FROM lineitem",
                node(OutputNode.class,
                        any(
                                node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "orderkey"))))
                        .withOutput("ORDERKEY"));
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
