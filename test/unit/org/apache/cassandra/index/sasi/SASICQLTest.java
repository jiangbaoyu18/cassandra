/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sasi;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;

public class SASICQLTest extends CQLTester
{
    @Test
    public void testPaging() throws Throwable
    {
        for (boolean forceFlush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int primary key, v int);");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex';");

            for (int i = 0; i < 10; i++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?);", i, 1);

            flush(forceFlush);

            Session session = sessionNet();
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v = 1");
            stmt.setFetchSize(5);
            List<Row> rs = session.execute(stmt).all();
            Assert.assertEquals(10, rs.size());
            Assert.assertEquals(Sets.newHashSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                                rs.stream().map((i) -> i.getInt("pk")).collect(Collectors.toSet()));
        }
    }

    @Test
    public void testPagingWithClustering() throws Throwable
    {
        for (boolean forceFlush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex';");

            for (int i = 0; i < 10; i++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?);", i, 1, 1);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?);", i, 2, 1);
            }

            flush(forceFlush);

            Session session = sessionNet();
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v = 1");
            stmt.setFetchSize(5);
            List<Row> rs = session.execute(stmt).all();
            Assert.assertEquals(20, rs.size());
        }
    }

    /**
     * Tests that a client warning is issued on SASI index creation.
     */
    @Test
    public void testClientWarningOnCreate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ClientWarn.instance.captureWarnings();
        createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        List<String> warnings = ClientWarn.instance.getWarnings();

        Assert.assertNotNull(warnings);
        Assert.assertEquals(1, warnings.size());
        Assert.assertEquals(SASIIndex.USAGE_WARNING, warnings.get(0));
    }

    /**
     * Tests the configuration flag to disable SASI indexes.
     */
    @Test
    public void testDisableSASIIndexes()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        boolean enableSASIIndexes = DatabaseDescriptor.getEnableSASIIndexes();
        try
        {
            DatabaseDescriptor.setEnableSASIIndexes(false);
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
            Assert.fail("Should not be able to create a SASI index if they are disabled");
        }
        catch (RuntimeException e)
        {
            Throwable cause = e.getCause();
            Assert.assertNotNull(cause);
            Assert.assertTrue(cause instanceof InvalidRequestException);
            Assert.assertTrue(cause.getMessage().contains("SASI indexes are disabled"));
        }
        finally
        {
            DatabaseDescriptor.setEnableSASIIndexes(enableSASIIndexes);
        }
    }
}
