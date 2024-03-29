/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Entries in indexes on non-compact tables (tables with composite comparators)
 * can be encapsulated as IndexedEntry instances. These are not used when dealing
 * with indexes on static/compact/thrift tables (i.e. KEYS indexes).
 */
public final class IndexEntry
{
    public final DecoratedKey indexValue;  // 创建索引的列的值
    public final Clustering indexClustering; // 索引表中的 clustering keys (base table 的 primary key )
    public final long timestamp;

    public final ByteBuffer indexedKey;     // 索引表中clustering key 的第一列 （base table 的 partition key ）
    public final Clustering indexedEntryClustering; // 索引表中clustering keys,除了第一列 （base table 的 clustering key ）

    public IndexEntry(DecoratedKey indexValue,
                      Clustering indexClustering,
                      long timestamp,
                      ByteBuffer indexedKey,
                      Clustering indexedEntryClustering)
    {
        this.indexValue = indexValue;
        this.indexClustering = indexClustering;
        this.timestamp = timestamp;
        this.indexedKey = indexedKey;
        this.indexedEntryClustering = indexedEntryClustering;
    }
}
