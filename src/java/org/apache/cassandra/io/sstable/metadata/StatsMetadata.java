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
package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.StreamingHistogram;

/**
 * SSTable metadata that always stay on heap.
 */
public class StatsMetadata extends MetadataComponent
{
    public static final IMetadataComponentSerializer serializer = new StatsMetadataSerializer();
    public static final ISerializer<IntervalSet<CommitLogPosition>> commitLogPositionSetSerializer = IntervalSet.serializer(CommitLogPosition.serializer);

    public final EstimatedHistogram estimatedPartitionSize;
    public final EstimatedHistogram estimatedColumnCount;
    public final IntervalSet<CommitLogPosition> commitLogIntervals;
    public final long minTimestamp; //最早一次更新时间
    public final long maxTimestamp; //最后一次更新时间
    public final int minLocalDeletionTime;//最早一个tombstone的创建时间
    public final int maxLocalDeletionTime;//最后一个tombstone的创建时间
    public final int minTTL;
    public final int maxTTL;
    public final double compressionRatio;
    public final StreamingHistogram estimatedTombstoneDropTime;
    public final int sstableLevel;
    public final List<ByteBuffer> minClusteringValues; // 分别对应每个clustering key 在该sstable中的最小值
    public final List<ByteBuffer> maxClusteringValues;
    public final boolean hasLegacyCounterShards;
    public final long repairedAt;
    public final long totalColumnsSet;
    public final long totalRows;

    public StatsMetadata(EstimatedHistogram estimatedPartitionSize,
                         EstimatedHistogram estimatedColumnCount,
                         IntervalSet<CommitLogPosition> commitLogIntervals,
                         long minTimestamp,
                         long maxTimestamp,
                         int minLocalDeletionTime,
                         int maxLocalDeletionTime,
                         int minTTL,
                         int maxTTL,
                         double compressionRatio,
                         StreamingHistogram estimatedTombstoneDropTime,
                         int sstableLevel,
                         List<ByteBuffer> minClusteringValues,
                         List<ByteBuffer> maxClusteringValues,
                         boolean hasLegacyCounterShards,
                         long repairedAt,
                         long totalColumnsSet,
                         long totalRows)
    {
        this.estimatedPartitionSize = estimatedPartitionSize;
        this.estimatedColumnCount = estimatedColumnCount;
        this.commitLogIntervals = commitLogIntervals;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.minLocalDeletionTime = minLocalDeletionTime;
        this.maxLocalDeletionTime = maxLocalDeletionTime;
        this.minTTL = minTTL;
        this.maxTTL = maxTTL;
        this.compressionRatio = compressionRatio;
        this.estimatedTombstoneDropTime = estimatedTombstoneDropTime;
        this.sstableLevel = sstableLevel;
        this.minClusteringValues = minClusteringValues;
        this.maxClusteringValues = maxClusteringValues;
        this.hasLegacyCounterShards = hasLegacyCounterShards;
        this.repairedAt = repairedAt;
        this.totalColumnsSet = totalColumnsSet;
        this.totalRows = totalRows;
    }

    public MetadataType getType()
    {
        return MetadataType.STATS;
    }

    /**
     * @param gcBefore gc time in seconds
     * @return estimated droppable tombstone ratio at given gcBefore time.
     */
    public double getEstimatedDroppableTombstoneRatio(int gcBefore)
    {
        long estimatedColumnCount = this.estimatedColumnCount.mean() * this.estimatedColumnCount.count();
        if (estimatedColumnCount > 0)
        {
            double droppable = getDroppableTombstonesBefore(gcBefore);
            return droppable / estimatedColumnCount;
        }
        return 0.0f;
    }

    /**
     * @param gcBefore gc time in seconds
     * @return amount of droppable tombstones
     */
    public double getDroppableTombstonesBefore(int gcBefore)
    {
        return estimatedTombstoneDropTime.sum(gcBefore);
    }

    public StatsMetadata mutateLevel(int newLevel)
    {
        return new StatsMetadata(estimatedPartitionSize,
                                 estimatedColumnCount,
                                 commitLogIntervals,
                                 minTimestamp,
                                 maxTimestamp,
                                 minLocalDeletionTime,
                                 maxLocalDeletionTime,
                                 minTTL,
                                 maxTTL,
                                 compressionRatio,
                                 estimatedTombstoneDropTime,
                                 newLevel,
                                 minClusteringValues,
                                 maxClusteringValues,
                                 hasLegacyCounterShards,
                                 repairedAt,
                                 totalColumnsSet,
                                 totalRows);
    }

    public StatsMetadata mutateRepairedAt(long newRepairedAt)
    {
        return new StatsMetadata(estimatedPartitionSize,
                                 estimatedColumnCount,
                                 commitLogIntervals,
                                 minTimestamp,
                                 maxTimestamp,
                                 minLocalDeletionTime,
                                 maxLocalDeletionTime,
                                 minTTL,
                                 maxTTL,
                                 compressionRatio,
                                 estimatedTombstoneDropTime,
                                 sstableLevel,
                                 minClusteringValues,
                                 maxClusteringValues,
                                 hasLegacyCounterShards,
                                 newRepairedAt,
                                 totalColumnsSet,
                                 totalRows);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsMetadata that = (StatsMetadata) o;
        return new EqualsBuilder()
                       .append(estimatedPartitionSize, that.estimatedPartitionSize)
                       .append(estimatedColumnCount, that.estimatedColumnCount)
                       .append(commitLogIntervals, that.commitLogIntervals)
                       .append(minTimestamp, that.minTimestamp)
                       .append(maxTimestamp, that.maxTimestamp)
                       .append(minLocalDeletionTime, that.minLocalDeletionTime)
                       .append(maxLocalDeletionTime, that.maxLocalDeletionTime)
                       .append(minTTL, that.minTTL)
                       .append(maxTTL, that.maxTTL)
                       .append(compressionRatio, that.compressionRatio)
                       .append(estimatedTombstoneDropTime, that.estimatedTombstoneDropTime)
                       .append(sstableLevel, that.sstableLevel)
                       .append(repairedAt, that.repairedAt)
                       .append(maxClusteringValues, that.maxClusteringValues)
                       .append(minClusteringValues, that.minClusteringValues)
                       .append(hasLegacyCounterShards, that.hasLegacyCounterShards)
                       .append(totalColumnsSet, that.totalColumnsSet)
                       .append(totalRows, that.totalRows)
                       .build();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                       .append(estimatedPartitionSize)
                       .append(estimatedColumnCount)
                       .append(commitLogIntervals)
                       .append(minTimestamp)
                       .append(maxTimestamp)
                       .append(minLocalDeletionTime)
                       .append(maxLocalDeletionTime)
                       .append(minTTL)
                       .append(maxTTL)
                       .append(compressionRatio)
                       .append(estimatedTombstoneDropTime)
                       .append(sstableLevel)
                       .append(repairedAt)
                       .append(maxClusteringValues)
                       .append(minClusteringValues)
                       .append(hasLegacyCounterShards)
                       .append(totalColumnsSet)
                       .append(totalRows)
                       .build();
    }

    public static class StatsMetadataSerializer implements IMetadataComponentSerializer<StatsMetadata>
    {
        public int serializedSize(Version version, StatsMetadata component) throws IOException
        {
            int size = 0;
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedPartitionSize);
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedColumnCount);
            size += CommitLogPosition.serializer.serializedSize(component.commitLogIntervals.upperBound().orElse(CommitLogPosition.NONE));
            if (version.storeRows())
                size += 8 + 8 + 4 + 4 + 4 + 4 + 8 + 8; // mix/max timestamp(long), min/maxLocalDeletionTime(int), min/max TTL, compressionRatio(double), repairedAt (long)
            else
                size += 8 + 8 + 4 + 8 + 8; // mix/max timestamp(long), maxLocalDeletionTime(int), compressionRatio(double), repairedAt (long)
            size += StreamingHistogram.serializer.serializedSize(component.estimatedTombstoneDropTime);
            size += TypeSizes.sizeof(component.sstableLevel);
            // min column names
            size += 4;
            for (ByteBuffer value : component.minClusteringValues)
                size += 2 + value.remaining(); // with short length
            // max column names
            size += 4;
            for (ByteBuffer value : component.maxClusteringValues)
                size += 2 + value.remaining(); // with short length
            size += TypeSizes.sizeof(component.hasLegacyCounterShards);
            if (version.storeRows())
                size += 8 + 8; // totalColumnsSet, totalRows
            if (version.hasCommitLogLowerBound())
                size += CommitLogPosition.serializer.serializedSize(component.commitLogIntervals.lowerBound().orElse(CommitLogPosition.NONE));
            if (version.hasCommitLogIntervals())
                size += commitLogPositionSetSerializer.serializedSize(component.commitLogIntervals);
            return size;
        }

        public void serialize(Version version, StatsMetadata component, DataOutputPlus out) throws IOException
        {
            EstimatedHistogram.serializer.serialize(component.estimatedPartitionSize, out);
            EstimatedHistogram.serializer.serialize(component.estimatedColumnCount, out);
            CommitLogPosition.serializer.serialize(component.commitLogIntervals.upperBound().orElse(CommitLogPosition.NONE), out);
            out.writeLong(component.minTimestamp);
            out.writeLong(component.maxTimestamp);
            if (version.storeRows())
                out.writeInt(component.minLocalDeletionTime);
            out.writeInt(component.maxLocalDeletionTime);
            if (version.storeRows())
            {
                out.writeInt(component.minTTL);
                out.writeInt(component.maxTTL);
            }
            out.writeDouble(component.compressionRatio);
            StreamingHistogram.serializer.serialize(component.estimatedTombstoneDropTime, out);
            out.writeInt(component.sstableLevel);
            out.writeLong(component.repairedAt);
            out.writeInt(component.minClusteringValues.size());
            for (ByteBuffer value : component.minClusteringValues)
                ByteBufferUtil.writeWithShortLength(value, out);
            out.writeInt(component.maxClusteringValues.size());
            for (ByteBuffer value : component.maxClusteringValues)
                ByteBufferUtil.writeWithShortLength(value, out);
            out.writeBoolean(component.hasLegacyCounterShards);

            if (version.storeRows())
            {
                out.writeLong(component.totalColumnsSet);
                out.writeLong(component.totalRows);
            }

            if (version.hasCommitLogLowerBound())
                CommitLogPosition.serializer.serialize(component.commitLogIntervals.lowerBound().orElse(CommitLogPosition.NONE), out);
            if (version.hasCommitLogIntervals())
                commitLogPositionSetSerializer.serialize(component.commitLogIntervals, out);
        }

        public StatsMetadata deserialize(Version version, DataInputPlus in) throws IOException
        {
            EstimatedHistogram partitionSizes = EstimatedHistogram.serializer.deserialize(in);
            EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);
            CommitLogPosition commitLogLowerBound = CommitLogPosition.NONE, commitLogUpperBound;
            commitLogUpperBound = CommitLogPosition.serializer.deserialize(in);
            long minTimestamp = in.readLong();
            long maxTimestamp = in.readLong();
            // We use MAX_VALUE as that's the default value for "no deletion time"
            int minLocalDeletionTime = version.storeRows() ? in.readInt() : Integer.MAX_VALUE;
            int maxLocalDeletionTime = in.readInt();
            int minTTL = version.storeRows() ? in.readInt() : 0;
            int maxTTL = version.storeRows() ? in.readInt() : Integer.MAX_VALUE;
            double compressionRatio = in.readDouble();
            StreamingHistogram tombstoneHistogram = StreamingHistogram.serializer.deserialize(in);
            int sstableLevel = in.readInt();
            long repairedAt = 0;
            if (version.hasRepairedAt())
                repairedAt = in.readLong();

            // for legacy sstables, we skip deserializing the min and max clustering value
            // to prevent erroneously excluding sstables from reads (see CASSANDRA-14861)
            int colCount = in.readInt();
            List<ByteBuffer> minClusteringValues = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++)
            {
                ByteBuffer val = ByteBufferUtil.readWithShortLength(in);
                if (version.hasAccurateMinMax())
                    minClusteringValues.add(val);
            }

            colCount = in.readInt();
            List<ByteBuffer> maxClusteringValues = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++)
            {
                ByteBuffer val = ByteBufferUtil.readWithShortLength(in);
                if (version.hasAccurateMinMax())
                    maxClusteringValues.add(val);
            }

            boolean hasLegacyCounterShards = true;
            if (version.tracksLegacyCounterShards())
                hasLegacyCounterShards = in.readBoolean();

            long totalColumnsSet = version.storeRows() ? in.readLong() : -1L;
            long totalRows = version.storeRows() ? in.readLong() : -1L;

            if (version.hasCommitLogLowerBound())
                commitLogLowerBound = CommitLogPosition.serializer.deserialize(in);
            IntervalSet<CommitLogPosition> commitLogIntervals;
            if (version.hasCommitLogIntervals())
                commitLogIntervals = commitLogPositionSetSerializer.deserialize(in);
            else
                commitLogIntervals = new IntervalSet<CommitLogPosition>(commitLogLowerBound, commitLogUpperBound);

            return new StatsMetadata(partitionSizes,
                                     columnCounts,
                                     commitLogIntervals,
                                     minTimestamp,
                                     maxTimestamp,
                                     minLocalDeletionTime,
                                     maxLocalDeletionTime,
                                     minTTL,
                                     maxTTL,
                                     compressionRatio,
                                     tombstoneHistogram,
                                     sstableLevel,
                                     minClusteringValues,
                                     maxClusteringValues,
                                     hasLegacyCounterShards,
                                     repairedAt,
                                     totalColumnsSet,
                                     totalRows);
        }
    }
}
