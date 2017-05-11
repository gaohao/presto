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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.kafka.KafkaColumnHandle.ColumnType;
import static com.facebook.presto.kafka.KafkaUtil.findAllOffsets;
import static com.facebook.presto.kafka.KafkaUtil.selectRandom;
import static java.util.Objects.requireNonNull;

public class KafkaPartitionManager
{
    private static final Logger log = Logger.get(KafkaUtil.class);

    private final String connectorId;
    private final KafkaSimpleConsumerManager consumerManager;
    private final Set<HostAddress> nodes;

    @Inject
    public KafkaPartitionManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaSimpleConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.nodes = ImmutableSet.copyOf(kafkaConnectorConfig.getNodes());
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");
    }

    public static void main(String[] args) throws Exception
    {
//        KafkaPartitionManager km = new KafkaPartitionManager();
//        km.printSmallest(1);
//        km.printLargest(1);
    }

    public KafkaPartitionResult getPartitions(ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        ImmutableList.Builder<KafkaPartition> partitions = ImmutableList.builder();
        List<KafkaColumnHandle> partitionColumns = getPartitionColumns(tableHandle);
        SimpleConsumer simpleConsumer = consumerManager.getConsumer(selectRandom(nodes));

        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of("event"));
        TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

        for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
            for (PartitionMetadata part : metadata.partitionsMetadata()) {
                log.debug("Adding Partition %s/%s", metadata.topic(), part.partitionId());

                Broker leader = part.leader();
                if (leader == null) { // Leader election going on...
                    log.warn("No leader for partition %s/%s found!", metadata.topic(), part.partitionId());
                    continue;
                }

                HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());

                SimpleConsumer leaderConsumer = consumerManager.getConsumer(partitionLeader);

                // Kafka contains a reverse list of "end - start" pairs for the splits
                long[] offsets = findAllOffsets(leaderConsumer,  metadata.topic(), part.partitionId());

                for (int i = offsets.length - 1; i > 0; i--) {
                    ImmutableMap.Builder<ColumnHandle, NullableValue> keysBuilder = ImmutableMap.builder();
                    keysBuilder.put(partitionColumns.get(0), NullableValue.of(BigintType.BIGINT, (long) part.partitionId()));
                    keysBuilder.put(partitionColumns.get(1), NullableValue.of(BigintType.BIGINT, offsets[i]));
                    keysBuilder.put(partitionColumns.get(2), NullableValue.of(BigintType.BIGINT, offsets[i - 1]));
                    partitions.add(new KafkaPartition(keysBuilder.build()));
                }
            }
        }

        return new KafkaPartitionResult(partitionColumns, partitions.build());
    }

    public List<ColumnMetadata> getPartitionColumnsMetadata()
    {
        List<ColumnMetadata> partitionColumns = new ArrayList<>();
        partitionColumns.add(new ColumnMetadata("partition_id", BigintType.BIGINT, "partition key", "partition key", false));
        partitionColumns.add(new ColumnMetadata("offset_start", BigintType.BIGINT, "offset start", "partition key", false));
        partitionColumns.add(new ColumnMetadata("offset_end", BigintType.BIGINT, "offset end", "partition key", false));
        return partitionColumns;
    }

    public List<ColumnHandle> getPartitionColumns(ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        List<ColumnHandle> partitionColumns = new ArrayList<>();
        partitionColumns.add(new KafkaColumnHandle(connectorId, "partition_id", BigintType.BIGINT, null, null, null, false, ColumnType.PARTITION_KEY, true));
        partitionColumns.add(new KafkaColumnHandle(connectorId, "offset_start", BigintType.BIGINT, null, null, null, false, ColumnType.PARTITION_KEY, true));
        partitionColumns.add(new KafkaColumnHandle(connectorId, "offset_end", BigintType.BIGINT, null, null, null, false, ColumnType.PARTITION_KEY, true));
        return partitionColumns;
    }

    public List<KafkaColumnHandle> getPartitionColumns(ConnectorTableHandle tableHandle)
    {
        ImmutableList.Builder<KafkaColumnHandle> partitionColumns = ImmutableList.builder();
        partitionColumns.add(new KafkaColumnHandle(connectorId, "partition_id", BigintType.BIGINT, null, null, null, false, ColumnType.PARTITION_KEY, true));
        partitionColumns.add(new KafkaColumnHandle(connectorId, "offset_start", BigintType.BIGINT, null, null, null, false, ColumnType.PARTITION_KEY, true));
        partitionColumns.add(new KafkaColumnHandle(connectorId, "offset_end", BigintType.BIGINT, null, null, null, false, ColumnType.PARTITION_KEY, true));
        return partitionColumns.build();
    }
}
