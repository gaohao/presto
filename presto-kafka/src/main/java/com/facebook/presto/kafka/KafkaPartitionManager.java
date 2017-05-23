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
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.kafka.KafkaUtil.selectRandom;
import static java.util.Objects.requireNonNull;

public class KafkaPartitionManager
{
    private static final Logger log = Logger.get(KafkaUtil.class);

    private final String connectorId;
    private final KafkaSimpleConsumerManager consumerManager;
    private final Set<HostAddress> nodes;
    private final String metastoreHost;
    private final int metastoreIndex;

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
        this.metastoreHost = kafkaConnectorConfig.getMetastoreHost();
        this.metastoreIndex = kafkaConnectorConfig.getMetastoreIndex();
    }

    public static void main(String[] args) throws Exception
    {
//        getPartitionsFromMetastore("");
    }

    public KafkaPartitionResult getPartitions(ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) tableHandle;
        ImmutableList.Builder<KafkaPartition> partitions = ImmutableList.builder();
        List<KafkaColumnHandle> partitionColumns = getPartitionColumns(tableHandle);
        SimpleConsumer simpleConsumer = consumerManager.getConsumer(selectRandom(nodes));

        String topicName = kafkaTableHandle.getTopicName();
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of(topicName));
        TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);
        ImmutableMap.Builder<Integer, HostAddress> partitionWithLeader = ImmutableMap.builder();
        for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
            for (PartitionMetadata part : metadata.partitionsMetadata()) {
                Broker leader = part.leader();
                if (leader == null) { // Leader election going on...
                    log.warn("No leader for partition %s/%s found!", metadata.topic(), part.partitionId());
                    continue;
                }

                HostAddress partitionLeader = HostAddress.fromParts(leader.host(), leader.port());
                partitionWithLeader.put(part.partitionId(), partitionLeader);
//                SimpleConsumer leaderConsumer = consumerManager.getConsumer(partitionLeader);
//
//                // Kafka contains a reverse list of "end - start" pairs for the splits
//                long[] offsets = findAllOffsets(leaderConsumer,  metadata.topic(), part.partitionId());
//
//                for (int i = offsets.length - 1; i > 0; i--) {
//                    ImmutableMap.Builder<ColumnHandle, NullableValue> partitionValuesBuilder = ImmutableMap.builder();
//                    partitionValuesBuilder.put(partitionColumns.get(0), NullableValue.of(BigintType.BIGINT, (long) part.partitionId()));
//                    partitionValuesBuilder.put(partitionColumns.get(1), NullableValue.of(BigintType.BIGINT, offsets[i]));
//                    partitionValuesBuilder.put(partitionColumns.get(2), NullableValue.of(BigintType.BIGINT, offsets[i - 1]));
//                    ImmutableMap<ColumnHandle, NullableValue> partitionValues = partitionValuesBuilder.build();
//                    if (constraint.predicate().test(partitionValues)) {
//                        log.debug("Adding Partition %s/%s", metadata.topic(), part.partitionId());
//                        partitions.add(new KafkaPartition(partitionValues, partitionLeader, part.partitionId(), offsets[i], offsets[i - 1]));
//                    }
//                }
            }
        }

        return new KafkaPartitionResult(partitionColumns,
                getPartitionsFromMetastore(topicName, partitionWithLeader.build(), partitionColumns, constraint));
    }

    public List<KafkaPartition> getPartitionsFromMetastore(String topicName, Map<Integer, HostAddress> partitionWithLeader,
                                                                  List<KafkaColumnHandle> partitionColumns, Constraint<ColumnHandle> constraint)
    {
        ImmutableList.Builder<KafkaPartition> partitions = ImmutableList.builder();

        Jedis jedis = new Jedis(metastoreHost);
        jedis.select(metastoreIndex);
        Set<String> topicTsVals = jedis.zrange(topicName, 1, -1);
        for (String topicTsVal : topicTsVals) {
            System.out.println(topicTsVal);
            String[] topicTsPair = topicTsVal.split(":");
            Set<String> partitionOffsetVals = jedis.smembers(topicTsVal);
            for (String partitionOffsetVal : partitionOffsetVals) {
                String[] partitionOffsetPair = partitionOffsetVal.split(":");
                System.out.println(partitionOffsetVal);
                ImmutableMap.Builder<ColumnHandle, NullableValue> partitionValuesBuilder = ImmutableMap.builder();
                partitionValuesBuilder.put(partitionColumns.get(0), NullableValue.of(BigintType.BIGINT, Long.parseLong(partitionOffsetPair[0])));
                partitionValuesBuilder.put(partitionColumns.get(1), NullableValue.of(BigintType.BIGINT, Long.parseLong(partitionOffsetPair[1])));
                partitionValuesBuilder.put(partitionColumns.get(2), NullableValue.of(BigintType.BIGINT, Long.parseLong(partitionOffsetPair[2])));
                partitionValuesBuilder.put(partitionColumns.get(3), NullableValue.of(BigintType.BIGINT, Long.parseLong(topicTsPair[1])));
                ImmutableMap<ColumnHandle, NullableValue> partitionValues = partitionValuesBuilder.build();
                if (constraint.predicate().test(partitionValues)) {
                    partitions.add(new KafkaPartition(partitionValues, partitionWithLeader.get(Integer.parseInt(partitionOffsetPair[0])),
                            Integer.parseInt(partitionOffsetPair[0]), Long.parseLong(partitionOffsetPair[1]), Long.parseLong(partitionOffsetPair[2]),
                            Long.parseLong(topicTsPair[1])));
                }
            }
        }
        return partitions.build();
    }

    public List<KafkaColumnHandle> getPartitionColumns(ConnectorTableHandle tableHandle)
    {
        ImmutableList.Builder<KafkaColumnHandle> partitionColumns = ImmutableList.builder();
        partitionColumns.add(KafkaInternalFieldDescription.PARTITION_ID.getColumnHandle(connectorId, 0));
        partitionColumns.add(KafkaInternalFieldDescription.OFFSET_START.getColumnHandle(connectorId, 0));
        partitionColumns.add(KafkaInternalFieldDescription.OFFSET_END.getColumnHandle(connectorId, 0));
        partitionColumns.add(KafkaInternalFieldDescription.TIMESTAMP.getColumnHandle(connectorId, 0));
        return partitionColumns.build();
    }
}
