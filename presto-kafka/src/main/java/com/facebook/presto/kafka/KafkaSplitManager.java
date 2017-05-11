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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import javax.inject.Inject;

import java.util.Set;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
import static com.facebook.presto.kafka.KafkaUtil.findAllOffsets;
import static com.facebook.presto.kafka.KafkaUtil.selectRandom;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaSimpleConsumerManager consumerManager;
    private final Set<HostAddress> nodes;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaSimpleConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.nodes = ImmutableSet.copyOf(kafkaConnectorConfig.getNodes());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
        KafkaTableLayoutHandle layoutHandle = (KafkaTableLayoutHandle) layout;

        SimpleConsumer simpleConsumer = consumerManager.getConsumer(selectRandom(nodes));

        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(ImmutableList.of(kafkaTableHandle.getTopicName()));
        TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

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
                    KafkaSplit split = new KafkaSplit(
                            connectorId,
                            metadata.topic(),
                            kafkaTableHandle.getKeyDataFormat(),
                            kafkaTableHandle.getMessageDataFormat(),
                            part.partitionId(),
                            offsets[i],
                            offsets[i - 1],
                            partitionLeader);
                    splits.add(split);
                }
            }
        }

        return new FixedSplitSource(splits.build());
    }
}
