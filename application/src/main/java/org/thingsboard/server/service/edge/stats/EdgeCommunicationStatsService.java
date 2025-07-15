/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
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
package org.thingsboard.server.service.edge.stats;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.LongDataEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.dao.timeseries.TimeseriesService;
import org.thingsboard.server.queue.discovery.TopicService;
import org.thingsboard.server.queue.kafka.TbKafkaAdmin;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class EdgeCommunicationStatsService {
    private static final long IGNORE_SELF_STATS_DELTA = -1;

//    private static final String CLOUD_EVENT_CONSUMER = "-cloud-event-consumer";
//    private static final String CLOUD_EVENT_TS_CONSUMER = "-cloud-event-ts-consumer";

    private static final String DOWNLINK_MSGS_ADDED = "downlinkMsgsAdded";
    private static final String DOWNLINK_MSGS_PUSHED = "downlinkMsgsPushed";
    private static final String DOWNLINK_MSGS_PERMANENTLY_FAILED = "downlinkMsgsPermanentlyFailed";
    private static final String DOWNLINK_MSGS_TMP_FAILED = "downlinkMsgsTmpFailed";
    private static final String DOWNLINK_MSGS_LAG = "downlinkMsgsLag";

    @Autowired
    private TimeseriesService tsService;
    @Autowired(required = false)
    private TbKafkaAdmin tbKafkaAdmin;
    @Autowired(required = false)
    private TopicService topicService;

    @Value("${edge.stats.enabled:true}")
    private boolean edgeStatsEnabled;
    @Value("${edge.stats.ttl-days:7}")
    private int edgeStatsTtlDays;
    @Value("${edge.stats.report-interval-millis:20000}")
    private long reportIntervalMillis;
    @Value("${service.type:monolith}")
    private String serviceType;

    private final ConcurrentMap<EdgeId, EdgeMsgCounters> countersByEdge = new ConcurrentHashMap<>();
    private final ConcurrentMap<EdgeId, TenantId> tenantIdByEdge = new ConcurrentHashMap<>();

    @Scheduled(fixedDelayString = "${edge.stats.report-interval-millis:20000}")
    public void reportStats() {
        log.debug("Reporting Edge communication stats...");
        if (!edgeStatsEnabled) {
            log.debug("Edge stats reporting is disabled by configuration.");
            return;
        }

        long ts = (System.currentTimeMillis() / reportIntervalMillis) * reportIntervalMillis;

        for (Map.Entry<EdgeId, EdgeMsgCounters> counterByEdge : countersByEdge.entrySet()) {
            EdgeId edgeId = counterByEdge.getKey();
            EdgeMsgCounters counters = counterByEdge.getValue();
            TenantId tenantId = tenantIdByEdge.get(edgeId);
            // Exclude self-generated stats from downlink stats
            counters.getMsgsAdded().addAndGet(IGNORE_SELF_STATS_DELTA);
            counters.getMsgsPushed().addAndGet(IGNORE_SELF_STATS_DELTA);

            List<TsKvEntry> statsEntries = List.of(
                    entry(ts, DOWNLINK_MSGS_ADDED, counters.getMsgsAdded().get()),
                    entry(ts, DOWNLINK_MSGS_PUSHED, counters.getMsgsPushed().get()),
                    entry(ts, DOWNLINK_MSGS_PERMANENTLY_FAILED, counters.getMsgsPermanentlyFailed().get()),
                    entry(ts, DOWNLINK_MSGS_TMP_FAILED, counters.getMsgsTmpFailed().get()),
                    entry(ts, DOWNLINK_MSGS_LAG, counters.getMsgsLag().get())
            );

            ObjectNode statsJson = JacksonUtil.newObjectNode();
            statsEntries.forEach(entry -> statsJson.put(entry.getKey(), entry.getValueAsString()));

            log.trace("Reported Edge communication stats: {}", statsJson);

            tsService.save(tenantId, edgeId, statsEntries, TimeUnit.DAYS.toSeconds(edgeStatsTtlDays));

//                // Зберегти CloudEvent, можна адаптувати під edgeId
//                cloudEventService.saveCloudEvent(
//                        tenantId,
//                        CloudEventType.EDGE,
//                        EdgeEventActionType.TIMESERIES_UPDATED,
//                        edgeId,
//                        statsJson
//                );

            log.info("Successfully saved edge event with stats: {} for edge: {}", statsJson, edgeId);
            counters.clear();
        }
    }

    public void registerEdge(EdgeId edgeId, TenantId tenantId) {
        tenantIdByEdge.putIfAbsent(edgeId, tenantId);
        countersByEdge.putIfAbsent(edgeId, new EdgeMsgCounters());
    }

    public void unregisterEdge(EdgeId edgeId) {
        countersByEdge.remove(edgeId);
        tenantIdByEdge.remove(edgeId);
    }

//    private void updateLagIfKafkaEnabled() {
//        if (tbKafkaAdmin != null) {
//            String cloudEventConsumerGroupId = topicService.buildTopicName(serviceType + CLOUD_EVENT_CONSUMER);
//            String cloudEventTsConsumerGroupId = topicService.buildTopicName(serviceType + CLOUD_EVENT_TS_CONSUMER);
//            setUplinkMsgsLag(tbKafkaAdmin.getTotalLagForGroups(cloudEventConsumerGroupId, cloudEventTsConsumerGroupId));
//        }
//    }

    private BasicTsKvEntry entry(long ts, String key, long value) {
        return new BasicTsKvEntry(ts, new LongDataEntry(key, value));
    }

    public void addDownlinkMsgsAdded(EdgeId edgeId, long value) {
        countersByEdge
                .computeIfAbsent(edgeId, id -> new EdgeMsgCounters()).getMsgsAdded()
                .addAndGet(value);
    }

    public void addDownlinkMsgsPushed(EdgeId edgeId, long value) {
        countersByEdge
                .computeIfAbsent(edgeId, id -> new EdgeMsgCounters())
                .getMsgsPushed().addAndGet(value);
    }

    public void addDownlinkMsgsPermanentlyFailed(EdgeId edgeId, long value) {
        countersByEdge
                .computeIfAbsent(edgeId, id -> new EdgeMsgCounters())
                .getMsgsPermanentlyFailed().addAndGet(value);
    }

    public void addDownlinkMsgsTmpFailed(EdgeId edgeId, long value) {
        countersByEdge
                .computeIfAbsent(edgeId, id -> new EdgeMsgCounters())
                .getMsgsTmpFailed().addAndGet(value);
    }

    public void setDownlinkMsgsLag(EdgeId edgeId, long value) {
        countersByEdge
                .computeIfAbsent(edgeId, id -> new EdgeMsgCounters())
                .getMsgsLag().set(value);
    }

}
