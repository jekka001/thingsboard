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
package org.thingsboard.server.service.edge;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.rule.engine.action.TbSaveToCustomCassandraTableNode;
import org.thingsboard.rule.engine.action.TbSaveToCustomCassandraTableNodeConfiguration;
import org.thingsboard.rule.engine.api.NodeConfiguration;
import org.thingsboard.rule.engine.telemetry.TbMsgAttributesNode;
import org.thingsboard.rule.engine.telemetry.TbMsgAttributesNodeConfiguration;
import org.thingsboard.rule.engine.telemetry.TbMsgTimeseriesNode;
import org.thingsboard.rule.engine.telemetry.TbMsgTimeseriesNodeConfiguration;
import org.thingsboard.server.common.data.rule.RuleChainMetaData;
import org.thingsboard.server.common.data.rule.RuleNode;
import org.thingsboard.server.gen.edge.v1.EdgeVersion;
import org.thingsboard.server.gen.edge.v1.RuleChainMetadataUpdateMsg;
import org.thingsboard.server.gen.edge.v1.UpdateMsgType;
import org.thingsboard.server.service.edge.rpc.utils.EdgeVersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class EdgeMsgConstructorUtilsTest {
    private static final int CONFIGURATION_VERSION = 5;

    private static final List<EdgeVersion> EDGE_VERSIONS = Arrays.asList(
            EdgeVersion.V_4_0_0, EdgeVersion.V_3_9_0, EdgeVersion.V_3_8_0, EdgeVersion.V_3_7_0
    );

    private static final Map<NodeConfiguration, String> CONFIG_TO_NODE = Map.of(
            new TbMsgTimeseriesNodeConfiguration(), TbMsgTimeseriesNode.class.getName(),
            new TbMsgAttributesNodeConfiguration(), TbMsgAttributesNode.class.getName(),
            new TbSaveToCustomCassandraTableNodeConfiguration(), TbSaveToCustomCassandraTableNode.class.getName()
    );

    private static final Map<String, Integer> NODE_TO_AMOUNT_CONFIG_PARAM = Map.of(
            TbMsgTimeseriesNode.class.getName(), 3,
            TbMsgAttributesNode.class.getName(), 5,
            TbSaveToCustomCassandraTableNode.class.getName(), 3
    );

    @Test
    public void testRuleChainMetadataUpdateMsgForAllEdgeVersions() {
        // GIVEN
        RuleChainMetaData metaData = createIncompatibleRuleNodesForOldEdge();

        for (EdgeVersion edgeVersion : EDGE_VERSIONS) {
            // WHEN
            List<RuleNode> ruleNode = getRuleNodeFromMetadataUpdateMessage(metaData, edgeVersion);

            // THEN
            assertRuleNodeConfiguration(ruleNode, edgeVersion);
        }
    }

    private RuleChainMetaData createIncompatibleRuleNodesForOldEdge() {
        RuleChainMetaData ruleChainMetaData = new RuleChainMetaData();
        List<RuleNode> ruleNodes = new ArrayList<>();

        for (Map.Entry<NodeConfiguration, String> configToNode : CONFIG_TO_NODE.entrySet()) {
            RuleNode ruleNode = new RuleNode();

            ruleNode.setName(configToNode.getValue());
            ruleNode.setType(configToNode.getValue());
            ruleNode.setConfigurationVersion(CONFIGURATION_VERSION);
            ruleNode.setConfiguration(JacksonUtil.valueToTree(configToNode.getKey().defaultConfiguration()));

            ruleNodes.add(ruleNode);
        }

        ruleChainMetaData.setFirstNodeIndex(0);
        ruleChainMetaData.setNodes(ruleNodes);

        return ruleChainMetaData;
    }

    private List<RuleNode> getRuleNodeFromMetadataUpdateMessage(RuleChainMetaData metaData, EdgeVersion edgeVersion) {
        RuleChainMetadataUpdateMsg ruleChainMetadataUpdateMsg =
                EdgeMsgConstructorUtils.constructRuleChainMetadataUpdatedMsg(UpdateMsgType.ENTITY_CREATED_RPC_MESSAGE, metaData, edgeVersion);

        RuleChainMetaData ruleChainMetaData = JacksonUtil.fromString(ruleChainMetadataUpdateMsg.getEntity(), RuleChainMetaData.class, true);
        Assert.assertNotNull("RuleChainMetaData is null", ruleChainMetaData);

        List<RuleNode> ruleNodes = ruleChainMetaData.getNodes();
        Assert.assertNotNull("RuleNode is null for Edge version " + edgeVersion, ruleNodes);

        return ruleNodes;
    }

    private void assertRuleNodeConfiguration(List<RuleNode> ruleNodes, EdgeVersion edgeVersion) {
        for (Map.Entry<NodeConfiguration, String> configToNode : CONFIG_TO_NODE.entrySet()) {
            RuleNode ruleNode = ruleNodes.stream().filter(node -> node.getName().equals(configToNode.getValue())).findFirst().get();
            int ruleNodeConfigAmount = NODE_TO_AMOUNT_CONFIG_PARAM.get(configToNode.getValue());

            boolean isOldEdge = EdgeVersionUtils.isEdgeVersionOlderThan(edgeVersion, EdgeVersion.V_3_9_0);
            int expectedConfigAmount = isOldEdge ? ruleNodeConfigAmount - 1 : ruleNodeConfigAmount;
            boolean includeConfigParam = !isOldEdge;

            checkConfigParams(ruleNode, expectedConfigAmount, includeConfigParam);
        }
    }

    private void checkConfigParams(RuleNode ruleNode, int expectedConfigAmount, boolean includeConfigParam) {
        String ignoreConfigParam = NODE_TO_IGNORE_PARAM.get(ruleNode.getName());

        Assert.assertEquals(
                String.format("Expected %d config params for ruleNode '%s', but found %d", expectedConfigAmount, ruleNode.getName(), ruleNode.getConfiguration().size()),
                expectedConfigAmount, ruleNode.getConfiguration().size()
        );

        boolean hasIgnoredField = ruleNode.getConfiguration().has(ignoreConfigParam);
        Assert.assertEquals(
                String.format("Field '%s' for ruleNode '%s' should %s be present", ignoreConfigParam, ruleNode.getName(), includeConfigParam ? "not" : ""),
                includeConfigParam, hasIgnoredField
        );
    }

}
