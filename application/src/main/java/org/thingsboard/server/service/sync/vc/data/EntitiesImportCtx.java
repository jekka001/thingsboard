/**
 * Copyright © 2016-2022 The Thingsboard Authors
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
package org.thingsboard.server.service.sync.vc.data;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.relation.EntityRelation;
import org.thingsboard.server.common.data.sync.ThrowingRunnable;
import org.thingsboard.server.common.data.sync.ie.EntityImportSettings;
import org.thingsboard.server.common.data.sync.vc.EntityTypeLoadResult;
import org.thingsboard.server.service.security.model.SecurityUser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Data
public class EntitiesImportCtx {

    private final SecurityUser user;
    private final String versionId;

    private final Map<EntityType, EntityTypeLoadResult> results = new HashMap<>();
    private final Map<EntityType, Set<EntityId>> importedEntities = new HashMap<>();
    private final Map<EntityId, EntityImportSettings> toReimport = new HashMap<>();
    private final List<ThrowingRunnable> referenceCallbacks = new ArrayList<>();
    private final List<ThrowingRunnable> eventCallbacks = new ArrayList<>();

    private final Map<EntityId, EntityId> externalToInternalIdMap = new HashMap<>();

    private final Set<EntityRelation> relations = new LinkedHashSet<>();

    private EntityImportSettings settings;

    public EntitiesImportCtx(SecurityUser user, String versionId) {
        this(user, versionId, null);
    }

    public EntitiesImportCtx(SecurityUser user, String versionId, EntityImportSettings settings) {
        this.user = user;
        this.versionId = versionId;
        this.settings = settings;
    }

    public TenantId getTenantId() {
        return user.getTenantId();
    }

    public boolean isFindExistingByName() {
        return getSettings().isFindExistingByName();
    }

    public boolean isUpdateRelations() {
        return getSettings().isUpdateRelations();
    }

    public boolean isSaveAttributes() {
        return getSettings().isSaveAttributes();
    }

    public boolean isSaveCredentials() {
        return getSettings().isSaveCredentials();
    }

    public boolean isResetExternalIdsOfAnotherTenant() {
        return getSettings().isResetExternalIdsOfAnotherTenant();
    }

    public EntityId getInternalId(EntityId externalId) {
        var result = externalToInternalIdMap.get(externalId);
        log.debug("[{}][{}] Local cache {} for id", externalId.getEntityType(), externalId.getId(), result != null ? "hit" : "miss");
        return result;
    }

    public void putInternalId(EntityId externalId, EntityId internalId) {
        log.debug("[{}][{}] Local cache put: {}", externalId.getEntityType(), externalId.getId(), internalId);
        externalToInternalIdMap.put(externalId, internalId);
    }

    public void registerResult(EntityType entityType, boolean created) {
        EntityTypeLoadResult result = results.computeIfAbsent(entityType, EntityTypeLoadResult::new);
        if (created) {
            result.setCreated(result.getCreated() + 1);
        } else {
            result.setUpdated(result.getUpdated() + 1);
        }
    }

    public void registerDeleted(EntityType entityType) {
        EntityTypeLoadResult result = results.computeIfAbsent(entityType, EntityTypeLoadResult::new);
        result.setDeleted(result.getDeleted() + 1);
    }

    public void addRelations(Collection<EntityRelation> values) {
        relations.addAll(values);
    }

    public void addReferenceCallback(ThrowingRunnable tr) {
        if (tr != null) {
            referenceCallbacks.add(tr);
        }
    }

    public void addEventCallback(ThrowingRunnable tr) {
        if (tr != null) {
            eventCallbacks.add(tr);
        }
    }


}
