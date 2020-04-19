/*
 * Copyright 2012-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.boot.actuate.cassandra;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TopologyDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;

import org.springframework.boot.actuate.health.Health;
import org.springframework.util.Assert;

public class CassandraHealthChecker {

	private final CqlSession session;

	public CassandraHealthChecker(CqlSession session) {
		Assert.notNull(session, "session must not be null");
		this.session = session;
	}

	public void doHealthCheck(Health.Builder builder) {
		TopologyDiagnostic topologyDiagnostic = createTopologyDiagnostic();
		com.datastax.oss.driver.api.core.metadata.diagnostic.Status status = topologyDiagnostic.getStatus();
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("topology", topologyDiagnostic.getDetails());
		Optional<TokenRingDiagnostic> tokenRingDiagnostic = createTokenRingDiagnostic();
		if (tokenRingDiagnostic.isPresent()) {
			status = status.mergeWith(tokenRingDiagnostic.get().getStatus());
			details.put("ring", tokenRingDiagnostic.get().getDetails());
		}
		switch (status) {
		case AVAILABLE:
		case PARTIALLY_AVAILABLE:
			builder.up();
			break;
		default:
			builder.down();
			break;
		}
		builder.withDetails(details);
	}

	protected TopologyDiagnostic createTopologyDiagnostic() {
		return this.session.getMetadata().generateTopologyDiagnostic();
	}

	protected Optional<TokenRingDiagnostic> createTokenRingDiagnostic() {
		return getKeyspace().flatMap((keyspace) -> this.session.getMetadata().generateTokenRingDiagnostic(keyspace,
				getConsistencyLevel(), getDatacenter().orElse(null)));
	}

	protected Optional<KeyspaceMetadata> getKeyspace() {
		return this.session.getKeyspace()
				.flatMap((keyspaceName) -> this.session.getMetadata().getKeyspace(keyspaceName));
	}

	protected ConsistencyLevel getConsistencyLevel() {
		DriverExecutionProfile defaultProfile = this.session.getContext().getConfig().getDefaultProfile();
		return DefaultConsistencyLevel.valueOf(defaultProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY));
	}

	protected Optional<String> getDatacenter() {
		// local DC was set programmatically
		InternalDriverContext context = (InternalDriverContext) this.session.getContext();
		DriverExecutionProfile defaultProfile = context.getConfig().getDefaultProfile();
		String localDatacenter = context.getLocalDatacenter(defaultProfile.getName());
		if (localDatacenter == null) {
			// local DC was specified in the config
			localDatacenter = defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null);
		}
		return Optional.ofNullable(localDatacenter);
	}

}
