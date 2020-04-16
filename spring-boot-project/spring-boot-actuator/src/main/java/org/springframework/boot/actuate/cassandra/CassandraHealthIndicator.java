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

import com.datastax.oss.driver.api.core.CqlSession;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.util.Assert;

/**
 * Simple implementation of a {@link HealthIndicator} returning status information for
 * Cassandra data stores.
 *
 * <p>
 * The health report is comprised of two main sections:
 *
 * <ol>
 * <li>Cluster topology report: checks the nodes in the cluster, and reports their state
 * (up or down);</li>
 * <li>Token ring availability report: checks the token ranges in the ring, and reports
 * their state (available or unavailable, according to the configured keyspace and
 * consistency level).</li>
 * </ol>
 *
 * <p>
 * The health status will be switched to {@link Status#DOWN} if:
 *
 * <ol>
 * <li>The entire cluster is down; or</li>
 * <li>There is at least one unavailable token range.</li>
 * </ol>
 *
 * Note: cluster topology and ring availability reports are based solely on Gossip events
 * received by the driver. But Gossip events are not 100% reliable, and therefore, the
 * accuracy of reported health statuses should be considered best-effort only, and are not
 * meant to replace a proper operational surveillance tool.
 *
 * @author Julien Dubois
 * @author Alexandre Dutra
 * @since 2.0.0
 */
public class CassandraHealthIndicator extends AbstractHealthIndicator {

	private final CassandraHealthChecker cassandraHealthChecker;

	/**
	 * Create a new {@link CassandraHealthIndicator} instance.
	 * @param session the {@link CqlSession}.
	 */
	public CassandraHealthIndicator(CqlSession session) {
		super("Cassandra health check failed");
		Assert.notNull(session, "session must not be null");
		this.cassandraHealthChecker = new CassandraHealthChecker(session);
	}

	@Override
	protected void doHealthCheck(Health.Builder builder) throws Exception {
		this.cassandraHealthChecker.doHealthCheck(builder);
	}

}
