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

import java.util.Objects;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import reactor.core.publisher.Mono;

import org.springframework.boot.actuate.health.AbstractReactiveHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.util.Assert;

/**
 * A {@link ReactiveHealthIndicator} for Cassandra.
 *
 * @author Artsiom Yudovin
 * @author Alexandre Dutra
 * @since 2.1.0
 */
public class CassandraReactiveHealthIndicator extends AbstractReactiveHealthIndicator {

	private static final SimpleStatement SELECT = SimpleStatement
			.newInstance("SELECT release_version FROM system.local").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);

	private final CqlSession session;

	/**
	 * Create a new {@link CassandraHealthIndicator} instance.
	 * @param session the {@link CqlSession}.
	 */
	public CassandraReactiveHealthIndicator(CqlSession session) {
		super("Cassandra health check failed");
		Assert.notNull(session, "session must not be null");
		this.session = session;
	}

	@Override
	protected Mono<Health> doHealthCheck(Health.Builder builder) {
		return Mono.from(this.session.executeReactive(SELECT))
				.map((row) -> Objects.requireNonNull(row.getString(0), "release_version should never be null"))
				.map((version) -> builder.up().withDetail("version", version).build());
	}

}
