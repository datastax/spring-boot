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

package org.springframework.boot.actuate.autoconfigure.cassandra;

import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;

import org.springframework.boot.actuate.autoconfigure.health.CompositeHealthContributorConfiguration;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.cassandra.CassandraHealthIndicator;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for
 * {@link CassandraHealthIndicator}.
 *
 * @author Julien Dubois
 * @author Stephane Nicoll
 * @author Alexandre Dutra
 * @since 2.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(CqlSession.class)
@ConditionalOnBean(CqlSession.class)
@ConditionalOnEnabledHealthIndicator("cassandra")
@AutoConfigureAfter({ CassandraAutoConfiguration.class, CassandraReactiveHealthContributorAutoConfiguration.class })
public class CassandraHealthContributorAutoConfiguration
		extends CompositeHealthContributorConfiguration<CassandraHealthIndicator, CqlSession> {

	@Bean
	@ConditionalOnMissingBean(name = { "cassandraHealthIndicator", "cassandraHealthContributor" })
	public HealthContributor cassandraHealthContributor(Map<String, CqlSession> sessions) {
		return createContributor(sessions);
	}

}
