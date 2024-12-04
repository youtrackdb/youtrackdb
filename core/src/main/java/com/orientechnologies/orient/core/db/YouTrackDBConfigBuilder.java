/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.core.db.config.ONodeConfigurationBuilder;
import com.orientechnologies.orient.core.security.OGlobalUser;
import com.orientechnologies.orient.core.security.OGlobalUserImpl;
import com.orientechnologies.orient.core.security.OSecurityConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class YouTrackDBConfigBuilder {

  private OContextConfiguration configurations = new OContextConfiguration();
  private final Map<ATTRIBUTES, Object> attributes = new HashMap<>();
  private final Set<ODatabaseListener> listeners = new HashSet<>();
  private ClassLoader classLoader;
  private final ONodeConfigurationBuilder nodeConfigurationBuilder = ONodeConfiguration.builder();
  private OSecurityConfig securityConfig;
  private final List<OGlobalUser> users = new ArrayList<OGlobalUser>();

  public YouTrackDBConfigBuilder fromGlobalMap(Map<YTGlobalConfiguration, Object> values) {
    for (Map.Entry<YTGlobalConfiguration, Object> entry : values.entrySet()) {
      addConfig(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public YouTrackDBConfigBuilder fromMap(Map<String, Object> values) {
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      configurations.setValue(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public YouTrackDBConfigBuilder addListener(ODatabaseListener listener) {
    listeners.add(listener);
    return this;
  }

  public YouTrackDBConfigBuilder addConfig(
      final YTGlobalConfiguration configuration, final Object value) {
    configurations.setValue(configuration, value);
    return this;
  }

  public YouTrackDBConfigBuilder addAttribute(final ATTRIBUTES attribute, final Object value) {
    attributes.put(attribute, value);
    return this;
  }

  public YouTrackDBConfigBuilder setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
    return this;
  }

  public ONodeConfigurationBuilder getNodeConfigurationBuilder() {
    return nodeConfigurationBuilder;
  }

  public YouTrackDBConfigBuilder setSecurityConfig(OSecurityConfig securityConfig) {
    this.securityConfig = securityConfig;
    return this;
  }

  public YouTrackDBConfig build() {
    return new YouTrackDBConfig(
        configurations,
        attributes,
        listeners,
        classLoader,
        nodeConfigurationBuilder.build(),
        securityConfig,
        users);
  }

  public YouTrackDBConfigBuilder fromContext(final OContextConfiguration contextConfiguration) {
    configurations = contextConfiguration;
    return this;
  }

  public YouTrackDBConfigBuilder addGlobalUser(
      final String user, final String password, final String resource) {
    users.add(new OGlobalUserImpl(user, password, resource));
    return this;
  }
}
