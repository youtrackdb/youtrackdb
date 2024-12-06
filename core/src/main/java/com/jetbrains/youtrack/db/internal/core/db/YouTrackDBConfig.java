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
package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal.ATTRIBUTES;
import com.jetbrains.youtrack.db.internal.core.db.config.NodeConfiguration;
import com.jetbrains.youtrack.db.internal.core.security.GlobalUser;
import com.jetbrains.youtrack.db.internal.core.security.DefaultSecurityConfig;
import com.jetbrains.youtrack.db.internal.core.security.SecurityConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class YouTrackDBConfig {

  public static final String LOCK_TYPE_MODIFICATION = "modification";
  public static final String LOCK_TYPE_READWRITE = "readwrite";

  private YouTrackDBConfig parent;
  private ContextConfiguration configurations;
  private Map<ATTRIBUTES, Object> attributes;
  private Set<DatabaseListener> listeners;
  private ClassLoader classLoader;
  private NodeConfiguration nodeConfiguration;
  private final SecurityConfig securityConfig;
  private final List<GlobalUser> users;

  protected YouTrackDBConfig() {
    configurations = new ContextConfiguration();
    attributes = new HashMap<>();
    parent = null;
    listeners = new HashSet<>();
    classLoader = this.getClass().getClassLoader();
    this.securityConfig = new DefaultSecurityConfig();
    this.users = new ArrayList<GlobalUser>();
  }

  protected YouTrackDBConfig(
      ContextConfiguration configurations,
      Map<ATTRIBUTES, Object> attributes,
      Set<DatabaseListener> listeners,
      ClassLoader classLoader,
      NodeConfiguration nodeConfiguration,
      SecurityConfig securityConfig,
      List<GlobalUser> users) {
    this.configurations = configurations;
    this.attributes = attributes;
    parent = null;
    if (listeners != null) {
      this.listeners = listeners;
    } else {
      this.listeners = Collections.emptySet();
    }
    if (classLoader != null) {
      this.classLoader = classLoader;
    } else {
      this.classLoader = this.getClass().getClassLoader();
    }
    this.nodeConfiguration = nodeConfiguration;
    this.securityConfig = securityConfig;
    this.users = users;
  }

  public static YouTrackDBConfig defaultConfig() {
    return new YouTrackDBConfig();
  }

  public static YouTrackDBConfigBuilder builder() {
    return new YouTrackDBConfigBuilder();
  }

  public Set<DatabaseListener> getListeners() {
    return listeners;
  }

  public ContextConfiguration getConfigurations() {
    return configurations;
  }

  public Map<ATTRIBUTES, Object> getAttributes() {
    return attributes;
  }

  public NodeConfiguration getNodeConfiguration() {
    return nodeConfiguration;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public SecurityConfig getSecurityConfig() {
    return securityConfig;
  }

  public List<GlobalUser> getUsers() {
    return users;
  }

  public void setParent(YouTrackDBConfig parent) {
    this.parent = parent;
    if (parent != null) {
      if (parent.attributes != null) {
        Map<ATTRIBUTES, Object> attrs = new HashMap<>();
        attrs.putAll(parent.attributes);
        if (attributes != null) {
          attrs.putAll(attributes);
        }
        this.attributes = attrs;
      }

      if (parent.configurations != null) {
        ContextConfiguration confis = new ContextConfiguration();
        confis.merge(parent.configurations);
        if (this.configurations != null) {
          confis.merge(this.configurations);
        }
        this.configurations = confis;
      }

      if (this.classLoader == null) {
        this.classLoader = parent.classLoader;
      }

      if (parent.listeners != null) {
        Set<DatabaseListener> lis = new HashSet<>();
        lis.addAll(parent.listeners);
        if (this.listeners != null) {
          lis.addAll(this.listeners);
        }
        this.listeners = lis;
      }
    }
  }
}
