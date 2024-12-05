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
package com.orientechnologies.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.core.db.YouTrackDBConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class YouTrackDBConfigTest {

  @Test
  public void testBuildSettings() {
    YouTrackDBConfig settings =
        YouTrackDBConfig.builder()
            .addConfig(YTGlobalConfiguration.DB_POOL_MAX, 20)
            .addAttribute(ATTRIBUTES.VALIDATION, true)
            .build();

    assertEquals(settings.getConfigurations().getValue(YTGlobalConfiguration.DB_POOL_MAX), 20);
    assertEquals(settings.getAttributes().get(ATTRIBUTES.VALIDATION), true);
  }

  @Test
  public void testBuildSettingsFromMap() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(YTGlobalConfiguration.DB_POOL_MAX.getKey(), 20);
    YouTrackDBConfig settings = YouTrackDBConfig.builder().fromMap(configs).build();
    assertEquals(settings.getConfigurations().getValue(YTGlobalConfiguration.DB_POOL_MAX), 20);
  }

  @Test
  public void testBuildSettingsFromGlobalMap() {
    Map<YTGlobalConfiguration, Object> configs = new HashMap<>();
    configs.put(YTGlobalConfiguration.DB_POOL_MAX, 20);
    YouTrackDBConfig settings = YouTrackDBConfig.builder().fromGlobalMap(configs).build();
    assertEquals(settings.getConfigurations().getValue(YTGlobalConfiguration.DB_POOL_MAX), 20);
  }

  @Test
  public void testParentConfig() {
    YouTrackDBConfig parent =
        YouTrackDBConfig.builder()
            .addConfig(YTGlobalConfiguration.DB_POOL_MAX, 20)
            .addAttribute(ATTRIBUTES.VALIDATION, true)
            .build();

    YouTrackDBConfig settings =
        YouTrackDBConfig.builder()
            .addConfig(YTGlobalConfiguration.CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
            .addAttribute(ATTRIBUTES.VALIDATION, false)
            .build();

    settings.setParent(parent);

    assertEquals(settings.getConfigurations().getValue(YTGlobalConfiguration.DB_POOL_MAX), 20);
    assertEquals(
        settings.getConfigurations().getValue(YTGlobalConfiguration.CLIENT_CONNECTION_STRATEGY),
        "ROUND_ROBIN_CONNECT");
    assertEquals(settings.getAttributes().get(ATTRIBUTES.VALIDATION), false);
  }
}
