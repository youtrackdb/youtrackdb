package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class OSharedContextEmbeddedTests {

  @Test
  public void testSimpleConfigStore() {
    try (OxygenDB oxygenDb = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig())) {
      oxygenDb.execute("create database test memory users(admin identified by 'admin' role admin)");
      try (var session = (ODatabaseSessionInternal) oxygenDb.open("test", "admin", "admin")) {
        OSharedContextEmbedded shared = (OSharedContextEmbedded) session.getSharedContext();
        Map<String, Object> config = new HashMap<>();
        config.put("one", "two");
        shared.saveConfig(session, "simple", config);
        var loadConfig = shared.loadConfig(session, "simple");
        assertEquals(config.get("one"), loadConfig.get("one"));
      }
    }
  }

  @Test
  public void testConfigStoreDouble() {
    try (OxygenDB oxygenDb = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig())) {
      oxygenDb.execute("create database test memory users(admin identified by 'admin' role admin)");
      try (var session = (ODatabaseSessionInternal) oxygenDb.open("test", "admin", "admin")) {
        OSharedContextEmbedded shared = (OSharedContextEmbedded) session.getSharedContext();
        Map<String, Object> config = new HashMap<>();
        config.put("one", "two");
        shared.saveConfig(session, "simple", config);
        var loadConfig = shared.loadConfig(session, "simple");
        assertEquals(config.get("one"), loadConfig.get("one"));

        Map<String, Object> other = new HashMap<>();
        other.put("one", "three");
        shared.saveConfig(session, "simple", other);
        var reLoadConfig = shared.loadConfig(session, "simple");
        assertEquals(other.get("one"), reLoadConfig.get("one"));
      }
    }
  }
}
