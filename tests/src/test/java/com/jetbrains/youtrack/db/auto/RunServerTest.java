package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test
public class RunServerTest {

  private YouTrackDBServer server;

  @BeforeSuite
  public void before() throws Exception {
    server = new YouTrackDBServer(false);
    server.startup(
        RunServerTest.class.getClassLoader().getResourceAsStream("youtrackdb-server-config.xml"));
    server.activate();
  }

  @Test
  public void test() {
  }

  @AfterSuite
  public void after() {
    server.shutdown();
  }
}
