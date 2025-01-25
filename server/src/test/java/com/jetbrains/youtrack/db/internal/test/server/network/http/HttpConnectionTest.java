package com.jetbrains.youtrack.db.internal.test.server.network.http;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests HTTP "connect" command.
 */
public class HttpConnectionTest extends BaseHttpDatabaseTest {

  @Test
  public void testConnect() throws Exception {
    Assert.assertEquals(get("connect/" + getDatabaseName()).getResponse().getCode(), 204);
  }

  public void testTooManyConnect() throws Exception {
    if (isInDevelopmentMode())
    // SKIP IT
    {
      return;
    }

    final int originalMax =
        GlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();
    try {

      int MAX = 10;
      int TOTAL = 30;

      int good = 0;
      int bad = 0;
      GlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.setValue(MAX);
      for (int i = 0; i < TOTAL; ++i) {
        try {
          final int response =
              get("connect/" + getDatabaseName()).setRetry(0).getResponse().getCode();
          Assert.assertEquals(response, 204);
          good++;
        } catch (IOException e) {
          bad++;
        }
      }

      System.out.printf("\nTOTAL %d - MAX %d - GOOD %d - BAD %d", TOTAL, MAX, good, bad);

      Assert.assertTrue(good >= MAX);
      Assert.assertEquals(bad + good, TOTAL);

    } finally {
      GlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.setValue(originalMax);
    }
  }

  public void testConnectAutoDisconnectKeepAlive() throws Exception {
    setKeepAlive(true);
    testConnectAutoDisconnect();
  }

  public void testConnectAutoDisconnectNoKeepAlive() throws Exception {
    setKeepAlive(false);
    testConnectAutoDisconnect();
  }

  protected void testConnectAutoDisconnect() throws Exception {
    if (isInDevelopmentMode())
    // SKIP IT
    {
      return;
    }

    final int max = GlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();

    int TOTAL = max * 3;

    for (int i = 0; i < TOTAL; ++i) {
      final int response = get("connect/" + getDatabaseName()).setRetry(0).getResponse().getCode();
      Assert.assertEquals(response, 204);

      if (i % 100 == 0) {
        System.out.printf("\nConnections " + i);
      }
    }

    System.out.print("\nTest completed");

    Collection<EntityImpl> conns = null;
    for (int i = 0; i < 20; ++i) {
      Assert.assertEquals(
          get("server")
              .setKeepAlive(false)
              .setUserName("root")
              .setUserPassword("root")
              .getResponse()
              .getCode(),
          200);

      final EntityImpl serverStatus =
          new EntityImpl(null).updateFromJSON(getResponse().getEntity().getContent());
      conns = serverStatus.field("connections");

      final int openConnections = conns.size();

      System.out.printf("\nConnections still open: " + openConnections);

      if (openConnections <= 1) {
        break;
      }

      Thread.sleep(1000);
    }

    System.out.printf("\nOK: connections: " + conns.size());

    Assert.assertTrue(conns.size() <= 1);
  }

  @Override
  public String getDatabaseName() {
    return "httpconnection";
  }
}
