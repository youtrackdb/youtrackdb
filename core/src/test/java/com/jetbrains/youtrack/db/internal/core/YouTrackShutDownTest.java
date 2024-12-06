package com.jetbrains.youtrack.db.internal.core;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.core.shutdown.ShutdownHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class YouTrackShutDownTest {

  private int test = 0;

  @Before
  public void before() {
    YouTrackDBManager.instance().startup();
  }

  @After
  public void after() {
    YouTrackDBManager.instance().startup();
  }

  @Test
  public void testShutdownHandler() {

    YouTrackDBManager.instance()
        .addShutdownHandler(
            new ShutdownHandler() {
              @Override
              public int getPriority() {
                return 0;
              }

              @Override
              public void shutdown() throws Exception {
                test += 1;
              }
            });

    YouTrackDBManager.instance().shutdown();
    assertEquals(1, test);
    YouTrackDBManager.instance().startup();
    YouTrackDBManager.instance().shutdown();
    assertEquals(1, test);
  }
}
