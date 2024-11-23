package com.orientechnologies.orient.core;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.shutdown.OShutdownHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OxygenShutDownTest {

  private int test = 0;

  @Before
  public void before() {
    Oxygen.instance().startup();
  }

  @After
  public void after() {
    Oxygen.instance().startup();
  }

  @Test
  public void testShutdownHandler() {

    Oxygen.instance()
        .addShutdownHandler(
            new OShutdownHandler() {
              @Override
              public int getPriority() {
                return 0;
              }

              @Override
              public void shutdown() throws Exception {
                test += 1;
              }
            });

    Oxygen.instance().shutdown();
    assertEquals(1, test);
    Oxygen.instance().startup();
    Oxygen.instance().shutdown();
    assertEquals(1, test);
  }
}
