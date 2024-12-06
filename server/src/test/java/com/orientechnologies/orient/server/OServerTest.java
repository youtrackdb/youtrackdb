package com.orientechnologies.orient.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import java.io.File;
import java.util.ArrayList;
import java.util.logging.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OServerTest {

  private String prevPassword;
  private String prevOrientHome;
  private boolean allowJvmShutdownPrev;
  private OServer server;
  private OServerConfiguration conf;

  @Before
  public void setUp() throws Exception {
    LogManager.instance().setConsoleLevel(Level.OFF.getName());
    prevPassword = System.setProperty("YOU_TRACK_DB_ROOT_PASSWORD", "rootPassword");
    prevOrientHome = System.setProperty("YOU_TRACK_DB_HOME", "./target/testhome");

    conf = new OServerConfiguration();

    conf.handlers = new ArrayList<OServerHandlerConfiguration>();
    OServerHandlerConfiguration handlerConfiguration = new OServerHandlerConfiguration();
    handlerConfiguration.clazz = ServerFailingOnStarupPluginStub.class.getName();
    handlerConfiguration.parameters = new OServerParameterConfiguration[0];

    conf.handlers.add(0, handlerConfiguration);
  }

  @After
  public void tearDown() throws Exception {
    if (server.isActive()) {
      server.shutdown();
    }

    YouTrackDBManager.instance().shutdown();
    FileUtils.deleteRecursively(new File("./target/testhome"));

    if (prevOrientHome != null) {
      System.setProperty("YOU_TRACK_DB_HOME", prevOrientHome);
    }
    if (prevPassword != null) {
      System.setProperty("YOU_TRACK_DB_ROOT_PASSWORD", prevPassword);
    }

    YouTrackDBManager.instance().startup();
  }

  @Test
  public void shouldShutdownOnPluginStartupException() {

    try {
      server = new OServer(false);
      server.startup(conf);
      server.activate();
    } catch (Exception e) {
      assertThat(e).isInstanceOf(BaseException.class);
    }

    assertThat(server.isActive()).isFalse();
    server.shutdown();
  }
}
