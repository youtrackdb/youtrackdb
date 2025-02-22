package com.jetbrains.youtrack.db.internal.server;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YourTracks;
import com.jetbrains.youtrack.db.internal.client.remote.ServerAdmin;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.hook.DocumentHookAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.tools.config.ServerConfigurationManager;
import com.jetbrains.youtrack.db.internal.tools.config.ServerHookConfiguration;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HookInstallServerTest {
  private static final String SERVER_DIRECTORY = "./target/dbfactory";

  public static class MyHook extends DocumentHookAbstract {

    public MyHook(DatabaseSession session) {
      super(session);
    }

    @Override
    public void onRecordAfterCreate(EntityImpl entity) {
      count++;
    }
  }

  private static int count = 0;
  private YouTrackDBServer server;

  @Before
  public void before()
      throws MalformedObjectNameException,
      InstanceAlreadyExistsException,
      MBeanRegistrationException,
      NotCompliantMBeanException,
      ClassNotFoundException,
      NullPointerException,
      IOException,
      IllegalArgumentException,
      SecurityException,
      InvocationTargetException,
      NoSuchMethodException,
      InstantiationException,
      IllegalAccessException {
    server = new YouTrackDBServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);

    var ret =
        new ServerConfigurationManager(
            this.getClass()
                .getClassLoader()
                .getResourceAsStream(
                    "com/jetbrains/youtrack/db/internal/server/network/youtrackdb-server-config.xml"));
    var hc = new ServerHookConfiguration();
    hc.clazz = MyHook.class.getName();
    ret.getConfiguration().hooks = new ArrayList<>();
    ret.getConfiguration().hooks.add(hc);
    server.startup(ret.getConfiguration());
    server.activate();

    var admin = new ServerAdmin("remote:localhost");
    admin.connect("root", "root");
    admin.createDatabase("test", "nothign", "memory");
    admin.close();
  }

  @After
  public void after() throws IOException {
    var admin = new ServerAdmin("remote:localhost");
    admin.connect("root", "root");
    admin.dropDatabase("test", "memory");
    admin.close();
    server.shutdown();

    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBEnginesManager.instance().startup();
  }

  @Test
  public void test() {
    final var initValue = count;

    try (var pool =
        YourTracks.remote("remote:localhost", "root", "root")) {
      for (var i = 0; i < 10; i++) {
        var poolInstance = pool.cachedPool("test", "admin", "admin");
        var id = i;
        try (var db = poolInstance.acquire()) {
          db.createClassIfNotExist("Test");

          db.executeInTx(() -> {
            ((EntityImpl) db.newEntity("Test")).field("entry", id);
            db.commit();
          });
        }
      }
    }

    Assert.assertEquals(initValue + 10, count);
  }
}
