package com.orientechnologies.orient.server;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.hook.YTDocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerHookConfiguration;
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

  public static class MyHook extends YTDocumentHookAbstract {

    public MyHook() {
    }

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
    }

    @Override
    public void onRecordAfterCreate(YTDocument iDocument) {
      count++;
    }
  }

  private static int count = 0;
  private OServer server;

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
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);

    OServerConfigurationManager ret =
        new OServerConfigurationManager(
            this.getClass()
                .getClassLoader()
                .getResourceAsStream(
                    "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    OServerHookConfiguration hc = new OServerHookConfiguration();
    hc.clazz = MyHook.class.getName();
    ret.getConfiguration().hooks = new ArrayList<>();
    ret.getConfiguration().hooks.add(hc);
    server.startup(ret.getConfiguration());
    server.activate();

    OServerAdmin admin = new OServerAdmin("remote:localhost");
    admin.connect("root", "root");
    admin.createDatabase("test", "nothign", "memory");
    admin.close();
  }

  @After
  public void after() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost");
    admin.connect("root", "root");
    admin.dropDatabase("test", "memory");
    admin.close();
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }

  @Test
  public void test() {
    final int initValue = count;

    try (var pool =
        YouTrackDB.remote("remote:localhost", "root", "root")) {
      for (int i = 0; i < 10; i++) {
        var poolInstance = pool.cachedPool("test", "admin", "admin");
        var id = i;
        try (var some = poolInstance.acquire()) {
          some.createClassIfNotExist("Test");

          some.executeInTx(() -> {
            some.save(new YTDocument("Test").field("entry", id));
            some.commit();
          });
        }
      }
    }

    Assert.assertEquals(initValue + 10, count);
  }
}
