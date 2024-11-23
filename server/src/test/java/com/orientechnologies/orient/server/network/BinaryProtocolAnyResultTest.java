package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class BinaryProtocolAnyResultTest {

  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @Test
  @Ignore
  public void scriptReturnValueTest() throws IOException {
    OxygenDB orient =
        new OxygenDB("remote:localhost", "root", "root", OxygenDBConfig.defaultConfig());

    if (orient.exists("test")) {
      orient.drop("test");
    }

    orient.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseSession db = orient.open("test", "admin", "admin");

    Object res = db.execute("SQL", " let $one = select from OUser limit 1; return [$one,1]");

    assertTrue(res instanceof List);
    assertTrue(((List) res).get(0) instanceof Collection);
    assertTrue(((List) res).get(1) instanceof Integer);
    db.close();

    orient.drop("test");
    orient.close();
  }

  @After
  public void after() {
    server.shutdown();

    Oxygen.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Oxygen.instance().startup();
  }
}
