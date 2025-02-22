package com.jetbrains.youtrack.db.internal.server.security;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RemoteBasicSecurityTest {

  private YouTrackDBServer server;

  @Before
  public void before()
      throws IOException,
      InstantiationException,
      InvocationTargetException,
      NoSuchMethodException,
      MBeanRegistrationException,
      IllegalAccessException,
      InstanceAlreadyExistsException,
      NotCompliantMBeanException,
      ClassNotFoundException,
      MalformedObjectNameException {
    GlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = YouTrackDBServer.startFromClasspathConfig("abstract-youtrackdb-server-config.xml");

    YouTrackDB youTrackDB =
        new YouTrackDBImpl("remote:localhost", "root", "root", YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database test memory users (admin identified by 'admin' role admin, reader"
            + " identified by 'reader' role reader, writer identified by 'writer' role writer)");
    try (var db = youTrackDB.open("test", "admin", "admin")) {
      db.createClass("one");
      db.begin();
      db.newEntity("one");
      db.commit();
    }
    youTrackDB.close();
  }

  @Test
  public void testCreateAndConnectWriter() {
    // CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (YouTrackDB writerOrient = new YouTrackDBImpl("remote:localhost",
        YouTrackDBConfig.defaultConfig())) {
      try (var db = writerOrient.open("test", "writer", "writer")) {
        db.begin();
        db.newEntity("one");
        db.commit();
        try (var rs = db.query("select from one")) {
          assertEquals(2, rs.stream().count());
        }
      }
    }
  }

  @Test
  public void testCreateAndConnectReader() {
    // CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (YouTrackDB writerOrient = new YouTrackDBImpl("remote:localhost",
        YouTrackDBConfig.defaultConfig())) {
      try (var writer = writerOrient.open("test", "reader", "reader")) {
        try (var rs = writer.query("select from one")) {
          assertEquals(1, rs.stream().count());
        }
      }
    }
  }

  @After
  public void after() {
    server.shutdown();
  }
}
