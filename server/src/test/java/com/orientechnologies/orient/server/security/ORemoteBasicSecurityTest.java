package com.orientechnologies.orient.server.security;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ORemoteBasicSecurityTest {

  private OServer server;

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
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = OServer.startFromClasspathConfig("abstract-orientdb-server-config.xml");

    YouTrackDB youTrackDB =
        new YouTrackDB("remote:localhost", "root", "root", YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database test memory users (admin identified by 'admin' role admin, reader"
            + " identified by 'reader' role reader, writer identified by 'writer' role writer)");
    try (ODatabaseSession session = youTrackDB.open("test", "admin", "admin")) {
      session.createClass("one");
      session.begin();
      session.save(new ODocument("one"));
      session.commit();
    }
    youTrackDB.close();
  }

  @Test
  public void testCreateAndConnectWriter() {
    // CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (YouTrackDB writerOrient = new YouTrackDB("remote:localhost",
        YouTrackDBConfig.defaultConfig())) {
      try (ODatabaseSession writer = writerOrient.open("test", "writer", "writer")) {
        writer.begin();
        writer.save(new ODocument("one"));
        writer.commit();
        try (OResultSet rs = writer.query("select from one")) {
          assertEquals(rs.stream().count(), 2);
        }
      }
    }
  }

  @Test
  public void testCreateAndConnectReader() {
    // CREATE A SEPARATE CONTEXT TO MAKE SURE IT LOAD STAFF FROM SCRATCH
    try (YouTrackDB writerOrient = new YouTrackDB("remote:localhost",
        YouTrackDBConfig.defaultConfig())) {
      try (ODatabaseSession writer = writerOrient.open("test", "reader", "reader")) {
        try (OResultSet rs = writer.query("select from one")) {
          assertEquals(rs.stream().count(), 1);
        }
      }
    }
  }

  @After
  public void after() {
    server.shutdown();
  }
}
