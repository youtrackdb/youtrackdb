package com.orientechnologies.lucene.integration;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LuceneCreateIndexIntegrationTest {

  private OServer server0;
  private YouTrackDB remote;

  @Before
  public void before() throws Exception {
    server0 =
        OServer.startFromClasspathConfig(
            "com/orientechnologies/lucene/integration/orientdb-simple-server-config.xml");
    remote = new YouTrackDB("remote:localhost", "root", "test", YouTrackDBConfig.defaultConfig());

    remote.execute(
        "create database LuceneCreateIndexIntegrationTest plocal users(admin identified by 'admin'"
            + " role admin) ");
    final ODatabaseSession session =
        remote.open("LuceneCreateIndexIntegrationTest", "admin", "admin");

    session.command("create class Person");
    session.command("create property Person.name STRING");
    session.command("create property Person.surname STRING");

    final OElement doc = session.newElement("Person");
    doc.setProperty("name", "Jon");
    doc.setProperty("surname", "Snow");
    session.begin();
    session.save(doc);
    session.commit();
    session.close();
  }

  @Test
  public void testCreateIndexJavaAPI() {
    final ODatabaseSessionInternal session =
        (ODatabaseSessionInternal) remote.open("LuceneCreateIndexIntegrationTest", "admin",
            "admin");
    OClass person = session.getMetadata().getSchema().getClass("Person");

    if (person == null) {
      person = session.getMetadata().getSchema().createClass("Person");
    }
    if (!person.existsProperty("name")) {
      person.createProperty(session, "name", OType.STRING);
    }
    if (!person.existsProperty("surname")) {
      person.createProperty(session, "surname", OType.STRING);
    }

    person.createIndex(session,
        "Person.firstName_lastName",
        "FULLTEXT",
        null,
        null,
        "LUCENE", new String[]{"name", "surname"});
    Assert.assertTrue(
        session.getMetadata().getSchema().getClass("Person")
            .areIndexed(session, "name", "surname"));
  }

  @After
  public void after() {
    remote.drop("LuceneCreateIndexIntegrationTest");
    server0.shutdown();
  }
}
