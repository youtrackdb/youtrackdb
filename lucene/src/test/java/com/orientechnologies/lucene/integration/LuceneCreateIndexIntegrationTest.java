package com.orientechnologies.lucene.integration;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
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
    final YTDatabaseSession session =
        remote.open("LuceneCreateIndexIntegrationTest", "admin", "admin");

    session.command("create class Person");
    session.command("create property Person.name STRING");
    session.command("create property Person.surname STRING");

    final YTEntity doc = session.newElement("Person");
    doc.setProperty("name", "Jon");
    doc.setProperty("surname", "Snow");
    session.begin();
    session.save(doc);
    session.commit();
    session.close();
  }

  @Test
  public void testCreateIndexJavaAPI() {
    final YTDatabaseSessionInternal session =
        (YTDatabaseSessionInternal) remote.open("LuceneCreateIndexIntegrationTest", "admin",
            "admin");
    YTClass person = session.getMetadata().getSchema().getClass("Person");

    if (person == null) {
      person = session.getMetadata().getSchema().createClass("Person");
    }
    if (!person.existsProperty("name")) {
      person.createProperty(session, "name", YTType.STRING);
    }
    if (!person.existsProperty("surname")) {
      person.createProperty(session, "surname", YTType.STRING);
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
