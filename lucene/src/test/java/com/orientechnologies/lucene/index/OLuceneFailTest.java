package com.orientechnologies.lucene.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.db.YouTrackDBConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OLuceneFailTest {

  private YouTrackDB odb;

  @Before
  public void before() {
    odb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig());
    odb.execute("create database tdb memory users (admin identified by 'admpwd' role admin)")
        .close();
  }

  @After
  public void after() {
    odb.close();
  }

  @Test
  public void test() {
    try (YTDatabaseSession session = odb.open("tdb", "admin", "admpwd")) {
      session.command("create property V.text string").close();
      session.command("create index lucene_index on V(text) FULLTEXT ENGINE LUCENE").close();
      try {
        session.query("select from V where search_class('*this breaks') = true").close();
      } catch (Exception e) {
      }
      session.query("select from V ").close();
    }
  }
}
