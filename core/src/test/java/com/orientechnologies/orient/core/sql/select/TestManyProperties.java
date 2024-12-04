package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Ignore;
import org.junit.Test;

public class TestManyProperties {

  @Test
  @Ignore
  public void test() {
    try (YouTrackDB orientdb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      orientdb
          .execute("create database test memory users(admin identified by 'admin' role admin)")
          .close();
      try (ODatabaseSession session = orientdb.open("test", "admin", "admin")) {
        OClass clazz = session.createClass("test");
        clazz.createProperty(session, "property1", OType.STRING);
        clazz.createProperty(session, "property2", OType.STRING);
        clazz.createProperty(session, "property3", OType.STRING);
        clazz.createProperty(session, "property4", OType.STRING);
        clazz.createProperty(session, "property5", OType.STRING);
        clazz.createProperty(session, "property6", OType.STRING);
        clazz.createProperty(session, "property7", OType.STRING);
        clazz.createProperty(session, "property8", OType.STRING);
        clazz.createProperty(session, "property9", OType.STRING);
        clazz.createProperty(session, "property10", OType.STRING);
        clazz.createProperty(session, "property11", OType.STRING);
        clazz.createProperty(session, "property12", OType.STRING);
        clazz.createProperty(session, "property13", OType.STRING);
        clazz.createProperty(session, "property14", OType.STRING);
        clazz.createProperty(session, "property15", OType.STRING);
        clazz.createProperty(session, "property16", OType.STRING);
        clazz.createProperty(session, "property17", OType.STRING);
        clazz.createProperty(session, "property18", OType.STRING);
        clazz.createProperty(session, "property19", OType.STRING);
        clazz.createProperty(session, "property20", OType.STRING);
        clazz.createProperty(session, "property21", OType.STRING);
        clazz.createProperty(session, "property22", OType.STRING);
        clazz.createProperty(session, "property23", OType.STRING);
        clazz.createProperty(session, "property24", OType.STRING);

        try (OResultSet set =
            session.query(
                "SELECT FROM test WHERE (((property1 is null) or (property1 = #107:150)) and"
                    + " ((property2 is null) or (property2 = #107:150)) and ((property3 is null) or"
                    + " (property3 = #107:150)) and ((property4 is null) or (property4 = #107:150))"
                    + " and ((property5 is null) or (property5 = #107:150)) and ((property6 is"
                    + " null) or (property6 = #107:150)) and ((property7 is null) or (property7 ="
                    + " #107:150)) and ((property8 is null) or (property8 = #107:150)) and"
                    + " ((property9 is null) or (property9 = #107:150)) and ((property10 is null)"
                    + " or (property10 = #107:150)) and ((property11 is null) or (property11 ="
                    + " #107:150)) and ((property12 is null) or (property12 = #107:150)) and"
                    + " ((property13 is null) or (property13 = #107:150)) and ((property14 is null)"
                    + " or (property14 = #107:150)) and ((property15 is null) or (property15 ="
                    + " #107:150)) and ((property16 is null) or (property16 = #107:150)) and"
                    + " ((property17 is null) or (property17 = #107:150)))")) {
          assertEquals(set.stream().count(), 0);
        }
      }
    }
  }
}
