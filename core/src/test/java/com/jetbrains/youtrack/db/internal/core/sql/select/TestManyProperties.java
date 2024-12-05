package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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
      try (YTDatabaseSession session = orientdb.open("test", "admin", "admin")) {
        YTClass clazz = session.createClass("test");
        clazz.createProperty(session, "property1", YTType.STRING);
        clazz.createProperty(session, "property2", YTType.STRING);
        clazz.createProperty(session, "property3", YTType.STRING);
        clazz.createProperty(session, "property4", YTType.STRING);
        clazz.createProperty(session, "property5", YTType.STRING);
        clazz.createProperty(session, "property6", YTType.STRING);
        clazz.createProperty(session, "property7", YTType.STRING);
        clazz.createProperty(session, "property8", YTType.STRING);
        clazz.createProperty(session, "property9", YTType.STRING);
        clazz.createProperty(session, "property10", YTType.STRING);
        clazz.createProperty(session, "property11", YTType.STRING);
        clazz.createProperty(session, "property12", YTType.STRING);
        clazz.createProperty(session, "property13", YTType.STRING);
        clazz.createProperty(session, "property14", YTType.STRING);
        clazz.createProperty(session, "property15", YTType.STRING);
        clazz.createProperty(session, "property16", YTType.STRING);
        clazz.createProperty(session, "property17", YTType.STRING);
        clazz.createProperty(session, "property18", YTType.STRING);
        clazz.createProperty(session, "property19", YTType.STRING);
        clazz.createProperty(session, "property20", YTType.STRING);
        clazz.createProperty(session, "property21", YTType.STRING);
        clazz.createProperty(session, "property22", YTType.STRING);
        clazz.createProperty(session, "property23", YTType.STRING);
        clazz.createProperty(session, "property24", YTType.STRING);

        try (YTResultSet set =
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
