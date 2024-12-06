package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import org.junit.Ignore;
import org.junit.Test;

public class TestManyProperties {

  @Test
  @Ignore
  public void test() {
    try (YouTrackDB orientdb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      orientdb
          .execute("create database test memory users(admin identified by 'admin' role admin)")
          .close();
      try (DatabaseSession session = orientdb.open("test", "admin", "admin")) {
        SchemaClass clazz = session.createClass("test");
        clazz.createProperty(session, "property1", PropertyType.STRING);
        clazz.createProperty(session, "property2", PropertyType.STRING);
        clazz.createProperty(session, "property3", PropertyType.STRING);
        clazz.createProperty(session, "property4", PropertyType.STRING);
        clazz.createProperty(session, "property5", PropertyType.STRING);
        clazz.createProperty(session, "property6", PropertyType.STRING);
        clazz.createProperty(session, "property7", PropertyType.STRING);
        clazz.createProperty(session, "property8", PropertyType.STRING);
        clazz.createProperty(session, "property9", PropertyType.STRING);
        clazz.createProperty(session, "property10", PropertyType.STRING);
        clazz.createProperty(session, "property11", PropertyType.STRING);
        clazz.createProperty(session, "property12", PropertyType.STRING);
        clazz.createProperty(session, "property13", PropertyType.STRING);
        clazz.createProperty(session, "property14", PropertyType.STRING);
        clazz.createProperty(session, "property15", PropertyType.STRING);
        clazz.createProperty(session, "property16", PropertyType.STRING);
        clazz.createProperty(session, "property17", PropertyType.STRING);
        clazz.createProperty(session, "property18", PropertyType.STRING);
        clazz.createProperty(session, "property19", PropertyType.STRING);
        clazz.createProperty(session, "property20", PropertyType.STRING);
        clazz.createProperty(session, "property21", PropertyType.STRING);
        clazz.createProperty(session, "property22", PropertyType.STRING);
        clazz.createProperty(session, "property23", PropertyType.STRING);
        clazz.createProperty(session, "property24", PropertyType.STRING);

        try (ResultSet set =
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
