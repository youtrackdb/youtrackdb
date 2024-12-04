package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InsertUnionValueTest {

  private YouTrackDB youTrackDB;

  @Before
  public void before() {
    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDB
        .execute(
            "create database ? memory users (admin identified by 'admpwd' role admin)",
            InsertUnionValueTest.class.getSimpleName())
        .close();
  }

  @After
  public void after() {
    youTrackDB.close();
  }

  @Test
  public void testUnionInsert() {
    try (YTDatabaseSession session =
        youTrackDB.open(InsertUnionValueTest.class.getSimpleName(), "admin", "admpwd")) {
      session.command("create class example extends V").close();
      session.command("create property example.metadata EMBEDDEDMAP").close();
      session
          .execute(
              "SQL",
              "  begin; "
                  + "  let $example = create vertex example;\n"
                  + "  let $a = {\"aKey\":\"aValue\"};\n"
                  + "  let $b = {\"anotherKey\":\"anotherValue\"};\n"
                  + "  let $u = unionAll($a, $b); \n"
                  + "\n"
                  + "  /* both of the following throw the exception and require to restart the"
                  + " server*/\n"
                  + "  update $example set metadata[\"something\"] = $u;\n"
                  + "  update $example set metadata.something = $u;"
                  + "  commit;")
          .close();
      long entries =
          session.query("select expand(metadata.something) from example").stream().count();
      assertEquals(entries, 2);
    }
  }
}
