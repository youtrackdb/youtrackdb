package com.orientechnologies.orient.core.sql.update;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import org.junit.Test;

public class SQLUpdateMapTest extends DBTestBase {

  @Test
  public void testMapPut() {

    YTDocument ret;
    YTDocument ret1;
    db.command("create class vRecord").close();
    db.command("create property vRecord.attrs EMBEDDEDMAP ").close();

    db.begin();
    try (OResultSet rs = db.command("insert into vRecord (title) values('first record')")) {
      ret = (YTDocument) rs.next().getRecord().get();
    }

    try (OResultSet rs = db.command("insert into vRecord (title) values('second record')")) {
      ret1 = (YTDocument) rs.next().getRecord().get();
    }
    db.commit();

    db.begin();
    db.command(
            "update " + ret.getIdentity() + " set attrs =  {'test1':" + ret1.getIdentity() + " }")
        .close();
    db.commit();
    reOpen("admin", "adminpwd");

    db.begin();
    db.command("update " + ret.getIdentity() + " set attrs['test'] = 'test value' ").close();
    db.commit();

    ret = db.bindToSession(ret);
    assertEquals(2, ((Map) ret.field("attrs")).size());
    assertEquals("test value", ((Map) ret.field("attrs")).get("test"));
    assertEquals(ret1.getIdentity(), ((Map) ret.field("attrs")).get("test1"));
  }
}
