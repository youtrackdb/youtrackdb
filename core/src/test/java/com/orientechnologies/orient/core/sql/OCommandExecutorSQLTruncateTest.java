package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.io.IOException;
import org.junit.Test;

public class OCommandExecutorSQLTruncateTest extends DBTestBase {

  @Test
  public void testTruncatePlain() {
    YTClass vcl = db.getMetadata().getSchema().createClass("A");
    db.getMetadata().getSchema().createClass("ab", vcl);

    db.begin();
    YTDocument doc = new YTDocument("A");
    db.save(doc);
    db.commit();

    db.begin();
    doc = new YTDocument("ab");
    db.save(doc);
    db.commit();

    YTResultSet ret = db.command("truncate class A ");
    assertEquals((long) ret.next().getProperty("count"), 1L);
  }

  @Test
  public void testTruncateAPI() throws IOException {
    db.getMetadata().getSchema().createClass("A");

    db.begin();
    YTDocument doc = new YTDocument("A");
    db.save(doc);
    db.commit();

    db.getMetadata().getSchema().getClasses().stream()
        .filter(oClass -> !oClass.getName().startsWith("OSecurity")) //
        .forEach(
            oClass -> {
              if (oClass.count(db) > 0) {
                db.command("truncate class " + oClass.getName() + " POLYMORPHIC UNSAFE").close();
              }
            });
  }

  @Test
  public void testTruncatePolimorphic() {
    YTClass vcl = db.getMetadata().getSchema().createClass("A");
    db.getMetadata().getSchema().createClass("ab", vcl);

    db.begin();
    YTDocument doc = new YTDocument("A");
    db.save(doc);
    db.commit();

    db.begin();
    doc = new YTDocument("ab");
    db.save(doc);
    db.commit();

    try (YTResultSet res = db.command("truncate class A POLYMORPHIC")) {
      assertEquals((long) res.next().getProperty("count"), 1L);
      assertEquals((long) res.next().getProperty("count"), 1L);
    }
  }
}
