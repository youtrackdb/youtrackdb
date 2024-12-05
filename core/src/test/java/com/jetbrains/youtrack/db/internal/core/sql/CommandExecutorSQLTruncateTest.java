package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.IOException;
import org.junit.Test;

public class CommandExecutorSQLTruncateTest extends DBTestBase {

  @Test
  public void testTruncatePlain() {
    YTClass vcl = db.getMetadata().getSchema().createClass("A");
    db.getMetadata().getSchema().createClass("ab", vcl);

    db.begin();
    EntityImpl doc = new EntityImpl("A");
    db.save(doc);
    db.commit();

    db.begin();
    doc = new EntityImpl("ab");
    db.save(doc);
    db.commit();

    YTResultSet ret = db.command("truncate class A ");
    assertEquals((long) ret.next().getProperty("count"), 1L);
  }

  @Test
  public void testTruncateAPI() throws IOException {
    db.getMetadata().getSchema().createClass("A");

    db.begin();
    EntityImpl doc = new EntityImpl("A");
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
    EntityImpl doc = new EntityImpl("A");
    db.save(doc);
    db.commit();

    db.begin();
    doc = new EntityImpl("ab");
    db.save(doc);
    db.commit();

    try (YTResultSet res = db.command("truncate class A POLYMORPHIC")) {
      assertEquals((long) res.next().getProperty("count"), 1L);
      assertEquals((long) res.next().getProperty("count"), 1L);
    }
  }
}
