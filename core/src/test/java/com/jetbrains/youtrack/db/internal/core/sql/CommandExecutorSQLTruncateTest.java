package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import org.junit.Test;

public class CommandExecutorSQLTruncateTest extends DbTestBase {

  @Test
  public void testTruncatePlain() {
    var vcl = db.getMetadata().getSchema().createClass("A");
    db.getMetadata().getSchema().createClass("ab", vcl);

    db.begin();
    var doc = (EntityImpl) db.newEntity("A");
    db.save(doc);
    db.commit();

    db.begin();
    doc = (EntityImpl) db.newEntity("ab");
    db.save(doc);
    db.commit();

    var ret = db.command("truncate class A ");
    assertEquals(1L, (long) ret.next().getProperty("count"));
  }

  @Test
  public void testTruncateAPI() throws IOException {
    db.getMetadata().getSchema().createClass("A");

    db.begin();
    var doc = (EntityImpl) db.newEntity("A");
    db.save(doc);
    db.commit();

    db.getMetadata().getSchema().getClasses(db).stream()
        .filter(oClass -> !oClass.getName().startsWith("OSecurity")) //
        .forEach(
            oClass -> {
              if (((SchemaClassInternal) oClass).count(db) > 0) {
                db.command("truncate class " + oClass.getName() + " POLYMORPHIC UNSAFE").close();
              }
            });
  }

  @Test
  public void testTruncatePolimorphic() {
    var vcl = db.getMetadata().getSchema().createClass("A");
    db.getMetadata().getSchema().createClass("ab", vcl);

    db.begin();
    var doc = (EntityImpl) db.newEntity("A");
    db.save(doc);
    db.commit();

    db.begin();
    doc = (EntityImpl) db.newEntity("ab");
    db.save(doc);
    db.commit();

    try (var res = db.command("truncate class A POLYMORPHIC")) {
      assertEquals(1L, (long) res.next().getProperty("count"));
      assertEquals(1L, (long) res.next().getProperty("count"));
    }
  }
}
