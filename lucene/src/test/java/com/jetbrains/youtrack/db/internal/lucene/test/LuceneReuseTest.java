package com.jetbrains.youtrack.db.internal.lucene.test;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneReuseTest extends BaseLuceneTest {

  @Test
  public void shouldUseTheRightIndex() {

    Schema schema = db.getMetadata().getSchema();

    SchemaClass cls = schema.createClass("Reuse");
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createProperty(db, "date", PropertyType.DATETIME);
    cls.createProperty(db, "surname", PropertyType.STRING);
    cls.createProperty(db, "age", PropertyType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE").close();
    db.command("create index Reuse.surname on Reuse (surname) FULLTEXT ENGINE LUCENE").close();

    for (int i = 0; i < 10; i++) {
      db.begin();
      db.save(
          ((EntityImpl) db.newEntity("Reuse"))
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
      db.commit();
    }
    ResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and surname LUCENE 'Reese'");

    Assert.assertEquals(10, results.stream().count());

    results = db.command("SELECT FROM Reuse WHERE surname LUCENE 'Reese' and name='John'");

    Assert.assertEquals(10, results.stream().count());
  }

  @Test
  public void shouldUseTheRightLuceneIndex() {

    Schema schema = db.getMetadata().getSchema();

    SchemaClass cls = schema.createClass("Reuse");
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createProperty(db, "date", PropertyType.DATETIME);
    cls.createProperty(db, "surname", PropertyType.STRING);
    cls.createProperty(db, "age", PropertyType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE").close();

    // lucene on name and surname
    db.command("create index Reuse.name_surname on Reuse (name,surname) FULLTEXT ENGINE LUCENE")
        .close();

    for (int i = 0; i < 10; i++) {
      db.begin();
      db.save(
          ((EntityImpl) db.newEntity("Reuse"))
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
      db.commit();
    }

    // additional record
    db.begin();
    db.save(
        ((EntityImpl) db.newEntity("Reuse"))
            .field("name", "John")
            .field("date", new Date())
            .field("surname", "Franklin")
            .field("age", 11));
    db.commit();
    ResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and [name,surname] LUCENE 'Reese'");

    Assert.assertEquals(10, results.stream().count());

    results = db.command("SELECT FROM Reuse WHERE [name,surname] LUCENE 'Reese' and name='John'");

    Assert.assertEquals(10, results.stream().count());

    results =
        db.command(
            "SELECT FROM Reuse WHERE name='John' and [name,surname] LUCENE '(surname:Franklin)'");

    Assert.assertEquals(1, results.stream().count());
  }
}
