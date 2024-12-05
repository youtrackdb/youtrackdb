package com.orientechnologies.lucene.test;

import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneReuseTest extends BaseLuceneTest {

  @Test
  public void shouldUseTheRightIndex() {

    YTSchema schema = db.getMetadata().getSchema();

    YTClass cls = schema.createClass("Reuse");
    cls.createProperty(db, "name", YTType.STRING);
    cls.createProperty(db, "date", YTType.DATETIME);
    cls.createProperty(db, "surname", YTType.STRING);
    cls.createProperty(db, "age", YTType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE").close();
    db.command("create index Reuse.surname on Reuse (surname) FULLTEXT ENGINE LUCENE").close();

    for (int i = 0; i < 10; i++) {
      db.begin();
      db.save(
          new YTEntityImpl("Reuse")
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
      db.commit();
    }
    YTResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and surname LUCENE 'Reese'");

    Assert.assertEquals(10, results.stream().count());

    results = db.command("SELECT FROM Reuse WHERE surname LUCENE 'Reese' and name='John'");

    Assert.assertEquals(10, results.stream().count());
  }

  @Test
  public void shouldUseTheRightLuceneIndex() {

    YTSchema schema = db.getMetadata().getSchema();

    YTClass cls = schema.createClass("Reuse");
    cls.createProperty(db, "name", YTType.STRING);
    cls.createProperty(db, "date", YTType.DATETIME);
    cls.createProperty(db, "surname", YTType.STRING);
    cls.createProperty(db, "age", YTType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE").close();

    // lucene on name and surname
    db.command("create index Reuse.name_surname on Reuse (name,surname) FULLTEXT ENGINE LUCENE")
        .close();

    for (int i = 0; i < 10; i++) {
      db.begin();
      db.save(
          new YTEntityImpl("Reuse")
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
      db.commit();
    }

    // additional record
    db.begin();
    db.save(
        new YTEntityImpl("Reuse")
            .field("name", "John")
            .field("date", new Date())
            .field("surname", "Franklin")
            .field("age", 11));
    db.commit();
    YTResultSet results =
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
