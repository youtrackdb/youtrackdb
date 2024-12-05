package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.Date;
import org.junit.Test;

/**
 *
 */
public class OLuceneReuseTest extends OLuceneBaseTest {

  @Test
  public void shouldUseTheRightIndex() {

    YTSchema schema = db.getMetadata().getSchema();

    YTClass cls = schema.createClass("Reuse");
    cls.createProperty(db, "name", YTType.STRING);
    cls.createProperty(db, "date", YTType.DATETIME);
    cls.createProperty(db, "surname", YTType.STRING);
    cls.createProperty(db, "age", YTType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE");
    db.command("create index Reuse.surname on Reuse (surname) FULLTEXT ENGINE LUCENE");

    for (int i = 0; i < 10; i++) {
      db.begin();
      db.save(
          new EntityImpl("Reuse")
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
      db.commit();
    }

    YTResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and search_class('Reese') =true");

    assertThat(results).hasSize(10);

    results = db.command("SELECT FROM Reuse WHERE search_class('Reese')=true  and name='John'");

    assertThat(results).hasSize(10);
  }

  @Test
  public void shouldUseTheRightLuceneIndex() {

    YTSchema schema = db.getMetadata().getSchema();

    YTClass cls = schema.createClass("Reuse");
    cls.createProperty(db, "name", YTType.STRING);
    cls.createProperty(db, "date", YTType.DATETIME);
    cls.createProperty(db, "surname", YTType.STRING);
    cls.createProperty(db, "age", YTType.LONG);

    db.command("create index Reuse.composite on Reuse (name,surname,date,age) UNIQUE");

    // lucene on name and surname
    db.command("create index Reuse.name_surname on Reuse (name,surname) FULLTEXT ENGINE LUCENE");

    for (int i = 0; i < 10; i++) {
      db.begin();
      db.save(
          new EntityImpl("Reuse")
              .field("name", "John")
              .field("date", new Date())
              .field("surname", "Reese")
              .field("age", i));
      db.begin();
    }

    // additional record
    db.begin();
    db.save(
        new EntityImpl("Reuse")
            .field("name", "John")
            .field("date", new Date())
            .field("surname", "Franklin")
            .field("age", 11));
    db.commit();

    // exact query on name uses Reuse.conposite
    YTResultSet results =
        db.command("SELECT FROM Reuse WHERE name='John' and search_class('Reese')=true");

    assertThat(results).hasSize(10);

    results = db.command("SELECT FROM Reuse WHERE search_class('Reese')=true and name='John'");

    assertThat(results).hasSize(10);

    results =
        db.command(
            "SELECT FROM Reuse WHERE name='John' AND search_class('surname:Franklin') =true");

    assertThat(results).hasSize(1);
  }
}
