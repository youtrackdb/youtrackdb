package com.jetbrains.youtrack.db.internal.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneFreezeReleaseTest extends LuceneBaseTest {

  @Before
  public void setUp() throws Exception {

    dropDatabase();
    createDatabase(DatabaseType.PLOCAL);
  }

  @Test
  public void freezeReleaseTest() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass person = schema.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.begin();
    db.save(((EntityImpl) db.newEntity("Person")).field("name", "John"));
    db.commit();

    ResultSet results = db.query("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    db.freeze();

    results = db.command("select from Person where search_class('John')=true");
    assertThat(results).hasSize(1);
    results.close();

    db.release();

    EntityImpl doc = db.newInstance("Person");
    doc.field("name", "John");

    db.begin();
    db.save(doc);
    db.commit();

    results = db.query("select from Person where search_class('John')=true");
    assertThat(results).hasSize(2);
    results.close();
  }

  // With double calling freeze/release
  @Test
  public void freezeReleaseMisUsageTest() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass person = schema.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.begin();
    db.save(((EntityImpl) db.newEntity("Person")).field("name", "John"));
    db.commit();

    ResultSet results = db.command("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    db.freeze();

    db.freeze();

    results = db.command("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    db.release();
    db.release();

    db.begin();
    db.save(((EntityImpl) db.newEntity("Person")).field("name", "John"));
    db.commit();

    results = db.command("select from Person where search_class('John')=true");
    assertThat(results).hasSize(2);
    results.close();
  }
}
