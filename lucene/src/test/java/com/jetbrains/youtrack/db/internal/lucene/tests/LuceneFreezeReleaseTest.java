package com.jetbrains.youtrack.db.internal.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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

    Schema schema = session.getMetadata().getSchema();
    var person = schema.createClass("Person");
    person.createProperty(session, "name", PropertyType.STRING);

    session.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    session.begin();
    ((EntityImpl) session.newEntity("Person")).field("name", "John");
    session.commit();

    var results = session.query("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    session.freeze();

    results = session.command("select from Person where search_class('John')=true");
    assertThat(results).hasSize(1);
    results.close();

    session.release();

    EntityImpl doc = session.newInstance("Person");
    doc.field("name", "John");

    session.begin();
    session.commit();

    results = session.query("select from Person where search_class('John')=true");
    assertThat(results).hasSize(2);
    results.close();
  }

  // With double calling freeze/release
  @Test
  public void freezeReleaseMisUsageTest() {

    Schema schema = session.getMetadata().getSchema();
    var person = schema.createClass("Person");
    person.createProperty(session, "name", PropertyType.STRING);

    session.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    session.begin();
    ((EntityImpl) session.newEntity("Person")).field("name", "John");
    session.commit();

    var results = session.command("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    session.freeze();

    session.freeze();

    results = session.command("select from Person where search_class('John')=true");

    assertThat(results).hasSize(1);
    results.close();

    session.release();
    session.release();

    session.begin();
    ((EntityImpl) session.newEntity("Person")).field("name", "John");
    session.commit();

    results = session.command("select from Person where search_class('John')=true");
    assertThat(results).hasSize(2);
    results.close();
  }
}
