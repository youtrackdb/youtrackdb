package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneFreezeReleaseTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {

    dropDatabase();
    createDatabase(ODatabaseType.PLOCAL);
  }

  @Test
  public void freezeReleaseTest() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass person = schema.createClass("Person");
    person.createProperty(db, "name", YTType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.begin();
    db.save(new EntityImpl("Person").field("name", "John"));
    db.commit();

    YTResultSet results = db.query("select from Person where search_class('John')=true");

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

    YTSchema schema = db.getMetadata().getSchema();
    YTClass person = schema.createClass("Person");
    person.createProperty(db, "name", YTType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE");

    db.begin();
    db.save(new EntityImpl("Person").field("name", "John"));
    db.commit();

    YTResultSet results = db.command("select from Person where search_class('John')=true");

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
    db.save(new EntityImpl("Person").field("name", "John"));
    db.commit();

    results = db.command("select from Person where search_class('John')=true");
    assertThat(results).hasSize(2);
    results.close();
  }
}
