package com.orientechnologies.lucene.test;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.io.File;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneFreezeReleaseTest {

  @Before
  public void setUp() throws Exception {
    FileUtils.deleteRecursively(new File("./target/freezeRelease"));
  }

  @Test
  public void freezeReleaseTest() {
    if (isWindows()) {
      return;
    }

    DatabaseSessionInternal db = new DatabaseDocumentTx("plocal:target/freezeRelease");

    db.create();

    Schema schema = db.getMetadata().getSchema();
    SchemaClass person = schema.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE").close();

    db.begin();
    db.save(new EntityImpl("Person").field("name", "John"));
    db.commit();

    try {

      ResultSet results = db.query("select from Person where name lucene 'John'");
      Assert.assertEquals(1, results.stream().count());
      db.freeze();

      results = db.query("select from Person where name lucene 'John'");
      Assert.assertEquals(1, results.stream().count());

      db.release();

      db.begin();
      db.save(new EntityImpl("Person").field("name", "John"));
      db.commit();

      results = db.query("select from Person where name lucene 'John'");
      Assert.assertEquals(2, results.stream().count());

    } finally {

      db.drop();
    }
  }

  // With double calling freeze/release
  @Test
  public void freezeReleaseMisUsageTest() {
    if (isWindows()) {
      return;
    }

    DatabaseSessionInternal db = new DatabaseDocumentTx("plocal:target/freezeRelease");

    db.create();

    Schema schema = db.getMetadata().getSchema();
    SchemaClass person = schema.createClass("Person");
    person.createProperty(db, "name", PropertyType.STRING);

    db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE").close();

    db.begin();
    db.save(new EntityImpl("Person").field("name", "John"));
    db.commit();

    try {

      ResultSet results = db.query("select from Person where name lucene 'John'");
      Assert.assertEquals(1, results.stream().count());

      db.freeze();

      db.freeze();

      results = db.query("select from Person where name lucene 'John'");
      Assert.assertEquals(1, results.stream().count());

      db.release();
      db.release();

      db.begin();
      db.save(new EntityImpl("Person").field("name", "John"));
      db.commit();

      results = db.query("select from Person where name lucene 'John'");
      Assert.assertEquals(2, results.stream().count());

    } finally {
      db.drop();
    }
  }

  private boolean isWindows() {
    final String osName = System.getProperty("os.name").toLowerCase();
    return osName.contains("win");
  }
}
