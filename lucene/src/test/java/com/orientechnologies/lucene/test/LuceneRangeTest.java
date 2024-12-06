package com.orientechnologies.lucene.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.document.DateTools;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class LuceneRangeTest extends BaseLuceneTest {

  @Before
  public void setUp() throws Exception {
    Schema schema = db.getMetadata().getSchema();

    SchemaClass cls = schema.createClass("Person");
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createProperty(db, "surname", PropertyType.STRING);
    cls.createProperty(db, "date", PropertyType.DATETIME);
    cls.createProperty(db, "age", PropertyType.INTEGER);

    List<String> names =
        Arrays.asList(
            "John",
            "Robert",
            "Jane",
            "andrew",
            "Scott",
            "luke",
            "Enriquez",
            "Luis",
            "Gabriel",
            "Sara");
    for (int i = 0; i < 10; i++) {
      db.begin();
      db.save(
          new EntityImpl("Person")
              .field("name", names.get(i))
              .field("surname", "Reese")
              // from today back one day a time
              .field("date", System.currentTimeMillis() - (i * 3600 * 24 * 1000))
              .field("age", i));
      db.commit();
    }
  }

  @Test
  public void shouldUseRangeQueryOnSingleIntegerField() {
    db.command("create index Person.age on Person(age) FULLTEXT ENGINE LUCENE").close();

    db.begin();
    assertThat(
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Person.age")
            .getInternal()
            .size(db))
        .isEqualTo(10);
    db.commit();

    // range
    ResultSet results = db.command("SELECT FROM Person WHERE age LUCENE 'age:[5 TO 6]'");

    assertThat(results).hasSize(2);

    // single value
    results = db.command("SELECT FROM Person WHERE age LUCENE 'age:5'");

    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldUseRangeQueryOnSingleDateField() {
    db.command("create index Person.date on Person(date) FULLTEXT ENGINE LUCENE").close();

    db.begin();
    assertThat(
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Person.date")
            .getInternal()
            .size(db))
        .isEqualTo(10);
    db.commit();

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo =
        DateTools.timeToString(
            System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    // range
    ResultSet results =
        db.command(
            "SELECT FROM Person WHERE date LUCENE 'date:[" + fiveDaysAgo + " TO " + today + "]'");

    assertThat(results).hasSize(5);
  }

  @Test
  @Ignore
  public void shouldUseRangeQueryMultipleField() {
    db.command(
            "create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE LUCENE")
        .close();

    db.begin();
    assertThat(
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Person.composite")
            .getInternal()
            .size(db))
        .isEqualTo(10);
    db.commit();

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo =
        DateTools.timeToString(
            System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    // name and age range
    ResultSet results =
        db.query(
            "SELECT * FROM Person WHERE [name,surname,date,age] LUCENE 'age:[5 TO 6] name:robert "
                + " '");

    assertThat(results).hasSize(3);

    // date range
    results =
        db.query(
            "SELECT FROM Person WHERE [name,surname,date,age] LUCENE 'date:["
                + fiveDaysAgo
                + " TO "
                + today
                + "]'");

    assertThat(results).hasSize(5);

    // age and date range with MUST
    results =
        db.query(
            "SELECT FROM Person WHERE [name,surname,date,age] LUCENE '+age:[4 TO 7]  +date:["
                + fiveDaysAgo
                + " TO "
                + today
                + "]'");

    assertThat(results).hasSize(2);
  }

  @Test
  public void shouldUseRangeQueryMultipleFieldWithDirectIndexAccess() {
    db.command(
            "create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE LUCENE")
        .close();

    db.begin();
    assertThat(
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Person.composite")
            .getInternal()
            .size(db))
        .isEqualTo(10);
    db.commit();

    String today = DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE);
    String fiveDaysAgo =
        DateTools.timeToString(
            System.currentTimeMillis() - (5 * 3600 * 24 * 1000), DateTools.Resolution.MINUTE);

    // name and age range
    final Index index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.composite");
    try (Stream<RID> stream = index.getInternal().getRids(db, "name:luke  age:[5 TO 6]")) {
      assertThat(stream.count()).isEqualTo(2);
    }

    // date range
    try (Stream<RID> stream =
        index.getInternal().getRids(db, "date:[" + fiveDaysAgo + " TO " + today + "]")) {
      assertThat(stream.count()).isEqualTo(5);
    }

    // age and date range with MUST
    try (Stream<RID> stream =
        index
            .getInternal()
            .getRids(db, "+age:[4 TO 7]  +date:[" + fiveDaysAgo + " TO " + today + "]")) {
      assertThat(stream.count()).isEqualTo(2);
    }
  }

  @Test
  public void shouldFetchOnlyFromACluster() {
    db.command("create index Person.name on Person(name) FULLTEXT ENGINE LUCENE").close();

    db.begin();
    assertThat(
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Person.name")
            .getInternal()
            .size(db))
        .isEqualTo(10);
    db.commit();

    int cluster = db.getMetadata().getSchema().getClass("Person").getClusterIds()[1];

    ResultSet results =
        db.command("SELECT FROM Person WHERE name LUCENE '+_CLUSTER:" + cluster + "'");

    assertThat(results).hasSize(2);
  }
}
