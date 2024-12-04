package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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
public class OLuceneRangeTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    YTSchema schema = db.getMetadata().getSchema();

    YTClass cls = schema.createClass("Person");
    cls.createProperty(db, "name", YTType.STRING);
    cls.createProperty(db, "surname", YTType.STRING);
    cls.createProperty(db, "date", YTType.DATETIME);
    cls.createProperty(db, "age", YTType.INTEGER);
    cls.createProperty(db, "weight", YTType.FLOAT);

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
          new YTDocument("Person")
              .field("name", names.get(i))
              .field("surname", "Reese")
              // from today back one day a time
              .field("date", System.currentTimeMillis() - (i * 3600 * 24 * 1000))
              .field("age", i)
              .field("weight", i + 0.1f));
      db.commit();
    }
  }

  @Test
  public void shouldUseRangeQueryOnSingleFloatField() {

    //noinspection EmptyTryBlock
    try (final OResultSet command =
        db.command("create index Person.weight on Person(weight) FULLTEXT ENGINE LUCENE")) {
    }

    db.begin();
    assertThat(
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "Person.weight")
            .getInternal()
            .size(db))
        .isEqualTo(10);
    db.commit();

    // range
    try (final OResultSet results =
        db.command("SELECT FROM Person WHERE search_class('weight:[0.0 TO 1.1]') = true")) {
      assertThat(results).hasSize(2);
    }

    // single value
    try (final OResultSet results =
        db.command("SELECT FROM Person WHERE search_class('weight:7.1') = true")) {
      assertThat(results).hasSize(1);
    }
  }

  @Test
  public void shouldUseRangeQueryOnSingleIntegerField() {

    //noinspection EmptyTryBlock
    try (OResultSet command =
        db.command("create index Person.age on Person(age) FULLTEXT ENGINE LUCENE")) {
    }

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
    try (OResultSet results =
        db.command("SELECT FROM Person WHERE search_class('age:[5 TO 6]') = true")) {

      assertThat(results).hasSize(2);
    }

    // single value
    try (OResultSet results = db.command("SELECT FROM Person WHERE search_class('age:5') = true")) {
      assertThat(results).hasSize(1);
    }
  }

  @Test
  public void shouldUseRangeQueryOnSingleDateField() {
    //noinspection EmptyTryBlock
    try (OResultSet command =
        db.command("create index Person.date on Person(date) FULLTEXT ENGINE LUCENE")) {
    }

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
    try (final OResultSet results =
        db.command(
            "SELECT FROM Person WHERE search_class('date:["
                + fiveDaysAgo
                + " TO "
                + today
                + "]')=true")) {
      assertThat(results).hasSize(5);
    }
  }

  @Test
  @Ignore
  public void shouldUseRangeQueryMultipleField() {

    //noinspection EmptyTryBlock
    try (OResultSet command =
        db.command(
            "create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE"
                + " LUCENE")) {
    }

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
    try (OResultSet results =
        db.command("SELECT * FROM Person WHERE search_class('age:[5 TO 6] name:robert  ')=true")) {

      assertThat(results).hasSize(3);
    }

    // date range
    try (OResultSet results =
        db.command(
            "SELECT FROM Person WHERE search_class('date:["
                + fiveDaysAgo
                + " TO "
                + today
                + "]')=true")) {

      assertThat(results).hasSize(5);
    }

    // age and date range with MUST
    try (OResultSet results =
        db.command(
            "SELECT FROM Person WHERE search_class('+age:[4 TO 7]  +date:["
                + fiveDaysAgo
                + " TO "
                + today
                + "]')=true")) {
      assertThat(results).hasSize(2);
    }
  }

  @Test
  public void shouldUseRangeQueryMultipleFieldWithDirectIndexAccess() {
    //noinspection EmptyTryBlock
    try (OResultSet command =
        db.command(
            "create index Person.composite on Person(name,surname,date,age) FULLTEXT ENGINE"
                + " LUCENE")) {
    }

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

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.composite");

    // name and age range
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "name:luke  age:[5 TO 6]")) {
      assertThat(stream.count()).isEqualTo(2);
    }
    try (Stream<YTRID> stream =
        index.getInternal().getRids(db, "date:[" + fiveDaysAgo + " TO " + today + "]")) {
      assertThat(stream.count()).isEqualTo(5);
    }
    try (Stream<YTRID> stream =
        index
            .getInternal()
            .getRids(db, "+age:[4 TO 7]  +date:[" + fiveDaysAgo + " TO " + today + "]")) {
      assertThat(stream.count()).isEqualTo(2);
    }
    try (Stream<YTRID> stream = index.getInternal().getRids(db, "*:*")) {
      assertThat(stream.count()).isEqualTo(11);
    }
  }
}
