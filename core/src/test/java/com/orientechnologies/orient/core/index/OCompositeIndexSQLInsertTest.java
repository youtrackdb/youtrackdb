package com.orientechnologies.orient.core.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class OCompositeIndexSQLInsertTest extends DBTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    OSchema schema = db.getMetadata().getSchema();
    OClass book = schema.createClass("Book");
    book.createProperty(db, "author", OType.STRING);
    book.createProperty(db, "title", OType.STRING);
    book.createProperty(db, "publicationYears", OType.EMBEDDEDLIST, OType.INTEGER);
    book.createIndex(db, "books", "unique", "author", "title", "publicationYears");

    book.createProperty(db, "nullKey1", OType.STRING);
    ODocument indexOptions = new ODocument();
    indexOptions.field("ignoreNullValues", true);
    book.createIndex(db,
        "indexignoresnulls", "NOTUNIQUE", null, indexOptions, new String[]{"nullKey1"});
  }

  @Test
  public void testCompositeIndexWithRangeAndContains() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass clazz = schema.createClass("CompositeIndexWithRangeAndConditions");
    clazz.createProperty(db, "id", OType.INTEGER);
    clazz.createProperty(db, "bar", OType.INTEGER);
    clazz.createProperty(db, "tags", OType.EMBEDDEDLIST, OType.STRING);
    clazz.createProperty(db, "name", OType.STRING);

    db.command(
            "create index CompositeIndexWithRangeAndConditions_id_tags_name on"
                + " CompositeIndexWithRangeAndConditions (id, tags, name) NOTUNIQUE")
        .close();

    db.begin();
    db.command(
            "insert into CompositeIndexWithRangeAndConditions set id = 1, tags ="
                + " [\"green\",\"yellow\"] , name = \"Foo\", bar = 1")
        .close();
    db.command(
            "insert into CompositeIndexWithRangeAndConditions set id = 1, tags ="
                + " [\"blue\",\"black\"] , name = \"Foo\", bar = 14")
        .close();
    db.command(
            "insert into CompositeIndexWithRangeAndConditions set id = 1, tags = [\"white\"] , name"
                + " = \"Foo\"")
        .close();
    db.command(
            "insert into CompositeIndexWithRangeAndConditions set id = 1, tags ="
                + " [\"green\",\"yellow\"], name = \"Foo1\", bar = 14")
        .close();
    db.commit();

    OResultSet res =
        db.query("select from CompositeIndexWithRangeAndConditions where id > 0 and bar = 1");

    long count = res.stream().count();
    Assert.assertEquals(1, count);

    long count1 =
        db
            .query(
                "select from CompositeIndexWithRangeAndConditions where id = 1 and tags CONTAINS"
                    + " \"white\"")
            .stream()
            .count();
    Assert.assertEquals(count1, 1);

    long count2 =
        db
            .query(
                "select from CompositeIndexWithRangeAndConditions where id > 0 and tags CONTAINS"
                    + " \"white\"")
            .stream()
            .count();
    Assert.assertEquals(count2, 1);

    long count3 =
        db
            .query("select from CompositeIndexWithRangeAndConditions where id > 0 and bar = 1")
            .stream()
            .count();

    Assert.assertEquals(count3, 1);

    long count4 =
        db
            .query(
                "select from CompositeIndexWithRangeAndConditions where tags CONTAINS \"white\" and"
                    + " id > 0")
            .stream()
            .count();
    Assert.assertEquals(count4, 1);
  }
}
