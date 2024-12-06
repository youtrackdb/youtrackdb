package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import org.junit.Assert;
import org.junit.Test;

public class CompositeIndexSQLInsertTest extends DbTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    Schema schema = db.getMetadata().getSchema();
    SchemaClass book = schema.createClass("Book");
    book.createProperty(db, "author", PropertyType.STRING);
    book.createProperty(db, "title", PropertyType.STRING);
    book.createProperty(db, "publicationYears", PropertyType.EMBEDDEDLIST, PropertyType.INTEGER);
    book.createIndex(db, "books", "unique", "author", "title", "publicationYears");

    book.createProperty(db, "nullKey1", PropertyType.STRING);
    EntityImpl indexOptions = new EntityImpl();
    indexOptions.field("ignoreNullValues", true);
    book.createIndex(db,
        "indexignoresnulls", "NOTUNIQUE", null, indexOptions, new String[]{"nullKey1"});
  }

  @Test
  public void testCompositeIndexWithRangeAndContains() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("CompositeIndexWithRangeAndConditions");
    clazz.createProperty(db, "id", PropertyType.INTEGER);
    clazz.createProperty(db, "bar", PropertyType.INTEGER);
    clazz.createProperty(db, "tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    clazz.createProperty(db, "name", PropertyType.STRING);

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

    ResultSet res =
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
