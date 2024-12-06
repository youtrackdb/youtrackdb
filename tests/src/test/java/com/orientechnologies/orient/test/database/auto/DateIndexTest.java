package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 10/21/13
 */
@Test
public class DateIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public DateIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();

    SchemaClass dateIndexTest = schema.createClass("DateIndexTest");

    dateIndexTest.createProperty(database, "dateField", PropertyType.DATE);
    dateIndexTest.createProperty(database, "dateTimeField", PropertyType.DATETIME);

    dateIndexTest.createProperty(database, "dateList", PropertyType.EMBEDDEDLIST,
        PropertyType.DATE);
    dateIndexTest.createProperty(database, "dateTimeList", PropertyType.EMBEDDEDLIST,
        PropertyType.DATETIME);

    dateIndexTest.createProperty(database, "value", PropertyType.STRING);

    dateIndexTest.createIndex(database, "DateIndexTestDateIndex", SchemaClass.INDEX_TYPE.UNIQUE,
        "dateField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value", "dateField");

    dateIndexTest.createIndex(database,
        "DateIndexTestDateTimeIndex", SchemaClass.INDEX_TYPE.UNIQUE, "dateTimeField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value", "dateTimeField");

    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateListIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value", "dateList");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeListIndex", SchemaClass.INDEX_TYPE.UNIQUE, "value",
        "dateTimeList");

    dateIndexTest.createIndex(database,
        "DateIndexTestDateHashIndex", SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "dateField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateHashIndex",
        SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateField");

    dateIndexTest.createIndex(database,
        "DateIndexTestDateTimeHashIndex", SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "dateTimeField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeHashIndex",
        SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateTimeField");

    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateListHashIndex",
        SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateList");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeListHashIndex",
        SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateTimeList");
  }

  public void testDateIndexes() {
    checkEmbeddedDB();

    final Date dateOne = new Date();

    final Date dateTwo = new Date(dateOne.getTime() + 24 * 60 * 60 * 1000 + 100);

    final EntityImpl dateDoc = new EntityImpl("DateIndexTest");

    dateDoc.field("dateField", dateOne);
    dateDoc.field("dateTimeField", dateTwo);

    final List<Date> dateList = new ArrayList<>();

    final Date dateThree = new Date(dateOne.getTime() + 100);
    final Date dateFour = new Date(dateThree.getTime() + 24 * 60 * 60 * 1000 + 100);

    dateList.add(new Date(dateThree.getTime()));
    dateList.add(new Date(dateFour.getTime()));

    final List<Date> dateTimeList = new ArrayList<>();

    dateTimeList.add(new Date(dateThree.getTime()));
    dateTimeList.add(new Date(dateFour.getTime()));

    dateDoc.field("dateList", dateList);
    dateDoc.field("dateTimeList", dateTimeList);

    dateDoc.field("value", "v1");

    database.begin();
    dateDoc.save();
    database.commit();

    final Index dateIndexTestDateIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateIndex");
    try (Stream<RID> stream = dateIndexTestDateIndex.getInternal().getRids(database, dateOne)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream = dateIndexTestDateIndex.getInternal().getRids(database, dateTwo)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestDateTimeIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateTimeIndex");
    try (Stream<RID> stream = dateIndexTestDateTimeIndex.getInternal()
        .getRids(database, dateTwo)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream = dateIndexTestDateTimeIndex.getInternal()
        .getRids(database, dateOne)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestValueDateIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateIndex");
    try (Stream<RID> stream =
        dateIndexTestValueDateIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateOne))) {
      Assert.assertEquals((stream.findAny().orElse(null)), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateTwo))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestValueDateTimeIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateTimeIndex");
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateTwo))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateOne))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestValueDateListIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListIndex");

    try (Stream<RID> stream =
        dateIndexTestValueDateListIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateListIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final Index dateIndexTestValueDateTimeListIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListIndex");
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeListIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeListIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final Index dateIndexTestDateHashIndexIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateHashIndex");
    try (Stream<RID> stream = dateIndexTestDateHashIndexIndex.getInternal()
        .getRids(database, dateOne)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream = dateIndexTestDateHashIndexIndex.getInternal()
        .getRids(database, dateTwo)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestDateTimeHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateTimeHashIndex");
    try (Stream<RID> stream = dateIndexTestDateTimeHashIndex.getInternal()
        .getRids(database, dateTwo)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream = dateIndexTestDateTimeHashIndex.getInternal()
        .getRids(database, dateOne)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestValueDateHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateHashIndex");
    try (Stream<RID> stream =
        dateIndexTestValueDateHashIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateOne))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateHashIndex.getInternal()
            .getRids(database, new CompositeKey("v1", dateTwo))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestValueDateTimeHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateTimeHashIndex");
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeHashIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateTwo))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeHashIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateOne))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final Index dateIndexTestValueDateListHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListHashIndex");

    try (Stream<RID> stream =
        dateIndexTestValueDateListHashIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateListHashIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final Index dateIndexTestValueDateTimeListHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListHashIndex");
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeListHashIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<RID> stream =
        dateIndexTestValueDateTimeListHashIndex
            .getInternal()
            .getRids(database, new CompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
  }
}
