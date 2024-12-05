package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
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

    final YTSchema schema = database.getMetadata().getSchema();

    YTClass dateIndexTest = schema.createClass("DateIndexTest");

    dateIndexTest.createProperty(database, "dateField", YTType.DATE);
    dateIndexTest.createProperty(database, "dateTimeField", YTType.DATETIME);

    dateIndexTest.createProperty(database, "dateList", YTType.EMBEDDEDLIST, YTType.DATE);
    dateIndexTest.createProperty(database, "dateTimeList", YTType.EMBEDDEDLIST, YTType.DATETIME);

    dateIndexTest.createProperty(database, "value", YTType.STRING);

    dateIndexTest.createIndex(database, "DateIndexTestDateIndex", YTClass.INDEX_TYPE.UNIQUE,
        "dateField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateIndex", YTClass.INDEX_TYPE.UNIQUE, "value", "dateField");

    dateIndexTest.createIndex(database,
        "DateIndexTestDateTimeIndex", YTClass.INDEX_TYPE.UNIQUE, "dateTimeField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeIndex", YTClass.INDEX_TYPE.UNIQUE, "value", "dateTimeField");

    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateListIndex", YTClass.INDEX_TYPE.UNIQUE, "value", "dateList");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeListIndex", YTClass.INDEX_TYPE.UNIQUE, "value", "dateTimeList");

    dateIndexTest.createIndex(database,
        "DateIndexTestDateHashIndex", YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "dateField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateHashIndex",
        YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateField");

    dateIndexTest.createIndex(database,
        "DateIndexTestDateTimeHashIndex", YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "dateTimeField");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeHashIndex",
        YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateTimeField");

    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateListHashIndex",
        YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateList");
    dateIndexTest.createIndex(database,
        "DateIndexTestValueDateTimeListHashIndex",
        YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX,
        "value", "dateTimeList");
  }

  public void testDateIndexes() {
    checkEmbeddedDB();

    final Date dateOne = new Date();

    final Date dateTwo = new Date(dateOne.getTime() + 24 * 60 * 60 * 1000 + 100);

    final YTEntityImpl dateDoc = new YTEntityImpl("DateIndexTest");

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

    final OIndex dateIndexTestDateIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateIndex");
    try (Stream<YTRID> stream = dateIndexTestDateIndex.getInternal().getRids(database, dateOne)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream = dateIndexTestDateIndex.getInternal().getRids(database, dateTwo)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestDateTimeIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateTimeIndex");
    try (Stream<YTRID> stream = dateIndexTestDateTimeIndex.getInternal()
        .getRids(database, dateTwo)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream = dateIndexTestDateTimeIndex.getInternal()
        .getRids(database, dateOne)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateIndex");
    try (Stream<YTRID> stream =
        dateIndexTestValueDateIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateOne))) {
      Assert.assertEquals((stream.findAny().orElse(null)), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateTwo))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateTimeIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateTimeIndex");
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateTwo))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateOne))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateListIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListIndex");

    try (Stream<YTRID> stream =
        dateIndexTestValueDateListIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateListIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final OIndex dateIndexTestValueDateTimeListIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListIndex");
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeListIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeListIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final OIndex dateIndexTestDateHashIndexIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateHashIndex");
    try (Stream<YTRID> stream = dateIndexTestDateHashIndexIndex.getInternal()
        .getRids(database, dateOne)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream = dateIndexTestDateHashIndexIndex.getInternal()
        .getRids(database, dateTwo)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestDateTimeHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestDateTimeHashIndex");
    try (Stream<YTRID> stream = dateIndexTestDateTimeHashIndex.getInternal()
        .getRids(database, dateTwo)) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream = dateIndexTestDateTimeHashIndex.getInternal()
        .getRids(database, dateOne)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateHashIndex");
    try (Stream<YTRID> stream =
        dateIndexTestValueDateHashIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateOne))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateHashIndex.getInternal()
            .getRids(database, new OCompositeKey("v1", dateTwo))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateTimeHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateTimeHashIndex");
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeHashIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateTwo))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeHashIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateOne))) {
      Assert.assertFalse(stream.findAny().isPresent());
    }

    final OIndex dateIndexTestValueDateListHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListHashIndex");

    try (Stream<YTRID> stream =
        dateIndexTestValueDateListHashIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateListHashIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }

    final OIndex dateIndexTestValueDateTimeListHashIndex =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "DateIndexTestValueDateListHashIndex");
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeListHashIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateThree))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
    try (Stream<YTRID> stream =
        dateIndexTestValueDateTimeListHashIndex
            .getInternal()
            .getRids(database, new OCompositeKey("v1", dateFour))) {
      Assert.assertEquals(stream.findAny().orElse(null), dateDoc.getIdentity());
    }
  }
}
