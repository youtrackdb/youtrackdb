package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.collate.CaseInsensitiveCollate;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class BinaryComparatorCompareTest extends AbstractComparatorTest {

  @Test
  public void testInteger() {
    testCompareNumber(PropertyType.INTEGER, 10);
  }

  @Test
  public void testLong() {
    testCompareNumber(PropertyType.LONG, 10L);
  }

  @Test
  public void testShort() {
    testCompareNumber(PropertyType.SHORT, (short) 10);
  }

  @Test
  public void testByte() {
    testCompareNumber(PropertyType.BYTE, (byte) 10);
  }

  @Test
  public void testFloat() {
    testCompareNumber(PropertyType.FLOAT, 10f);
  }

  @Test
  public void testDouble() {
    testCompareNumber(PropertyType.DOUBLE, 10d);
  }

  @Test
  public void testDatetime() throws ParseException {
    testCompareNumber(PropertyType.DATETIME, 10L);

    final var format =
        new SimpleDateFormat(StorageConfiguration.DEFAULT_DATETIME_FORMAT);
    format.setTimeZone(DateHelper.getDatabaseTimeZone());

    var now1 = format.format(new Date());
    var now = format.parse(now1);

    Assert.assertEquals(
        0, comparator.compare(field(db, PropertyType.DATETIME, now),
            field(db, PropertyType.STRING, format.format(now))));
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.DATETIME, new Date(now.getTime() + 1)),
            field(db, PropertyType.STRING, format.format(now)))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.DATETIME, new Date(now.getTime() - 1)),
            field(db, PropertyType.STRING, format.format(now)))
            < 0);
  }

  @Test
  public void testBinary() {
    final var b1 = new byte[]{0, 1, 2, 3};
    final var b2 = new byte[]{0, 1, 2, 4};
    final var b3 = new byte[]{1, 1, 2, 4};

    Assert.assertEquals(
        "For values " + field(db, PropertyType.BINARY, b1) + " and " + field(db,
            PropertyType.BINARY, b1),
        0,
        comparator.compare(field(db, PropertyType.BINARY, b1), field(db, PropertyType.BINARY, b1)));
    Assert.assertFalse(
        comparator.compare(field(db, PropertyType.BINARY, b1), field(db, PropertyType.BINARY, b2))
            > 1);
    Assert.assertFalse(
        comparator.compare(field(db, PropertyType.BINARY, b1), field(db, PropertyType.BINARY, b3))
            > 1);
  }

  @Test
  public void testLinks() {
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.LINK, new RecordId(1, 2))));
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.LINK, new RecordId(2, 1)))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.LINK, new RecordId(0, 2)))
            > 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.STRING, new RecordId(1, 2).toString())));
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.STRING, new RecordId(0, 2).toString()))
            > 0);
  }

  @Test
  public void testString() {
    Assert.assertEquals(
        0, comparator.compare(field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test")));
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "test"))
            > 0);
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test2"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "te")) < 0);

    // DEF COLLATE
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test")));
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test2", new DefaultCollate()),
            field(db, PropertyType.STRING, "test"))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test2"))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "t", new DefaultCollate()),
            field(db, PropertyType.STRING, "te"))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test2", new DefaultCollate()),
            field(db, PropertyType.STRING, "test", new DefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test2", new DefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "t", new DefaultCollate()),
            field(db, PropertyType.STRING, "te", new DefaultCollate()))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "test", new DefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test2", new DefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "te", new DefaultCollate()))
            < 0);

    // CASE INSENSITIVE COLLATE
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test", new CaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "test", new CaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test2", new CaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "te", new CaseInsensitiveCollate()))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.STRING, "TEST"),
            field(db, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.STRING, "TE"),
            field(db, PropertyType.STRING, "te", new CaseInsensitiveCollate())));

    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "TEST", new CaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "TEST2", new CaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "tE", new CaseInsensitiveCollate()))
            < 0);
  }

  @Test
  public void testDecimal() {
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DECIMAL, new BigDecimal(10))));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DECIMAL, new BigDecimal(11))));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DECIMAL, new BigDecimal(9))));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.SHORT, (short) 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.SHORT, (short) 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)), field(db, PropertyType.SHORT,
                (short) 9)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.INTEGER, 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.INTEGER, 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.INTEGER, 9)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.LONG, 10L)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.LONG, 11L)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)), field(db, PropertyType.LONG, 9L)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.FLOAT, 10F)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.FLOAT, 11F)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.FLOAT, 9F)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DOUBLE, 10.0)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DOUBLE, 11.0)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DOUBLE, 9.0)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.BYTE, (byte) 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.BYTE, (byte) 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.BYTE, (byte) 9)));

    Assert.assertEquals(
        0,
        comparator.compare(field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.STRING, "10")));
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.STRING, "11"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.STRING, "9"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.DECIMAL, new BigDecimal(20)),
            field(db, PropertyType.STRING, "11"))
            > 0);
  }

  @Test
  public void testBoolean() {
    Assert.assertEquals(
        0, comparator.compare(field(db, PropertyType.BOOLEAN, true),
            field(db, PropertyType.BOOLEAN, true)));
    Assert.assertEquals(
        1, comparator.compare(field(db, PropertyType.BOOLEAN, true),
            field(db, PropertyType.BOOLEAN, false)));
    Assert.assertEquals(
        -1, comparator.compare(field(db, PropertyType.BOOLEAN, false),
            field(db, PropertyType.BOOLEAN, true)));

    Assert.assertEquals(
        0, comparator.compare(field(db, PropertyType.BOOLEAN, true),
            field(db, PropertyType.STRING, "true")));
    Assert.assertEquals(
        0, comparator.compare(field(db, PropertyType.BOOLEAN, false),
            field(db, PropertyType.STRING, "false")));
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.BOOLEAN, false),
            field(db, PropertyType.STRING, "true")) < 0);
    Assert.assertTrue(
        comparator.compare(field(db, PropertyType.BOOLEAN, true),
            field(db, PropertyType.STRING, "false")) > 0);
  }

  protected void testCompareNumber(PropertyType sourceType, Number value10AsSourceType) {
    var numberTypes =
        new PropertyType[]{
            PropertyType.BYTE,
            PropertyType.DOUBLE,
            PropertyType.FLOAT,
            PropertyType.SHORT,
            PropertyType.INTEGER,
            PropertyType.LONG,
            PropertyType.DATETIME
        };

    for (var t : numberTypes) {
      if (sourceType == PropertyType.DATETIME && t == PropertyType.BYTE)
      // SKIP TEST
      {
        continue;
      }

      testCompare(sourceType, t);
    }

    for (var t : numberTypes) {
      testCompare(t, sourceType);
    }

    // STRING
    if (sourceType != PropertyType.DATETIME) {
      Assert.assertEquals(
          0,
          comparator.compare(
              field(db, sourceType, value10AsSourceType),
              field(db, PropertyType.STRING, value10AsSourceType.toString())));
      Assert.assertTrue(
          comparator.compare(field(db, sourceType, value10AsSourceType),
              field(db, PropertyType.STRING, "9"))
              < 0);
      Assert.assertTrue(
          comparator.compare(field(db, sourceType, value10AsSourceType),
              field(db, PropertyType.STRING, "11"))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, sourceType, value10AsSourceType.intValue() * 2),
              field(db, PropertyType.STRING,
                  "11"))
              > 0);

      Assert.assertEquals(
          0,
          comparator.compare(
              field(db, PropertyType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType)));
      Assert.assertTrue(
          comparator.compare(
              field(db, PropertyType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() - 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, PropertyType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() + 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, PropertyType.STRING, "" + value10AsSourceType.intValue() * 2),
              field(db, sourceType, value10AsSourceType.intValue()))
              > 0);
    }
  }

  protected void testCompare(PropertyType sourceType, PropertyType destType) {
    testCompare(sourceType, destType, 10);
  }

  protected void testCompare(PropertyType sourceType, PropertyType destType, final Number value) {
    try {
      Assert.assertEquals(
          0,
          comparator.compare(field(db, sourceType, value), field(db, destType, value)));
      Assert.assertEquals(
          1,
          comparator.compare(field(db, sourceType, value),
              field(db, destType, value.intValue() - 1)));
      Assert.assertEquals(
          -1,
          comparator.compare(field(db, sourceType, value),
              field(db, destType, value.intValue() + 1)));
    } catch (AssertionError e) {
      System.out.println("ERROR: testCompare(" + sourceType + "," + destType + "," + value + ")");
      System.out.flush();
      throw e;
    }
  }
}
