package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.collate.OCaseInsensitiveCollate;
import com.jetbrains.youtrack.db.internal.core.collate.ODefaultCollate;
import com.jetbrains.youtrack.db.internal.core.config.OStorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.util.ODateHelper;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class BinaryComparatorCompareTest extends AbstractComparatorTest {

  @Test
  public void testInteger() {
    testCompareNumber(YTType.INTEGER, 10);
  }

  @Test
  public void testLong() {
    testCompareNumber(YTType.LONG, 10L);
  }

  @Test
  public void testShort() {
    testCompareNumber(YTType.SHORT, (short) 10);
  }

  @Test
  public void testByte() {
    testCompareNumber(YTType.BYTE, (byte) 10);
  }

  @Test
  public void testFloat() {
    testCompareNumber(YTType.FLOAT, 10f);
  }

  @Test
  public void testDouble() {
    testCompareNumber(YTType.DOUBLE, 10d);
  }

  @Test
  public void testDatetime() throws ParseException {
    testCompareNumber(YTType.DATETIME, 10L);

    final SimpleDateFormat format =
        new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
    format.setTimeZone(ODateHelper.getDatabaseTimeZone());

    String now1 = format.format(new Date());
    Date now = format.parse(now1);

    Assert.assertEquals(
        0, comparator.compare(field(db, YTType.DATETIME, now),
            field(db, YTType.STRING, format.format(now))));
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.DATETIME, new Date(now.getTime() + 1)),
            field(db, YTType.STRING, format.format(now)))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.DATETIME, new Date(now.getTime() - 1)),
            field(db, YTType.STRING, format.format(now)))
            < 0);
  }

  @Test
  public void testBinary() {
    final byte[] b1 = new byte[]{0, 1, 2, 3};
    final byte[] b2 = new byte[]{0, 1, 2, 4};
    final byte[] b3 = new byte[]{1, 1, 2, 4};

    Assert.assertEquals(
        "For values " + field(db, YTType.BINARY, b1) + " and " + field(db, YTType.BINARY, b1),
        0,
        comparator.compare(field(db, YTType.BINARY, b1), field(db, YTType.BINARY, b1)));
    Assert.assertFalse(
        comparator.compare(field(db, YTType.BINARY, b1), field(db, YTType.BINARY, b2)) > 1);
    Assert.assertFalse(
        comparator.compare(field(db, YTType.BINARY, b1), field(db, YTType.BINARY, b3)) > 1);
  }

  @Test
  public void testLinks() {
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.LINK, new YTRecordId(1, 2))));
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.LINK, new YTRecordId(2, 1)))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.LINK, new YTRecordId(0, 2)))
            > 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.STRING, new YTRecordId(1, 2).toString())));
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.STRING, new YTRecordId(0, 2).toString()))
            > 0);
  }

  @Test
  public void testString() {
    Assert.assertEquals(
        0, comparator.compare(field(db, YTType.STRING, "test"), field(db, YTType.STRING, "test")));
    Assert.assertTrue(
        comparator.compare(field(db, YTType.STRING, "test2"), field(db, YTType.STRING, "test"))
            > 0);
    Assert.assertTrue(
        comparator.compare(field(db, YTType.STRING, "test"), field(db, YTType.STRING, "test2"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, YTType.STRING, "t"), field(db, YTType.STRING, "te")) < 0);

    // DEF COLLATE
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test")));
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test2", new ODefaultCollate()),
            field(db, YTType.STRING, "test"))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test2"))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "t", new ODefaultCollate()), field(db, YTType.STRING, "te"))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test", new ODefaultCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test2", new ODefaultCollate()),
            field(db, YTType.STRING, "test", new ODefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test2", new ODefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "t", new ODefaultCollate()),
            field(db, YTType.STRING, "te", new ODefaultCollate()))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test", new ODefaultCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test2"),
            field(db, YTType.STRING, "test", new ODefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test2", new ODefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "t"), field(db, YTType.STRING, "te", new ODefaultCollate()))
            < 0);

    // CASE INSENSITIVE COLLATE
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test", new OCaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test2"),
            field(db, YTType.STRING, "test", new OCaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test2", new OCaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "t"),
            field(db, YTType.STRING, "te", new OCaseInsensitiveCollate()))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.STRING, "TEST"),
            field(db, YTType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.STRING, "TE"),
            field(db, YTType.STRING, "te", new OCaseInsensitiveCollate())));

    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test2"),
            field(db, YTType.STRING, "TEST", new OCaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "TEST2", new OCaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, YTType.STRING, "t"),
            field(db, YTType.STRING, "tE", new OCaseInsensitiveCollate()))
            < 0);
  }

  @Test
  public void testDecimal() {
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DECIMAL, new BigDecimal(10))));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DECIMAL, new BigDecimal(11))));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DECIMAL, new BigDecimal(9))));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.SHORT, (short) 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.SHORT, (short) 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)), field(db, YTType.SHORT,
                (short) 9)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.INTEGER, 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.INTEGER, 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.INTEGER, 9)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)), field(db, YTType.LONG, 10L)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)), field(db, YTType.LONG, 11L)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)), field(db, YTType.LONG, 9L)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.FLOAT, 10F)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.FLOAT, 11F)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)), field(db, YTType.FLOAT, 9F)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DOUBLE, 10.0)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DOUBLE, 11.0)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DOUBLE, 9.0)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.BYTE, (byte) 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.BYTE, (byte) 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.BYTE, (byte) 9)));

    Assert.assertEquals(
        0,
        comparator.compare(field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.STRING, "10")));
    Assert.assertTrue(
        comparator.compare(field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.STRING, "11"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.STRING, "9"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, YTType.DECIMAL, new BigDecimal(20)),
            field(db, YTType.STRING, "11"))
            > 0);
  }

  @Test
  public void testBoolean() {
    Assert.assertEquals(
        0, comparator.compare(field(db, YTType.BOOLEAN, true), field(db, YTType.BOOLEAN, true)));
    Assert.assertEquals(
        1, comparator.compare(field(db, YTType.BOOLEAN, true), field(db, YTType.BOOLEAN, false)));
    Assert.assertEquals(
        -1, comparator.compare(field(db, YTType.BOOLEAN, false), field(db, YTType.BOOLEAN, true)));

    Assert.assertEquals(
        0, comparator.compare(field(db, YTType.BOOLEAN, true), field(db, YTType.STRING, "true")));
    Assert.assertEquals(
        0, comparator.compare(field(db, YTType.BOOLEAN, false), field(db, YTType.STRING, "false")));
    Assert.assertTrue(
        comparator.compare(field(db, YTType.BOOLEAN, false), field(db, YTType.STRING, "true")) < 0);
    Assert.assertTrue(
        comparator.compare(field(db, YTType.BOOLEAN, true), field(db, YTType.STRING, "false")) > 0);
  }

  protected void testCompareNumber(YTType sourceType, Number value10AsSourceType) {
    YTType[] numberTypes =
        new YTType[]{
            YTType.BYTE,
            YTType.DOUBLE,
            YTType.FLOAT,
            YTType.SHORT,
            YTType.INTEGER,
            YTType.LONG,
            YTType.DATETIME
        };

    for (YTType t : numberTypes) {
      if (sourceType == YTType.DATETIME && t == YTType.BYTE)
      // SKIP TEST
      {
        continue;
      }

      testCompare(sourceType, t);
    }

    for (YTType t : numberTypes) {
      testCompare(t, sourceType);
    }

    // STRING
    if (sourceType != YTType.DATETIME) {
      Assert.assertEquals(
          0,
          comparator.compare(
              field(db, sourceType, value10AsSourceType),
              field(db, YTType.STRING, value10AsSourceType.toString())));
      Assert.assertTrue(
          comparator.compare(field(db, sourceType, value10AsSourceType),
              field(db, YTType.STRING, "9"))
              < 0);
      Assert.assertTrue(
          comparator.compare(field(db, sourceType, value10AsSourceType),
              field(db, YTType.STRING, "11"))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, sourceType, value10AsSourceType.intValue() * 2), field(db, YTType.STRING,
                  "11"))
              > 0);

      Assert.assertEquals(
          0,
          comparator.compare(
              field(db, YTType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType)));
      Assert.assertTrue(
          comparator.compare(
              field(db, YTType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() - 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, YTType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() + 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, YTType.STRING, "" + value10AsSourceType.intValue() * 2),
              field(db, sourceType, value10AsSourceType.intValue()))
              > 0);
    }
  }

  protected void testCompare(YTType sourceType, YTType destType) {
    testCompare(sourceType, destType, 10);
  }

  protected void testCompare(YTType sourceType, YTType destType, final Number value) {
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
