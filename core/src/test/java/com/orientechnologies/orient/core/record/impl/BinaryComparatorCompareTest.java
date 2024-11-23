package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class BinaryComparatorCompareTest extends AbstractComparatorTest {

  @Test
  public void testInteger() {
    testCompareNumber(OType.INTEGER, 10);
  }

  @Test
  public void testLong() {
    testCompareNumber(OType.LONG, 10L);
  }

  @Test
  public void testShort() {
    testCompareNumber(OType.SHORT, (short) 10);
  }

  @Test
  public void testByte() {
    testCompareNumber(OType.BYTE, (byte) 10);
  }

  @Test
  public void testFloat() {
    testCompareNumber(OType.FLOAT, 10f);
  }

  @Test
  public void testDouble() {
    testCompareNumber(OType.DOUBLE, 10d);
  }

  @Test
  public void testDatetime() throws ParseException {
    testCompareNumber(OType.DATETIME, 10L);

    final SimpleDateFormat format =
        new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
    format.setTimeZone(ODateHelper.getDatabaseTimeZone());

    String now1 = format.format(new Date());
    Date now = format.parse(now1);

    Assert.assertEquals(
        0, comparator.compare(field(db, OType.DATETIME, now),
            field(db, OType.STRING, format.format(now))));
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.DATETIME, new Date(now.getTime() + 1)),
            field(db, OType.STRING, format.format(now)))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.DATETIME, new Date(now.getTime() - 1)),
            field(db, OType.STRING, format.format(now)))
            < 0);
  }

  @Test
  public void testBinary() {
    final byte[] b1 = new byte[]{0, 1, 2, 3};
    final byte[] b2 = new byte[]{0, 1, 2, 4};
    final byte[] b3 = new byte[]{1, 1, 2, 4};

    Assert.assertEquals(
        "For values " + field(db, OType.BINARY, b1) + " and " + field(db, OType.BINARY, b1),
        0,
        comparator.compare(field(db, OType.BINARY, b1), field(db, OType.BINARY, b1)));
    Assert.assertFalse(
        comparator.compare(field(db, OType.BINARY, b1), field(db, OType.BINARY, b2)) > 1);
    Assert.assertFalse(
        comparator.compare(field(db, OType.BINARY, b1), field(db, OType.BINARY, b3)) > 1);
  }

  @Test
  public void testLinks() {
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.LINK, new ORecordId(1, 2)),
            field(db, OType.LINK, new ORecordId(1, 2))));
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.LINK, new ORecordId(1, 2)), field(db, OType.LINK, new ORecordId(2, 1)))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.LINK, new ORecordId(1, 2)), field(db, OType.LINK, new ORecordId(0, 2)))
            > 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.LINK, new ORecordId(1, 2)),
            field(db, OType.STRING, new ORecordId(1, 2).toString())));
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.LINK, new ORecordId(1, 2)),
            field(db, OType.STRING, new ORecordId(0, 2).toString()))
            > 0);
  }

  @Test
  public void testString() {
    Assert.assertEquals(
        0, comparator.compare(field(db, OType.STRING, "test"), field(db, OType.STRING, "test")));
    Assert.assertTrue(
        comparator.compare(field(db, OType.STRING, "test2"), field(db, OType.STRING, "test")) > 0);
    Assert.assertTrue(
        comparator.compare(field(db, OType.STRING, "test"), field(db, OType.STRING, "test2")) < 0);
    Assert.assertTrue(
        comparator.compare(field(db, OType.STRING, "t"), field(db, OType.STRING, "te")) < 0);

    // DEF COLLATE
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.STRING, "test", new ODefaultCollate()),
            field(db, OType.STRING, "test")));
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test2", new ODefaultCollate()),
            field(db, OType.STRING, "test"))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test", new ODefaultCollate()),
            field(db, OType.STRING, "test2"))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "t", new ODefaultCollate()), field(db, OType.STRING, "te"))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.STRING, "test", new ODefaultCollate()),
            field(db, OType.STRING, "test", new ODefaultCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test2", new ODefaultCollate()),
            field(db, OType.STRING, "test", new ODefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test", new ODefaultCollate()),
            field(db, OType.STRING, "test2", new ODefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "t", new ODefaultCollate()),
            field(db, OType.STRING, "te", new ODefaultCollate()))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.STRING, "test"),
            field(db, OType.STRING, "test", new ODefaultCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test2"),
            field(db, OType.STRING, "test", new ODefaultCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test"),
            field(db, OType.STRING, "test2", new ODefaultCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "t"), field(db, OType.STRING, "te", new ODefaultCollate()))
            < 0);

    // CASE INSENSITIVE COLLATE
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.STRING, "test"),
            field(db, OType.STRING, "test", new OCaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test2"),
            field(db, OType.STRING, "test", new OCaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test"),
            field(db, OType.STRING, "test2", new OCaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "t"),
            field(db, OType.STRING, "te", new OCaseInsensitiveCollate()))
            < 0);

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.STRING, "test"),
            field(db, OType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.STRING, "TEST"),
            field(db, OType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.STRING, "TE"),
            field(db, OType.STRING, "te", new OCaseInsensitiveCollate())));

    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test2"),
            field(db, OType.STRING, "TEST", new OCaseInsensitiveCollate()))
            > 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "test"),
            field(db, OType.STRING, "TEST2", new OCaseInsensitiveCollate()))
            < 0);
    Assert.assertTrue(
        comparator.compare(
            field(db, OType.STRING, "t"),
            field(db, OType.STRING, "tE", new OCaseInsensitiveCollate()))
            < 0);
  }

  @Test
  public void testDecimal() {
    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.DECIMAL, new BigDecimal(10))));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.DECIMAL, new BigDecimal(11))));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.DECIMAL, new BigDecimal(9))));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.SHORT, (short) 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.SHORT, (short) 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)), field(db, OType.SHORT,
                (short) 9)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.INTEGER, 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.INTEGER, 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.INTEGER, 9)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)), field(db, OType.LONG, 10L)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)), field(db, OType.LONG, 11L)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)), field(db, OType.LONG, 9L)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.FLOAT, 10F)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.FLOAT, 11F)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)), field(db, OType.FLOAT, 9F)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.DOUBLE, 10.0)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.DOUBLE, 11.0)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.DOUBLE, 9.0)));

    Assert.assertEquals(
        0,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.BYTE, (byte) 10)));
    Assert.assertEquals(
        -1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.BYTE, (byte) 11)));
    Assert.assertEquals(
        1,
        comparator.compare(
            field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.BYTE, (byte) 9)));

    Assert.assertEquals(
        0,
        comparator.compare(field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.STRING, "10")));
    Assert.assertTrue(
        comparator.compare(field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.STRING, "11"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, OType.DECIMAL, new BigDecimal(10)),
            field(db, OType.STRING, "9"))
            < 0);
    Assert.assertTrue(
        comparator.compare(field(db, OType.DECIMAL, new BigDecimal(20)),
            field(db, OType.STRING, "11"))
            > 0);
  }

  @Test
  public void testBoolean() {
    Assert.assertEquals(
        0, comparator.compare(field(db, OType.BOOLEAN, true), field(db, OType.BOOLEAN, true)));
    Assert.assertEquals(
        1, comparator.compare(field(db, OType.BOOLEAN, true), field(db, OType.BOOLEAN, false)));
    Assert.assertEquals(
        -1, comparator.compare(field(db, OType.BOOLEAN, false), field(db, OType.BOOLEAN, true)));

    Assert.assertEquals(
        0, comparator.compare(field(db, OType.BOOLEAN, true), field(db, OType.STRING, "true")));
    Assert.assertEquals(
        0, comparator.compare(field(db, OType.BOOLEAN, false), field(db, OType.STRING, "false")));
    Assert.assertTrue(
        comparator.compare(field(db, OType.BOOLEAN, false), field(db, OType.STRING, "true")) < 0);
    Assert.assertTrue(
        comparator.compare(field(db, OType.BOOLEAN, true), field(db, OType.STRING, "false")) > 0);
  }

  protected void testCompareNumber(OType sourceType, Number value10AsSourceType) {
    OType[] numberTypes =
        new OType[]{
            OType.BYTE,
            OType.DOUBLE,
            OType.FLOAT,
            OType.SHORT,
            OType.INTEGER,
            OType.LONG,
            OType.DATETIME
        };

    for (OType t : numberTypes) {
      if (sourceType == OType.DATETIME && t == OType.BYTE)
      // SKIP TEST
      {
        continue;
      }

      testCompare(sourceType, t);
    }

    for (OType t : numberTypes) {
      testCompare(t, sourceType);
    }

    // STRING
    if (sourceType != OType.DATETIME) {
      Assert.assertEquals(
          0,
          comparator.compare(
              field(db, sourceType, value10AsSourceType),
              field(db, OType.STRING, value10AsSourceType.toString())));
      Assert.assertTrue(
          comparator.compare(field(db, sourceType, value10AsSourceType),
              field(db, OType.STRING, "9"))
              < 0);
      Assert.assertTrue(
          comparator.compare(field(db, sourceType, value10AsSourceType),
              field(db, OType.STRING, "11"))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, sourceType, value10AsSourceType.intValue() * 2), field(db, OType.STRING,
                  "11"))
              > 0);

      Assert.assertEquals(
          0,
          comparator.compare(
              field(db, OType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType)));
      Assert.assertTrue(
          comparator.compare(
              field(db, OType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() - 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, OType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() + 1))
              < 0);
      Assert.assertTrue(
          comparator.compare(
              field(db, OType.STRING, "" + value10AsSourceType.intValue() * 2),
              field(db, sourceType, value10AsSourceType.intValue()))
              > 0);
    }
  }

  protected void testCompare(OType sourceType, OType destType) {
    testCompare(sourceType, destType, 10);
  }

  protected void testCompare(OType sourceType, OType destType, final Number value) {
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
