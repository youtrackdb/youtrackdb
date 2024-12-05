package com.orientechnologies.core.record.impl;

import com.orientechnologies.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.core.collate.ODefaultCollate;
import com.orientechnologies.core.config.OStorageConfiguration;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.serialization.serializer.record.binary.OBinaryField;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BinaryComparatorEqualsTest extends AbstractComparatorTest {

  @Before
  public void before() {
    ODatabaseRecordThreadLocal.instance().remove();
  }

  @Test
  public void testInteger() {
    testEquals(YTType.INTEGER, 10);
  }

  @Test
  public void testLong() {
    testEquals(YTType.LONG, 10L);
  }

  @Test
  public void testShort() {
    testEquals(YTType.SHORT, (short) 10);
  }

  @Test
  public void testByte() {
    testEquals(YTType.BYTE, (byte) 10);
  }

  @Test
  public void testFloat() {
    testEquals(YTType.FLOAT, 10f);
  }

  @Test
  public void testDouble() {
    testEquals(YTType.DOUBLE, 10d);
  }

  @Test
  public void testDatetime() throws ParseException {
    testEquals(YTType.DATETIME, 10L);

    final SimpleDateFormat format =
        new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);

    String now1 = format.format(new Date());
    Date now = format.parse(now1);

    Assert.assertTrue(
        comparator.isEqual(field(db, YTType.DATETIME, now),
            field(db, YTType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.DATETIME, new Date(now.getTime() + 1)),
            field(db, YTType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.DATETIME, new Date(now.getTime() - 1)),
            field(db, YTType.STRING, format.format(now))));
  }

  @Test
  public void testBinary() throws ParseException {
    final byte[] b1 = new byte[]{0, 1, 2, 3};
    final byte[] b2 = new byte[]{0, 1, 2, 4};
    final byte[] b3 = new byte[]{1, 1, 2, 4};

    Assert.assertTrue(
        comparator.isEqual(field(db, YTType.BINARY, b1), field(db, YTType.BINARY, b1)));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.BINARY, b1), field(db, YTType.BINARY, b2)));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.BINARY, b1), field(db, YTType.BINARY, b3)));
  }

  @Test
  public void testLinks() throws ParseException {
    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.LINK, new YTRecordId(1, 2))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.LINK, new YTRecordId(2, 1))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.LINK, new YTRecordId(0, 2))));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.STRING, new YTRecordId(1, 2).toString())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.LINK, new YTRecordId(1, 2)),
            field(db, YTType.STRING, new YTRecordId(0, 2).toString())));
  }

  @Test
  public void testString() {
    Assert.assertTrue(comparator.isEqual(field(db, YTType.STRING, "test"), field(db, YTType.STRING,
        "test")));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.STRING, "test2"), field(db, YTType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.STRING, "test"), field(db, YTType.STRING, "test2")));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.STRING, "t"), field(db, YTType.STRING, "te")));

    // DEF COLLATE
    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test2", new ODefaultCollate()),
            field(db, YTType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test2")));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "t", new ODefaultCollate()), field(db, YTType.STRING, "te")));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test2", new ODefaultCollate()),
            field(db, YTType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test", new ODefaultCollate()),
            field(db, YTType.STRING, "test2", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "t", new ODefaultCollate()),
            field(db, YTType.STRING, "te", new ODefaultCollate())));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test2"),
            field(db, YTType.STRING, "test", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test2", new ODefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "t"), field(db, YTType.STRING, "te", new ODefaultCollate())));

    // CASE INSENSITIVE COLLATE
    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test2"),
            field(db, YTType.STRING, "test", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "test2", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "t"),
            field(db, YTType.STRING, "te", new OCaseInsensitiveCollate())));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.STRING, "TEST"),
            field(db, YTType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.STRING, "TE"),
            field(db, YTType.STRING, "te", new OCaseInsensitiveCollate())));

    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test2"),
            field(db, YTType.STRING, "TEST", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "test"),
            field(db, YTType.STRING, "TEST2", new OCaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.STRING, "t"),
            field(db, YTType.STRING, "tE", new OCaseInsensitiveCollate())));
  }

  @Test
  public void testDecimal() {
    Assert.assertTrue(
        comparator.isEqual(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DECIMAL, new BigDecimal(10))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DECIMAL, new BigDecimal(11))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, YTType.DECIMAL, new BigDecimal(10)),
            field(db, YTType.DECIMAL, new BigDecimal(9))));
  }

  @Test
  public void testBoolean() {
    Assert.assertTrue(
        comparator.isEqual(field(db, YTType.BOOLEAN, true), field(db, YTType.BOOLEAN, true)));
    Assert.assertFalse(comparator.isEqual(field(db, YTType.BOOLEAN, true), field(db, YTType.BOOLEAN,
        false)));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.BOOLEAN, false), field(db, YTType.BOOLEAN,
        true)));

    Assert.assertTrue(
        comparator.isEqual(field(db, YTType.BOOLEAN, true), field(db, YTType.STRING, "true")));
    Assert.assertTrue(
        comparator.isEqual(field(db, YTType.BOOLEAN, false), field(db, YTType.STRING, "false")));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.BOOLEAN, false), field(db, YTType.STRING, "true")));
    Assert.assertFalse(
        comparator.isEqual(field(db, YTType.BOOLEAN, true), field(db, YTType.STRING, "false")));
  }

  @Test
  public void testBinaryFieldCopy() {
    final OBinaryField f = field(db, YTType.BYTE, 10, new OCaseInsensitiveCollate()).copy();
    Assert.assertEquals(f.type, YTType.BYTE);
    Assert.assertNotNull(f.bytes);
    Assert.assertEquals(f.collate.getName(), OCaseInsensitiveCollate.NAME);
  }

  @Test
  public void testBinaryComparable() {
    for (YTType t : YTType.values()) {
      switch (t) {
        case INTEGER:
        case LONG:
        case DATETIME:
        case SHORT:
        case STRING:
        case DOUBLE:
        case FLOAT:
        case BYTE:
        case BOOLEAN:
        case DATE:
        case BINARY:
        case LINK:
        case DECIMAL:
          Assert.assertTrue(comparator.isBinaryComparable(t));
          break;

        default:
          Assert.assertFalse(comparator.isBinaryComparable(t));
      }
    }
  }

  protected void testEquals(YTType sourceType, Number value10AsSourceType) {
    YTType[] numberTypes =
        new YTType[]{YTType.BYTE, YTType.DOUBLE, YTType.FLOAT, YTType.SHORT, YTType.INTEGER,
            YTType.LONG};

    for (YTType t : numberTypes) {
      if (sourceType == YTType.DATETIME && t == YTType.BYTE)
      // SKIP TEST
      {
        continue;
      }

      testEquals(db, sourceType, t);
    }

    for (YTType t : numberTypes) {
      testEquals(db, t, sourceType);
    }

    if (sourceType != YTType.DATETIME) {
      // STRING
      Assert.assertTrue(
          comparator.isEqual(
              field(db, sourceType, value10AsSourceType),
              field(db, YTType.STRING, value10AsSourceType.toString())));
      Assert.assertFalse(
          comparator.isEqual(field(db, sourceType, value10AsSourceType),
              field(db, YTType.STRING, "9")));
      Assert.assertFalse(
          comparator.isEqual(field(db, sourceType, value10AsSourceType),
              field(db, YTType.STRING, "11")));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, sourceType, value10AsSourceType.intValue() * 2),
              field(db, YTType.STRING, "11")));

      Assert.assertTrue(
          comparator.isEqual(
              field(db, YTType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType)));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, YTType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() - 1)));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, YTType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() + 1)));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, YTType.STRING, "" + value10AsSourceType.intValue() * 2),
              field(db, sourceType, value10AsSourceType.intValue())));
    }
  }

  protected void testEquals(YTDatabaseSessionInternal db, YTType sourceType, YTType destType) {
    try {
      Assert.assertTrue(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 10)));
      Assert.assertFalse(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 9)));
      Assert.assertFalse(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 11)));
    } catch (AssertionError e) {
      System.out.println("ERROR: testEquals(" + sourceType + "," + destType + ")");
      System.out.flush();
      throw e;
    }
  }
}
