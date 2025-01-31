package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.collate.CaseInsensitiveCollate;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
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
    DatabaseRecordThreadLocal.instance().remove();
  }

  @Test
  public void testInteger() {
    testEquals(PropertyType.INTEGER, 10);
  }

  @Test
  public void testLong() {
    testEquals(PropertyType.LONG, 10L);
  }

  @Test
  public void testShort() {
    testEquals(PropertyType.SHORT, (short) 10);
  }

  @Test
  public void testByte() {
    testEquals(PropertyType.BYTE, (byte) 10);
  }

  @Test
  public void testFloat() {
    testEquals(PropertyType.FLOAT, 10f);
  }

  @Test
  public void testDouble() {
    testEquals(PropertyType.DOUBLE, 10d);
  }

  @Test
  public void testDatetime() throws ParseException {
    testEquals(PropertyType.DATETIME, 10L);

    final var format =
        new SimpleDateFormat(StorageConfiguration.DEFAULT_DATETIME_FORMAT);

    var now1 = format.format(new Date());
    var now = format.parse(now1);

    Assert.assertTrue(
        comparator.isEqual(field(db, PropertyType.DATETIME, now),
            field(db, PropertyType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.DATETIME, new Date(now.getTime() + 1)),
            field(db, PropertyType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.DATETIME, new Date(now.getTime() - 1)),
            field(db, PropertyType.STRING, format.format(now))));
  }

  @Test
  public void testBinary() throws ParseException {
    final var b1 = new byte[]{0, 1, 2, 3};
    final var b2 = new byte[]{0, 1, 2, 4};
    final var b3 = new byte[]{1, 1, 2, 4};

    Assert.assertTrue(
        comparator.isEqual(field(db, PropertyType.BINARY, b1), field(db, PropertyType.BINARY, b1)));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.BINARY, b1), field(db, PropertyType.BINARY, b2)));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.BINARY, b1), field(db, PropertyType.BINARY, b3)));
  }

  @Test
  public void testLinks() throws ParseException {
    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.LINK, new RecordId(1, 2))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.LINK, new RecordId(2, 1))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.LINK, new RecordId(0, 2))));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.STRING, new RecordId(1, 2).toString())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.LINK, new RecordId(1, 2)),
            field(db, PropertyType.STRING, new RecordId(0, 2).toString())));
  }

  @Test
  public void testString() {
    Assert.assertTrue(
        comparator.isEqual(field(db, PropertyType.STRING, "test"), field(db, PropertyType.STRING,
        "test")));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test2")));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "te")));

    // DEF COLLATE
    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test2", new DefaultCollate()),
            field(db, PropertyType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test2")));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "t", new DefaultCollate()),
            field(db, PropertyType.STRING, "te")));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test2", new DefaultCollate()),
            field(db, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test", new DefaultCollate()),
            field(db, PropertyType.STRING, "test2", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "t", new DefaultCollate()),
            field(db, PropertyType.STRING, "te", new DefaultCollate())));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test2", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "te", new DefaultCollate())));

    // CASE INSENSITIVE COLLATE
    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "test", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "test2", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "te", new CaseInsensitiveCollate())));

    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.STRING, "TEST"),
            field(db, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.STRING, "TE"),
            field(db, PropertyType.STRING, "te", new CaseInsensitiveCollate())));

    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test2"),
            field(db, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "test"),
            field(db, PropertyType.STRING, "TEST2", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.STRING, "t"),
            field(db, PropertyType.STRING, "tE", new CaseInsensitiveCollate())));
  }

  @Test
  public void testDecimal() {
    Assert.assertTrue(
        comparator.isEqual(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DECIMAL, new BigDecimal(10))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DECIMAL, new BigDecimal(11))));
    Assert.assertFalse(
        comparator.isEqual(
            field(db, PropertyType.DECIMAL, new BigDecimal(10)),
            field(db, PropertyType.DECIMAL, new BigDecimal(9))));
  }

  @Test
  public void testBoolean() {
    Assert.assertTrue(
        comparator.isEqual(field(db, PropertyType.BOOLEAN, true),
            field(db, PropertyType.BOOLEAN, true)));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.BOOLEAN, true), field(db, PropertyType.BOOLEAN,
        false)));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.BOOLEAN, false), field(db, PropertyType.BOOLEAN,
        true)));

    Assert.assertTrue(
        comparator.isEqual(field(db, PropertyType.BOOLEAN, true),
            field(db, PropertyType.STRING, "true")));
    Assert.assertTrue(
        comparator.isEqual(field(db, PropertyType.BOOLEAN, false),
            field(db, PropertyType.STRING, "false")));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.BOOLEAN, false),
            field(db, PropertyType.STRING, "true")));
    Assert.assertFalse(
        comparator.isEqual(field(db, PropertyType.BOOLEAN, true),
            field(db, PropertyType.STRING, "false")));
  }

  @Test
  public void testBinaryFieldCopy() {
    final var f = field(db, PropertyType.BYTE, 10, new CaseInsensitiveCollate()).copy();
    Assert.assertEquals(f.type, PropertyType.BYTE);
    Assert.assertNotNull(f.bytes);
    Assert.assertEquals(f.collate.getName(), CaseInsensitiveCollate.NAME);
  }

  @Test
  public void testBinaryComparable() {
    for (var t : PropertyType.values()) {
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

  protected void testEquals(PropertyType sourceType, Number value10AsSourceType) {
    var numberTypes =
        new PropertyType[]{PropertyType.BYTE, PropertyType.DOUBLE, PropertyType.FLOAT,
            PropertyType.SHORT, PropertyType.INTEGER,
            PropertyType.LONG};

    for (var t : numberTypes) {
      if (sourceType == PropertyType.DATETIME && t == PropertyType.BYTE)
      // SKIP TEST
      {
        continue;
      }

      testEquals(db, sourceType, t);
    }

    for (var t : numberTypes) {
      testEquals(db, t, sourceType);
    }

    if (sourceType != PropertyType.DATETIME) {
      // STRING
      Assert.assertTrue(
          comparator.isEqual(
              field(db, sourceType, value10AsSourceType),
              field(db, PropertyType.STRING, value10AsSourceType.toString())));
      Assert.assertFalse(
          comparator.isEqual(field(db, sourceType, value10AsSourceType),
              field(db, PropertyType.STRING, "9")));
      Assert.assertFalse(
          comparator.isEqual(field(db, sourceType, value10AsSourceType),
              field(db, PropertyType.STRING, "11")));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, sourceType, value10AsSourceType.intValue() * 2),
              field(db, PropertyType.STRING, "11")));

      Assert.assertTrue(
          comparator.isEqual(
              field(db, PropertyType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType)));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, PropertyType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() - 1)));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, PropertyType.STRING, value10AsSourceType.toString()),
              field(db, sourceType, value10AsSourceType.intValue() + 1)));
      Assert.assertFalse(
          comparator.isEqual(
              field(db, PropertyType.STRING, "" + value10AsSourceType.intValue() * 2),
              field(db, sourceType, value10AsSourceType.intValue())));
    }
  }

  protected void testEquals(DatabaseSessionInternal db, PropertyType sourceType,
      PropertyType destType) {
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
