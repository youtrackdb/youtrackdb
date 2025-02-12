package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.collate.CaseInsensitiveCollate;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class BinaryComparatorEqualsTest extends AbstractComparatorTest {

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
        comparator.isEqual(session,
            field(session, PropertyType.DATETIME, now),
            field(session, PropertyType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.DATETIME, new Date(now.getTime() + 1)),
            field(session, PropertyType.STRING, format.format(now))));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.DATETIME, new Date(now.getTime() - 1)),
            field(session, PropertyType.STRING, format.format(now))));
  }

  @Test
  public void testBinary() throws ParseException {
    final var b1 = new byte[]{0, 1, 2, 3};
    final var b2 = new byte[]{0, 1, 2, 4};
    final var b3 = new byte[]{1, 1, 2, 4};

    Assert.assertTrue(
        comparator.isEqual(
            session, field(session, PropertyType.BINARY, b1),
            field(session, PropertyType.BINARY, b1)));
    Assert.assertFalse(
        comparator.isEqual(
            session, field(session, PropertyType.BINARY, b1),
            field(session, PropertyType.BINARY, b2)));
    Assert.assertFalse(
        comparator.isEqual(
            session, field(session, PropertyType.BINARY, b1),
            field(session, PropertyType.BINARY, b3)));
  }

  @Test
  public void testLinks() throws ParseException {
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.LINK, new RecordId(1, 2)),
            field(session, PropertyType.LINK, new RecordId(1, 2))));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.LINK, new RecordId(1, 2)),
            field(session, PropertyType.LINK, new RecordId(2, 1))));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.LINK, new RecordId(1, 2)),
            field(session, PropertyType.LINK, new RecordId(0, 2))));

    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.LINK, new RecordId(1, 2)),
            field(session, PropertyType.STRING, new RecordId(1, 2).toString())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.LINK, new RecordId(1, 2)),
            field(session, PropertyType.STRING, new RecordId(0, 2).toString())));
  }

  @Test
  public void testString() {
    Assert.assertTrue(
        comparator.isEqual(
            session, field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING,
        "test")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test2"),
            field(session, PropertyType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING, "test2")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "t"), field(session, PropertyType.STRING, "te")));

    // DEF COLLATE
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test", new DefaultCollate()),
            field(session, PropertyType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test2", new DefaultCollate()),
            field(session, PropertyType.STRING, "test")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test", new DefaultCollate()),
            field(session, PropertyType.STRING, "test2")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "t", new DefaultCollate()),
            field(session, PropertyType.STRING, "te")));

    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test", new DefaultCollate()),
            field(session, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test2", new DefaultCollate()),
            field(session, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test", new DefaultCollate()),
            field(session, PropertyType.STRING, "test2", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "t", new DefaultCollate()),
            field(session, PropertyType.STRING, "te", new DefaultCollate())));

    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test2"),
            field(session, PropertyType.STRING, "test", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING, "test2", new DefaultCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "t"),
            field(session, PropertyType.STRING, "te", new DefaultCollate())));

    // CASE INSENSITIVE COLLATE
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING, "test", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test2"),
            field(session, PropertyType.STRING, "test", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING, "test2", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "t"),
            field(session, PropertyType.STRING, "te", new CaseInsensitiveCollate())));

    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "TEST"),
            field(session, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "TE"),
            field(session, PropertyType.STRING, "te", new CaseInsensitiveCollate())));

    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test2"),
            field(session, PropertyType.STRING, "TEST", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "test"),
            field(session, PropertyType.STRING, "TEST2", new CaseInsensitiveCollate())));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.STRING, "t"),
            field(session, PropertyType.STRING, "tE", new CaseInsensitiveCollate())));
  }

  @Test
  public void testDecimal() {
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.DECIMAL, new BigDecimal(10)),
            field(session, PropertyType.DECIMAL, new BigDecimal(10))));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.DECIMAL, new BigDecimal(10)),
            field(session, PropertyType.DECIMAL, new BigDecimal(11))));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.DECIMAL, new BigDecimal(10)),
            field(session, PropertyType.DECIMAL, new BigDecimal(9))));
  }

  @Test
  public void testBoolean() {
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.BOOLEAN, true),
            field(session, PropertyType.BOOLEAN, true)));
    Assert.assertFalse(
        comparator.isEqual(
            session, field(session, PropertyType.BOOLEAN, true),
            field(session, PropertyType.BOOLEAN,
        false)));
    Assert.assertFalse(
        comparator.isEqual(
            session, field(session, PropertyType.BOOLEAN, false),
            field(session, PropertyType.BOOLEAN,
        true)));

    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.BOOLEAN, true),
            field(session, PropertyType.STRING, "true")));
    Assert.assertTrue(
        comparator.isEqual(session,
            field(session, PropertyType.BOOLEAN, false),
            field(session, PropertyType.STRING, "false")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.BOOLEAN, false),
            field(session, PropertyType.STRING, "true")));
    Assert.assertFalse(
        comparator.isEqual(session,
            field(session, PropertyType.BOOLEAN, true),
            field(session, PropertyType.STRING, "false")));
  }

  @Test
  public void testBinaryFieldCopy() {
    final var f = field(session, PropertyType.BYTE, 10, new CaseInsensitiveCollate()).copy();
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

      testEquals(session, sourceType, t);
    }

    for (var t : numberTypes) {
      testEquals(session, t, sourceType);
    }

    if (sourceType != PropertyType.DATETIME) {
      // STRING
      Assert.assertTrue(
          comparator.isEqual(session,
              field(session, sourceType, value10AsSourceType),
              field(session, PropertyType.STRING, value10AsSourceType.toString())));
      Assert.assertFalse(
          comparator.isEqual(session,
              field(session, sourceType, value10AsSourceType),
              field(session, PropertyType.STRING, "9")));
      Assert.assertFalse(
          comparator.isEqual(session,
              field(session, sourceType, value10AsSourceType),
              field(session, PropertyType.STRING, "11")));
      Assert.assertFalse(
          comparator.isEqual(session,
              field(session, sourceType, value10AsSourceType.intValue() * 2),
              field(session, PropertyType.STRING, "11")));

      Assert.assertTrue(
          comparator.isEqual(session,
              field(session, PropertyType.STRING, value10AsSourceType.toString()),
              field(session, sourceType, value10AsSourceType)));
      Assert.assertFalse(
          comparator.isEqual(session,
              field(session, PropertyType.STRING, value10AsSourceType.toString()),
              field(session, sourceType, value10AsSourceType.intValue() - 1)));
      Assert.assertFalse(
          comparator.isEqual(session,
              field(session, PropertyType.STRING, value10AsSourceType.toString()),
              field(session, sourceType, value10AsSourceType.intValue() + 1)));
      Assert.assertFalse(
          comparator.isEqual(session,
              field(session, PropertyType.STRING, "" + value10AsSourceType.intValue() * 2),
              field(session, sourceType, value10AsSourceType.intValue())));
    }
  }

  protected void testEquals(DatabaseSessionInternal db, PropertyType sourceType,
      PropertyType destType) {
    try {
      Assert.assertTrue(comparator.isEqual(db, field(db, sourceType, 10), field(db, destType, 10)));
      Assert.assertFalse(comparator.isEqual(db, field(db, sourceType, 10), field(db, destType, 9)));
      Assert.assertFalse(
          comparator.isEqual(db, field(db, sourceType, 10), field(db, destType, 11)));
    } catch (AssertionError e) {
      System.out.println("ERROR: testEquals(" + sourceType + "," + destType + ")");
      System.out.flush();
      throw e;
    }
  }
}
