package com.jetbrains.youtrack.db.internal.core.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.index.CompositeKeySerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetworkV37;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALChanges;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.wal.WALPageChangesPortion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;

public class CompositeKeyTest extends DbTestBase {

  @Test
  public void testEqualSameKeys() {
    final CompositeKey compositeKey = new CompositeKey();

    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final CompositeKey anotherCompositeKey = new CompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(compositeKey, anotherCompositeKey);
    assertEquals(compositeKey.hashCode(), anotherCompositeKey.hashCode());
  }

  @Test
  public void testEqualNotSameKeys() {
    final CompositeKey compositeKey = new CompositeKey();

    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final CompositeKey anotherCompositeKey = new CompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");
    anotherCompositeKey.addKey("c");

    assertNotEquals(compositeKey, anotherCompositeKey);
  }

  @Test
  public void testEqualNull() {
    final CompositeKey compositeKey = new CompositeKey();
    assertNotEquals(null, compositeKey);
  }

  @Test
  public void testEqualSame() {
    final CompositeKey compositeKey = new CompositeKey();
    //noinspection EqualsWithItself
    assertEquals(compositeKey, compositeKey);
  }

  @Test
  public void testEqualDiffClass() {
    final CompositeKey compositeKey = new CompositeKey();
    assertNotEquals("1", compositeKey);
  }

  @Test
  public void testAddKeyComparable() {
    final CompositeKey compositeKey = new CompositeKey();

    compositeKey.addKey("a");

    assertEquals(1, compositeKey.getKeys().size());
    assertTrue(compositeKey.getKeys().contains("a"));
  }

  @Test
  public void testAddKeyComposite() {
    final CompositeKey compositeKey = new CompositeKey();

    compositeKey.addKey("a");

    final CompositeKey compositeKeyToAdd = new CompositeKey();
    compositeKeyToAdd.addKey("a");
    compositeKeyToAdd.addKey("b");

    compositeKey.addKey(compositeKeyToAdd);

    assertEquals(3, compositeKey.getKeys().size());
    assertTrue(compositeKey.getKeys().contains("a"));
    assertTrue(compositeKey.getKeys().contains("b"));
  }

  @Test
  public void testCompareToSame() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final CompositeKey anotherCompositeKey = new CompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(0, compositeKey.compareTo(anotherCompositeKey));
  }

  @Test
  public void testCompareToPartiallyOneCase() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final CompositeKey anotherCompositeKey = new CompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");
    anotherCompositeKey.addKey("c");

    assertEquals(0, compositeKey.compareTo(anotherCompositeKey));
  }

  @Test
  public void testCompareToPartiallySecondCase() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");
    compositeKey.addKey("c");

    final CompositeKey anotherCompositeKey = new CompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(0, compositeKey.compareTo(anotherCompositeKey));
  }

  @Test
  public void testCompareToGT() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey("b");

    final CompositeKey anotherCompositeKey = new CompositeKey();
    anotherCompositeKey.addKey("a");
    anotherCompositeKey.addKey("b");

    assertEquals(1, compositeKey.compareTo(anotherCompositeKey));
  }

  @Test
  public void testCompareToLT() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey("a");
    compositeKey.addKey("b");

    final CompositeKey anotherCompositeKey = new CompositeKey();

    anotherCompositeKey.addKey("b");

    assertEquals(-1, compositeKey.compareTo(anotherCompositeKey));
  }

  @Test
  public void testCompareStringsToLT() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey("name4");
    final CompositeKey anotherCompositeKey = new CompositeKey();
    anotherCompositeKey.addKey("name5");
    assertEquals(-1, compositeKey.compareTo(anotherCompositeKey));
  }

  @Test
  public void testCompareToSymmetryOne() {
    final CompositeKey compositeKeyOne = new CompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(2);

    final CompositeKey compositeKeyTwo = new CompositeKey();
    compositeKeyTwo.addKey(1);
    compositeKeyTwo.addKey(3);
    compositeKeyTwo.addKey(1);

    assertEquals(-1, compositeKeyOne.compareTo(compositeKeyTwo));
    assertEquals(1, compositeKeyTwo.compareTo(compositeKeyOne));
  }

  @Test
  public void testCompareToSymmetryTwo() {
    final CompositeKey compositeKeyOne = new CompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(2);

    final CompositeKey compositeKeyTwo = new CompositeKey();
    compositeKeyTwo.addKey(1);
    compositeKeyTwo.addKey(2);
    compositeKeyTwo.addKey(3);

    assertEquals(0, compositeKeyOne.compareTo(compositeKeyTwo));
    assertEquals(0, compositeKeyTwo.compareTo(compositeKeyOne));
  }

  @Test
  public void testCompareNullAtTheEnd() {
    final CompositeKey compositeKeyOne = new CompositeKey();
    compositeKeyOne.addKey(2);
    compositeKeyOne.addKey(2);

    final CompositeKey compositeKeyTwo = new CompositeKey();
    compositeKeyTwo.addKey(2);
    compositeKeyTwo.addKey(null);

    final CompositeKey compositeKeyThree = new CompositeKey();
    compositeKeyThree.addKey(2);
    compositeKeyThree.addKey(null);

    assertEquals(1, compositeKeyOne.compareTo(compositeKeyTwo));
    assertEquals(-1, compositeKeyTwo.compareTo(compositeKeyOne));
    assertEquals(0, compositeKeyTwo.compareTo(compositeKeyThree));
  }

  @Test
  public void testCompareNullAtTheMiddle() {
    final CompositeKey compositeKeyOne = new CompositeKey();
    compositeKeyOne.addKey(2);
    compositeKeyOne.addKey(2);
    compositeKeyOne.addKey(3);

    final CompositeKey compositeKeyTwo = new CompositeKey();
    compositeKeyTwo.addKey(2);
    compositeKeyTwo.addKey(null);
    compositeKeyTwo.addKey(3);

    final CompositeKey compositeKeyThree = new CompositeKey();
    compositeKeyThree.addKey(2);
    compositeKeyThree.addKey(null);
    compositeKeyThree.addKey(3);

    assertEquals(1, compositeKeyOne.compareTo(compositeKeyTwo));
    assertEquals(-1, compositeKeyTwo.compareTo(compositeKeyOne));
    assertEquals(0, compositeKeyTwo.compareTo(compositeKeyThree));
  }

  @Test
  public void testDocumentSerializationCompositeKeyNull() {
    final CompositeKey compositeKeyOne = new CompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    EntityImpl document = compositeKeyOne.toEntity(db);

    final CompositeKey compositeKeyTwo = new CompositeKey();
    compositeKeyTwo.fromDocument(document);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);
  }

  @Test
  public void testNativeBinarySerializationCompositeKeyNull() {
    final CompositeKey compositeKeyOne = new CompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    int len = CompositeKeySerializer.INSTANCE.getObjectSize(compositeKeyOne);
    byte[] data = new byte[len];
    CompositeKeySerializer.INSTANCE.serializeNativeObject(compositeKeyOne, data, 0);

    final CompositeKey compositeKeyTwo =
        CompositeKeySerializer.INSTANCE.deserializeNativeObject(data, 0);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);
  }

  @Test
  public void testByteBufferBinarySerializationCompositeKeyNull() {
    final int serializationOffset = 5;

    final CompositeKey compositeKeyOne = new CompositeKey();
    compositeKeyOne.addKey(1);
    compositeKeyOne.addKey(null);
    compositeKeyOne.addKey(2);

    final int len = CompositeKeySerializer.INSTANCE.getObjectSize(compositeKeyOne);

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    CompositeKeySerializer.INSTANCE.serializeInByteBufferObject(compositeKeyOne, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    assertEquals(CompositeKeySerializer.INSTANCE.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    final CompositeKey compositeKeyTwo =
        CompositeKeySerializer.INSTANCE.deserializeFromByteBufferObject(buffer);

    assertEquals(compositeKeyOne, compositeKeyTwo);
    assertNotSame(compositeKeyOne, compositeKeyTwo);

    assertEquals(buffer.position() - serializationOffset, len);
  }

  @Test
  public void testWALChangesBinarySerializationCompositeKeyNull() {
    final int serializationOffset = 5;

    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(1);
    compositeKey.addKey(null);
    compositeKey.addKey(2);

    final int len = CompositeKeySerializer.INSTANCE.getObjectSize(compositeKey);
    final ByteBuffer buffer =
        ByteBuffer.allocateDirect(len + serializationOffset + WALPageChangesPortion.PORTION_BYTES)
            .order(ByteOrder.nativeOrder());
    final byte[] data = new byte[len];

    CompositeKeySerializer.INSTANCE.serializeNativeObject(compositeKey, data, 0);
    final WALChanges walChanges = new WALPageChangesPortion();
    walChanges.setBinaryValue(buffer, data, serializationOffset);

    assertEquals(
        CompositeKeySerializer.INSTANCE.getObjectSizeInByteBuffer(
            buffer, walChanges, serializationOffset),
        len);
    assertEquals(
        CompositeKeySerializer.INSTANCE.deserializeFromByteBufferObject(
            buffer, walChanges, serializationOffset),
        compositeKey);
  }

  @Test
  public void testNetworkSerialization() throws IOException {
    String k1 = "key";
    CompositeKey k2 = new CompositeKey(null, new CompositeKey("user1", 12.5));
    CompositeKey compositeKey = new CompositeKey(k1, k2);
    RecordSerializerNetworkV37 serializer = RecordSerializerNetworkV37.INSTANCE;
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outStream);
    compositeKey.toStream(db, serializer, out);
    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    DataInputStream in = new DataInputStream(inStream);
    CompositeKey deserializedCompositeKey = new CompositeKey();
    deserializedCompositeKey.fromStream(db, serializer, in);
    assertEquals(compositeKey, deserializedCompositeKey);
  }
}
