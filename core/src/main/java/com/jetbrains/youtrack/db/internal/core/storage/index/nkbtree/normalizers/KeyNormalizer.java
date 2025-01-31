package com.jetbrains.youtrack.db.internal.core.storage.index.nkbtree.normalizers;

import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyNormalizer {

  private final Map<PropertyType, KeyNormalizers> normalizers = new HashMap<>();

  public KeyNormalizer() {
    normalizers.put(null, new NullKeyNormalizer());
    normalizers.put(PropertyType.INTEGER, new IntegerKeyNormalizer());
    normalizers.put(PropertyType.FLOAT, new FloatKeyNormalizer());
    normalizers.put(PropertyType.DOUBLE, new DoubleKeyNormalizer());
    normalizers.put(PropertyType.SHORT, new ShortKeyNormalizer());
    normalizers.put(PropertyType.BOOLEAN, new BooleanKeyNormalizer());
    normalizers.put(PropertyType.BYTE, new ByteKeyNormalizer());
    normalizers.put(PropertyType.LONG, new LongKeyNormalizer());
    normalizers.put(PropertyType.STRING, new StringKeyNormalizer());
    normalizers.put(PropertyType.DECIMAL, new DecimalKeyNormalizer());
    normalizers.put(PropertyType.DATE, new DateKeyNormalizer());
    normalizers.put(PropertyType.DATETIME, new DateTimeKeyNormalizer());
    normalizers.put(PropertyType.BINARY, new BinaryKeyNormalizer());
  }

  public byte[] normalize(
      final CompositeKey keys, final PropertyType[] keyTypes, final int decompositon) {
    if (keys == null) {
      throw new IllegalArgumentException("Keys must not be null.");
    }
    if (keys.getKeys().size() != keyTypes.length) {
      throw new IllegalArgumentException(
          "Number of keys must fit to number of types: "
              + keys.getKeys().size()
              + " != "
              + keyTypes.length
              + ".");
    }
    final var counter = new AtomicInteger(0);
    return keys.getKeys().stream()
        .collect(
            ByteArrayOutputStream::new,
            (baos, key) -> {
              normalizeCompositeKeys(baos, key, keyTypes[counter.getAndIncrement()], decompositon);
            },
            (baos1, baos2) -> baos1.write(baos2.toByteArray(), 0, baos2.size()))
        .toByteArray();
  }

  private void normalizeCompositeKeys(
      final ByteArrayOutputStream normalizedKeyStream,
      final Object key,
      final PropertyType keyType,
      final int decompositon) {
    try {
      final var keyNormalizer = normalizers.get(keyType);
      if (keyNormalizer == null) {
        throw new UnsupportedOperationException(
            "Type " + key.getClass().getTypeName() + " is currently not supported");
      }
      normalizedKeyStream.write(keyNormalizer.execute(key, decompositon));
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }
}
