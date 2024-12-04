package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyNormalizer {

  private final Map<YTType, KeyNormalizers> normalizers = new HashMap<>();

  public KeyNormalizer() {
    normalizers.put(null, new NullKeyNormalizer());
    normalizers.put(YTType.INTEGER, new IntegerKeyNormalizer());
    normalizers.put(YTType.FLOAT, new FloatKeyNormalizer());
    normalizers.put(YTType.DOUBLE, new DoubleKeyNormalizer());
    normalizers.put(YTType.SHORT, new ShortKeyNormalizer());
    normalizers.put(YTType.BOOLEAN, new BooleanKeyNormalizer());
    normalizers.put(YTType.BYTE, new ByteKeyNormalizer());
    normalizers.put(YTType.LONG, new LongKeyNormalizer());
    normalizers.put(YTType.STRING, new StringKeyNormalizer());
    normalizers.put(YTType.DECIMAL, new DecimalKeyNormalizer());
    normalizers.put(YTType.DATE, new DateKeyNormalizer());
    normalizers.put(YTType.DATETIME, new DateTimeKeyNormalizer());
    normalizers.put(YTType.BINARY, new BinaryKeyNormalizer());
  }

  public byte[] normalize(
      final OCompositeKey keys, final YTType[] keyTypes, final int decompositon) {
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
    final AtomicInteger counter = new AtomicInteger(0);
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
      final YTType keyType,
      final int decompositon) {
    try {
      final KeyNormalizers keyNormalizer = normalizers.get(keyType);
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
