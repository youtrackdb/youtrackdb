package com.jetbrains.youtrack.db.internal.core.storage.index.nkbtree.normalizers;

import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.Collator;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(1)
public class KeyNormalizerBenchmark {

  private KeyNormalizer keyNormalizer;

  private CompositeKey binaryCompositeKey;
  private PropertyType[] binaryTypes;

  private CompositeKey dateCompositeKey;
  private PropertyType[] dateTypes;

  private CompositeKey dateTimeCompositeKey;
  private PropertyType[] dateTimeTypes;

  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("KeyNormalizerBenchmark.*")
            .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    new Runner(opt).run();
  }

  @Setup(Level.Iteration)
  public void setup() {
    binaryFixture();
    dateFixture();
    dateTimeFixture();
  }

  private void binaryFixture() {
    keyNormalizer = new KeyNormalizer();
    final byte[] binaryKey = new byte[]{1, 2, 3, 4, 5, 6};
    binaryCompositeKey = new CompositeKey();
    binaryCompositeKey.addKey(binaryKey);
    binaryTypes = new PropertyType[1];
    binaryTypes[0] = PropertyType.BINARY;
  }

  private void dateFixture() {
    final Date key = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    dateCompositeKey = new CompositeKey();
    dateCompositeKey.addKey(key);
    dateTypes = new PropertyType[1];
    dateTypes[0] = PropertyType.DATE;
  }

  private void dateTimeFixture() {
    final LocalDateTime ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final Date key = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    dateTimeCompositeKey = new CompositeKey();
    dateTimeCompositeKey.addKey(key);
    dateTimeTypes = new PropertyType[1];
    dateTimeTypes[0] = PropertyType.DATETIME;
  }

  // final ByteArrayOutputStream bos = new ByteArrayOutputStream();

  @Benchmark
  public void normalizeCompositeNull() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(null);

    final PropertyType[] types = new PropertyType[1];
    types[0] = null;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeAddKeyNull() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(null);
  }

  @Benchmark
  public void normalizeCompositeNullInt() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(null);
    compositeKey.addKey(5);

    final PropertyType[] types = new PropertyType[2];
    types[0] = null;
    types[1] = PropertyType.INTEGER;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeInt() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(5);

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.INTEGER;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeFloat() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(1.5f);

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.FLOAT;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDouble() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(1.5d);

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.DOUBLE;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeBoolean() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(true);

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.BOOLEAN;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeLong() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(5L);

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.LONG;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeByte() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey((byte) 3);

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.BYTE;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeShort() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey((short) 3);

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.SHORT;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDecimal() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(new BigDecimal("3.14159265359"));

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.DECIMAL;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeString() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey("abcd");

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.STRING;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeTwoStrings() {
    final CompositeKey compositeKey = new CompositeKey();
    final String key = "abcd";
    compositeKey.addKey(key);
    final String secondKey = "test";
    compositeKey.addKey(secondKey);

    final PropertyType[] types = new PropertyType[2];
    types[0] = PropertyType.STRING;
    types[1] = PropertyType.STRING;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDate() {
    keyNormalizer.normalize(dateTimeCompositeKey, dateTimeTypes, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDateTime() {
    keyNormalizer.normalize(dateCompositeKey, dateTypes, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeBinary() {
    keyNormalizer.normalize(binaryCompositeKey, binaryTypes, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void decimalNormalizer() {
    final CompositeKey compositeKey = new CompositeKey();
    compositeKey.addKey(new BigDecimal(new BigInteger("20"), 2));

    final PropertyType[] types = new PropertyType[1];
    types[0] = PropertyType.DECIMAL;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }
}
