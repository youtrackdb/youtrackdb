package com.jetbrains.youtrack.db.internal.core.storage.index.nkbtree.normalizers;

import com.jetbrains.youtrack.db.internal.core.index.OCompositeKey;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
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

  private OCompositeKey binaryCompositeKey;
  private YTType[] binaryTypes;

  private OCompositeKey dateCompositeKey;
  private YTType[] dateTypes;

  private OCompositeKey dateTimeCompositeKey;
  private YTType[] dateTimeTypes;

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
    binaryCompositeKey = new OCompositeKey();
    binaryCompositeKey.addKey(binaryKey);
    binaryTypes = new YTType[1];
    binaryTypes[0] = YTType.BINARY;
  }

  private void dateFixture() {
    final Date key = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    dateCompositeKey = new OCompositeKey();
    dateCompositeKey.addKey(key);
    dateTypes = new YTType[1];
    dateTypes[0] = YTType.DATE;
  }

  private void dateTimeFixture() {
    final LocalDateTime ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final Date key = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    dateTimeCompositeKey = new OCompositeKey();
    dateTimeCompositeKey.addKey(key);
    dateTimeTypes = new YTType[1];
    dateTimeTypes[0] = YTType.DATETIME;
  }

  // final ByteArrayOutputStream bos = new ByteArrayOutputStream();

  @Benchmark
  public void normalizeCompositeNull() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);

    final YTType[] types = new YTType[1];
    types[0] = null;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeAddKeyNull() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
  }

  @Benchmark
  public void normalizeCompositeNullInt() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
    compositeKey.addKey(5);

    final YTType[] types = new YTType[2];
    types[0] = null;
    types[1] = YTType.INTEGER;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeInt() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(5);

    final YTType[] types = new YTType[1];
    types[0] = YTType.INTEGER;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeFloat() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(1.5f);

    final YTType[] types = new YTType[1];
    types[0] = YTType.FLOAT;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDouble() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(1.5d);

    final YTType[] types = new YTType[1];
    types[0] = YTType.DOUBLE;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeBoolean() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(true);

    final YTType[] types = new YTType[1];
    types[0] = YTType.BOOLEAN;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeLong() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(5L);

    final YTType[] types = new YTType[1];
    types[0] = YTType.LONG;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeByte() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey((byte) 3);

    final YTType[] types = new YTType[1];
    types[0] = YTType.BYTE;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeShort() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey((short) 3);

    final YTType[] types = new YTType[1];
    types[0] = YTType.SHORT;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDecimal() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(new BigDecimal("3.14159265359"));

    final YTType[] types = new YTType[1];
    types[0] = YTType.DECIMAL;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeString() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("abcd");

    final YTType[] types = new YTType[1];
    types[0] = YTType.STRING;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeTwoStrings() {
    final OCompositeKey compositeKey = new OCompositeKey();
    final String key = "abcd";
    compositeKey.addKey(key);
    final String secondKey = "test";
    compositeKey.addKey(secondKey);

    final YTType[] types = new YTType[2];
    types[0] = YTType.STRING;
    types[1] = YTType.STRING;
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
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(new BigDecimal(new BigInteger("20"), 2));

    final YTType[] types = new YTType[1];
    types[0] = YTType.DECIMAL;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }
}
