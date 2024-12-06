package com.jetbrains.youtrack.db.internal.core.metadata.sequence;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseType;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.SequenceException;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class SequenceTest {

  private static YouTrackDB youTrackDB;

  private DatabaseSessionInternal db;
  private SequenceLibrary sequences;

  @BeforeClass
  public static void beforeClass() {
    var builder = YouTrackDBConfig.builder();

    builder.addConfig(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    youTrackDB = YouTrackDB.embedded("./target/databases/" + SequenceTest.class.getSimpleName(),
        builder.build());
  }

  @AfterClass
  public static void afterClass() {
    youTrackDB.close();
  }

  @Before
  public void setUp() throws Exception {
    youTrackDB.create(
        SequenceTest.class.getSimpleName(), DatabaseType.MEMORY, "admin", "admin", "admin");
    db =
        (DatabaseSessionInternal)
            youTrackDB.open(SequenceTest.class.getSimpleName(), "admin", "admin");
    sequences = db.getMetadata().getSequenceLibrary();
  }

  @After
  public void after() {
    youTrackDB.drop(SequenceTest.class.getSimpleName());
    db.close();
  }

  @Test
  public void shouldCreateSeqWithGivenAttribute() {
    try {
      sequences.createSequence(
          "mySeq", Sequence.SEQUENCE_TYPE.ORDERED, new Sequence.CreateParams().setDefaults());
    } catch (DatabaseException exc) {
      Assert.fail("Can not create sequence");
    }

    assertThat(sequences.getSequenceCount()).isEqualTo(1);
    assertThat(sequences.getSequenceNames()).contains("MYSEQ");

    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.getSequenceType()).isEqualTo(Sequence.SEQUENCE_TYPE.ORDERED);
    assertThat(myseq.getMaxRetry()).isEqualTo(1_000);
  }

  @Test
  public void shouldGivesValuesOrdered() {
    sequences.createSequence(
        "mySeq", Sequence.SEQUENCE_TYPE.ORDERED, new Sequence.CreateParams().setDefaults());
    Sequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.current()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.current()).isEqualTo(2);
  }

  @Test
  public void shouldGivesValuesWithIncrement() {
    Sequence.CreateParams params = new Sequence.CreateParams().setDefaults().setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    Sequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(60);
  }

  @Test
  public void shouldCache() {
    Sequence.CreateParams params =
        new Sequence.CreateParams().setDefaults().setCacheSize(100).setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    db.begin();
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq).isInstanceOf(SequenceCached.class);
    db.commit();

    db.begin();
    assertThat(myseq.current()).isEqualTo(0);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(30);
    db.commit();

    db.begin();
    assertThat(myseq.current()).isEqualTo(30);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(60);
    db.commit();

    db.begin();
    assertThat(myseq.current()).isEqualTo(60);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(90);
    db.commit();

    db.begin();
    assertThat(myseq.current()).isEqualTo(90);
    assertThat(myseq.next()).isEqualTo(120);
    db.commit();

    db.begin();
    assertThat(myseq.current()).isEqualTo(120);
    db.commit();
  }

  @Test(expected = SequenceException.class)
  public void shouldThrowExceptionOnDuplicateSeqDefinition() {
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, null);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, null);
  }

  @Test
  public void shouldDropSequence() {
    db.begin();
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, null);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();

    db.begin();
    assertThat(sequences.getSequenceCount()).isEqualTo(0);
    db.commit();

    db.begin();
    // IDEMPOTENT
    sequences.dropSequence("MYSEQ");
    db.commit();

    db.begin();
    assertThat(sequences.getSequenceCount()).isEqualTo(0);
    db.commit();
  }

  @Test
  public void testCreateSequenceWithoutExplicitDefaults() {
    // issue #6484
    Sequence.CreateParams params = new Sequence.CreateParams().setStart(0L);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
  }

  @Test
  public void shouldSequenceMTNoTx() throws Exception {
    Sequence.CreateParams params = new Sequence.CreateParams().setStart(0L);
    Sequence mtSeq = sequences.createSequence("mtSeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    mtSeq.setMaxRetry(1000);
    final int count = 1000;
    final int threads = 2;
    final CountDownLatch latch = new CountDownLatch(count);
    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicInteger success = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(threads);

    for (int i = 0; i < threads; i++) {
      service.execute(
          () -> {
            DatabaseSessionInternal databaseDocument =
                (DatabaseSessionInternal)
                    youTrackDB.open(SequenceTest.class.getSimpleName(), "admin", "admin");
            Sequence mtSeq1 =
                databaseDocument.getMetadata().getSequenceLibrary().getSequence("mtSeq");

            for (int j = 0; j < count / threads; j++) {
              try {
                mtSeq1.next();
                success.incrementAndGet();
              } catch (Exception e) {
                e.printStackTrace();
                errors.incrementAndGet();
              }
              latch.countDown();
            }
          });
    }
    latch.await();

    assertThat(errors.get()).isEqualTo(0);
    assertThat(success.get()).isEqualTo(1000);
    //    assertThat(mtSeq.getDocument().getVersion()).isEqualTo(1001);
    assertThat(mtSeq.current()).isEqualTo(1000);
  }

  @Test
  public void shouldSequenceMTTx() throws Exception {
    Sequence.CreateParams params = new Sequence.CreateParams().setStart(0L);
    Sequence mtSeq = sequences.createSequence("mtSeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    final int count = 1000;
    final int threads = 2;
    final CountDownLatch latch = new CountDownLatch(count);
    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicInteger success = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(threads);

    for (int i = 0; i < threads; i++) {
      service.execute(
          () -> {
            DatabaseSessionInternal databaseDocument =
                (DatabaseSessionInternal)
                    youTrackDB.open(SequenceTest.class.getSimpleName(), "admin", "admin");
            Sequence mtSeq1 =
                databaseDocument.getMetadata().getSequenceLibrary().getSequence("mtSeq");

            for (int j = 0; j < count / threads; j++) {
              for (int retry = 0; retry < 10; ++retry) {
                try {

                  databaseDocument.begin();
                  mtSeq1.next();
                  databaseDocument.commit();
                  success.incrementAndGet();
                  break;

                } catch (ConcurrentModificationException e) {
                  if (retry >= 10) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                    break;
                  }

                  // RETRY
                  try {
                    Thread.sleep(10 + new Random().nextInt(100));
                  } catch (InterruptedException e1) {
                  }
                  continue;
                } catch (Exception e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
                }
              }
              latch.countDown();
            }
          });
    }
    latch.await();

    assertThat(errors.get()).isEqualTo(0);
    assertThat(success.get()).isEqualTo(1000);
    assertThat(mtSeq.current()).isEqualTo(1000);
  }

  @Test
  public void shouldSequenceWithDefaultValueNoTx() {

    db.command("CREATE CLASS Person EXTENDS V");
    db.command("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
    db.command(
        "CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default"
            + " \"sequence('personIdSequence').next()\");");
    db.command("CREATE INDEX Person.id ON Person (id) UNIQUE");

    db.executeInTx(
        () -> {
          for (int i = 0; i < 10; i++) {
            Vertex person = db.newVertex("Person");
            person.setProperty("name", "Foo" + i);
            person.save();
          }
        });

    assertThat(db.countClass("Person")).isEqualTo(10);
  }

  @Test
  public void shouldSequenceWithDefaultValueTx() {

    db.command("CREATE CLASS Person EXTENDS V");
    db.command("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
    db.command(
        "CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default"
            + " \"sequence('personIdSequence').next()\");");
    db.command("CREATE INDEX Person.id ON Person (id) UNIQUE");

    db.begin();

    for (int i = 0; i < 10; i++) {
      Vertex person = db.newVertex("Person");
      person.setProperty("name", "Foo" + i);
      person.save();
    }

    db.commit();

    assertThat(db.countClass("Person")).isEqualTo(10);
  }

  @Test
  public void testCachedSequeneceUpperLimit() throws Exception {
    // issue #6484
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setRecyclable(true)
            .setLimitValue(30L);
    db.begin();
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(10);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(20);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(30);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(0);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testNegativeCachedSequeneceDownerLimit() {
    // issue #6484
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(30L)
            .setIncrement(10)
            .setLimitValue(0L)
            .setRecyclable(true)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(20);
    assertThat(myseq.next()).isEqualTo(10);
    assertThat(myseq.next()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testCachedSequeneceOverCache() throws Exception {
    // issue #6484
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams().setStart(0L).setIncrement(1).setCacheSize(3);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.next()).isEqualTo(3);
    assertThat(myseq.next()).isEqualTo(4);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testNegativeCachedSequeneceOverCache() throws Exception {
    // issue #6484
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(6L)
            .setIncrement(1)
            .setCacheSize(3)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(6);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(5);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(4);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(2);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(1);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(0);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testOrderedSequeneceUpperLimit() throws Exception {
    // issue #6484
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setRecyclable(true)
            .setLimitValue(30L);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(10);
    assertThat(myseq.next()).isEqualTo(20);
    assertThat(myseq.next()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(0);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testNegativeOrderedSequenece() throws Exception {
    // issue #6484
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(6L)
            .setIncrement(1)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(6);
    assertThat(myseq.next()).isEqualTo(5);
    assertThat(myseq.next()).isEqualTo(4);
    assertThat(myseq.next()).isEqualTo(3);
    assertThat(myseq.next()).isEqualTo(2);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testNegativeOrderedSequeneceDownerLimit() throws Exception {
    // issue #6484
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(30L)
            .setIncrement(10)
            .setLimitValue(0L)
            .setRecyclable(true)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(20);
    assertThat(myseq.next()).isEqualTo(10);
    assertThat(myseq.next()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testNonRecyclableCachedSequeneceLimitReach() throws Exception {
    // issue #6484
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setLimitValue(30L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE)
            .setRecyclable(false);
    db.begin();
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    db.commit();

    db.begin();
    assertThat(myseq.current()).isEqualTo(0);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(10);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(20);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(30);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (SequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);

          sequences.dropSequence("MYSEQ");
        });
  }

  @Test
  public void testNonRecyclableOrderedSequeneceLimitReach() throws Exception {
    // issue #6484
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setLimitValue(30L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE)
            .setRecyclable(false);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(10);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(20);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(30);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (SequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);

          sequences.dropSequence("MYSEQ");
        });
  }

  @Test
  public void testReinitSequence() {
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(5L)
            .setCacheSize(3)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    Sequence newSeq = new SequenceCached(myseq.docRid.getRecord());
    long val = newSeq.current();
    assertThat(val).isEqualTo(5);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            newSeq.next();
          } catch (SequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);
        });

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testTurnLimitOffCached() {
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(3L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (SequenceLimitReachedException exc) {
            exceptionsCought++;
          }

          assertThat(exceptionsCought).isEqualTo((byte) 1);
        });

    db.begin();
    params = new Sequence.CreateParams().resetNull().setTurnLimitOff(true);
    myseq.updateParams(params);
    db.commit();

    db.begin();
    // there is reset after update params, so go from begining
    assertThat(myseq.next()).isEqualTo(4);
    assertThat(myseq.next()).isEqualTo(5);
    assertThat(myseq.next()).isEqualTo(6);
    assertThat(myseq.next()).isEqualTo(7);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testTurnLimitOnCached() throws Exception {
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    params = new Sequence.CreateParams().resetNull().setLimitValue(3L);
    myseq.updateParams(params);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (SequenceLimitReachedException exc) {
            exceptionsCought++;
          }

          assertThat(exceptionsCought).isEqualTo((byte) 1);
        });

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testTurnLimitOffOrdered() throws Exception {
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(3L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(1);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(2);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (SequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);
        });

    db.begin();
    params = new Sequence.CreateParams().resetNull().setTurnLimitOff(true);
    myseq.updateParams(params);
    db.commit();

    db.begin();
    // there is reset after update params, so go from begining
    assertThat(myseq.next()).isEqualTo(4);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(5);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(6);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(7);
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testTurnLimitOnOrdered() throws Exception {
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    db.begin();
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(1);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(2);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    params = new Sequence.CreateParams().resetNull().setLimitValue(3L);
    myseq.updateParams(params);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (SequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);
        });

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }

  @Test
  public void testAfterNextCache() throws Exception {
    db.begin();
    Sequence.CreateParams params =
        new Sequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(10L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", Sequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    Sequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.next()).isEqualTo(1);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(2);
    db.commit();

    db.begin();
    params = new Sequence.CreateParams().resetNull().setRecyclable(true).setCacheSize(3);
    myseq.updateParams(params);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(4);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(5);
    assertThat(myseq.next()).isEqualTo(6);
    assertThat(myseq.next()).isEqualTo(7);
    assertThat(myseq.next()).isEqualTo(8);
    db.commit();

    db.begin();
    params = new Sequence.CreateParams().resetNull().setLimitValue(11L);
    myseq.updateParams(params);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(9);
    assertThat(myseq.next()).isEqualTo(10);
    assertThat(myseq.next()).isEqualTo(11);
    assertThat(myseq.next()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    db.commit();

    db.begin();
    params = new Sequence.CreateParams().resetNull().setLimitValue(12L);
    myseq.updateParams(params);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    sequences.dropSequence("MYSEQ");
    db.commit();
  }
}
