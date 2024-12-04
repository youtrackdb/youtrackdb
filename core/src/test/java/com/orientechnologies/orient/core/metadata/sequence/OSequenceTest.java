package com.orientechnologies.orient.core.metadata.sequence;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSequenceException;
import com.orientechnologies.orient.core.record.OVertex;
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
public class OSequenceTest {

  private static OxygenDB oxygenDB;

  private ODatabaseSessionInternal db;
  private OSequenceLibrary sequences;

  @BeforeClass
  public static void beforeClass() {
    var builder = OxygenDBConfig.builder();

    builder.addConfig(OGlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    oxygenDB = OxygenDB.embedded("./target/databases/" + OSequenceTest.class.getSimpleName(),
        builder.build());
  }

  @AfterClass
  public static void afterClass() {
    oxygenDB.close();
  }

  @Before
  public void setUp() throws Exception {
    oxygenDB.create(
        OSequenceTest.class.getSimpleName(), ODatabaseType.MEMORY, "admin", "admin", "admin");
    db =
        (ODatabaseSessionInternal)
            oxygenDB.open(OSequenceTest.class.getSimpleName(), "admin", "admin");
    sequences = db.getMetadata().getSequenceLibrary();
  }

  @After
  public void after() {
    oxygenDB.drop(OSequenceTest.class.getSimpleName());
    db.close();
  }

  @Test
  public void shouldCreateSeqWithGivenAttribute() {
    try {
      sequences.createSequence(
          "mySeq", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams().setDefaults());
    } catch (ODatabaseException exc) {
      Assert.fail("Can not create sequence");
    }

    assertThat(sequences.getSequenceCount()).isEqualTo(1);
    assertThat(sequences.getSequenceNames()).contains("MYSEQ");

    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.getSequenceType()).isEqualTo(OSequence.SEQUENCE_TYPE.ORDERED);
    assertThat(myseq.getMaxRetry()).isEqualTo(1_000);
  }

  @Test
  public void shouldGivesValuesOrdered() {
    sequences.createSequence(
        "mySeq", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams().setDefaults());
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.current()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.current()).isEqualTo(2);
  }

  @Test
  public void shouldGivesValuesWithIncrement() {
    OSequence.CreateParams params = new OSequence.CreateParams().setDefaults().setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");

    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(30);
    assertThat(myseq.current()).isEqualTo(30);
    assertThat(myseq.next()).isEqualTo(60);
  }

  @Test
  public void shouldCache() {
    OSequence.CreateParams params =
        new OSequence.CreateParams().setDefaults().setCacheSize(100).setIncrement(30);
    assertThat(params.increment).isEqualTo(30);

    db.begin();
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq).isInstanceOf(OSequenceCached.class);
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

  @Test(expected = OSequenceException.class)
  public void shouldThrowExceptionOnDuplicateSeqDefinition() {
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);
  }

  @Test
  public void shouldDropSequence() {
    db.begin();
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, null);
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
    OSequence.CreateParams params = new OSequence.CreateParams().setStart(0L);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
  }

  @Test
  public void shouldSequenceMTNoTx() throws Exception {
    OSequence.CreateParams params = new OSequence.CreateParams().setStart(0L);
    OSequence mtSeq = sequences.createSequence("mtSeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
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
            ODatabaseSessionInternal databaseDocument =
                (ODatabaseSessionInternal)
                    oxygenDB.open(OSequenceTest.class.getSimpleName(), "admin", "admin");
            OSequence mtSeq1 =
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
    OSequence.CreateParams params = new OSequence.CreateParams().setStart(0L);
    OSequence mtSeq = sequences.createSequence("mtSeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    final int count = 1000;
    final int threads = 2;
    final CountDownLatch latch = new CountDownLatch(count);
    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicInteger success = new AtomicInteger(0);
    ExecutorService service = Executors.newFixedThreadPool(threads);

    for (int i = 0; i < threads; i++) {
      service.execute(
          () -> {
            ODatabaseSessionInternal databaseDocument =
                (ODatabaseSessionInternal)
                    oxygenDB.open(OSequenceTest.class.getSimpleName(), "admin", "admin");
            OSequence mtSeq1 =
                databaseDocument.getMetadata().getSequenceLibrary().getSequence("mtSeq");

            for (int j = 0; j < count / threads; j++) {
              for (int retry = 0; retry < 10; ++retry) {
                try {

                  databaseDocument.begin();
                  mtSeq1.next();
                  databaseDocument.commit();
                  success.incrementAndGet();
                  break;

                } catch (OConcurrentModificationException e) {
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
            OVertex person = db.newVertex("Person");
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
      OVertex person = db.newVertex("Person");
      person.setProperty("name", "Foo" + i);
      person.save();
    }

    db.commit();

    assertThat(db.countClass("Person")).isEqualTo(10);
  }

  @Test
  public void testCachedSequeneceUpperLimit() throws Exception {
    // issue #6484
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setRecyclable(true)
            .setLimitValue(30L);
    db.begin();
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(30L)
            .setIncrement(10)
            .setLimitValue(0L)
            .setRecyclable(true)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    OSequence.CreateParams params =
        new OSequence.CreateParams().setStart(0L).setIncrement(1).setCacheSize(3);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(6L)
            .setIncrement(1)
            .setCacheSize(3)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setRecyclable(true)
            .setLimitValue(30L);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(6L)
            .setIncrement(1)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(30L)
            .setIncrement(10)
            .setLimitValue(0L)
            .setRecyclable(true)
            .setOrderType(SequenceOrderType.ORDER_NEGATIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setLimitValue(30L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE)
            .setRecyclable(false);
    db.begin();
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
          } catch (OSequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);

          sequences.dropSequence("MYSEQ");
        });
  }

  @Test
  public void testNonRecyclableOrderedSequeneceLimitReach() throws Exception {
    // issue #6484
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(10)
            .setLimitValue(30L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE)
            .setRecyclable(false);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
          } catch (OSequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);

          sequences.dropSequence("MYSEQ");
        });
  }

  @Test
  public void testReinitSequence() {
    db.begin();
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(5L)
            .setCacheSize(3)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    OSequence newSeq = new OSequenceCached(myseq.docRid.getRecord());
    long val = newSeq.current();
    assertThat(val).isEqualTo(5);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            newSeq.next();
          } catch (OSequenceLimitReachedException exc) {
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(3L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
          } catch (OSequenceLimitReachedException exc) {
            exceptionsCought++;
          }

          assertThat(exceptionsCought).isEqualTo((byte) 1);
        });

    db.begin();
    params = new OSequence.CreateParams().resetNull().setTurnLimitOff(true);
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.current()).isEqualTo(0);
    assertThat(myseq.next()).isEqualTo(1);
    assertThat(myseq.next()).isEqualTo(2);
    assertThat(myseq.next()).isEqualTo(3);
    db.commit();

    db.begin();
    params = new OSequence.CreateParams().resetNull().setLimitValue(3L);
    myseq.updateParams(params);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (OSequenceLimitReachedException exc) {
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(3L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
          } catch (OSequenceLimitReachedException exc) {
            exceptionsCought++;
          }
          assertThat(exceptionsCought).isEqualTo((byte) 1);
        });

    db.begin();
    params = new OSequence.CreateParams().resetNull().setTurnLimitOff(true);
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    db.begin();
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.ORDERED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
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
    params = new OSequence.CreateParams().resetNull().setLimitValue(3L);
    myseq.updateParams(params);
    db.commit();

    db.executeInTx(
        () -> {
          Byte exceptionsCought = 0;
          try {
            myseq.next();
          } catch (OSequenceLimitReachedException exc) {
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
    OSequence.CreateParams params =
        new OSequence.CreateParams()
            .setStart(0L)
            .setIncrement(1)
            .setLimitValue(10L)
            .setOrderType(SequenceOrderType.ORDER_POSITIVE);
    sequences.createSequence("mySeq", OSequence.SEQUENCE_TYPE.CACHED, params);
    db.commit();

    db.begin();
    OSequence myseq = sequences.getSequence("MYSEQ");
    assertThat(myseq.next()).isEqualTo(1);
    db.commit();

    db.begin();
    assertThat(myseq.next()).isEqualTo(2);
    db.commit();

    db.begin();
    params = new OSequence.CreateParams().resetNull().setRecyclable(true).setCacheSize(3);
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
    params = new OSequence.CreateParams().resetNull().setLimitValue(11L);
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
    params = new OSequence.CreateParams().resetNull().setLimitValue(12L);
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
