package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTSequenceException;
import com.orientechnologies.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.core.metadata.sequence.YTSequence;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 3/5/2015
 */
@Test(groups = "SqlSequence")
public class SQLSequenceTest extends DocumentDBBaseTest {

  private static final long FIRST_START = YTSequence.DEFAULT_START;
  private static final long SECOND_START = 31;

  @Parameters(value = "remote")
  public SQLSequenceTest(boolean remote) {
    super(remote);
  }

  @Test
  public void trivialTest() {
    testSequence("seqSQL1", YTSequence.SEQUENCE_TYPE.ORDERED);
    testSequence("seqSQL2", YTSequence.SEQUENCE_TYPE.CACHED);
  }

  private void testSequence(String sequenceName, YTSequence.SEQUENCE_TYPE sequenceType) {

    database.command("CREATE SEQUENCE " + sequenceName + " TYPE " + sequenceType).close();

    YTCommandExecutionException err = null;
    try {
      database.command("CREATE SEQUENCE " + sequenceName + " TYPE " + sequenceType).close();
    } catch (YTCommandExecutionException se) {
      err = se;
    }
    Assert.assertTrue(
        err == null || err.getMessage().toLowerCase(Locale.ENGLISH).contains("already exists"),
        "Creating a second "
            + sequenceType.toString()
            + " sequences with same name doesn't throw an exception");

    // Doing it twice to check everything works after reset
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(sequenceCurrent(sequenceName), 0L);
      Assert.assertEquals(sequenceNext(sequenceName), 1L);
      Assert.assertEquals(sequenceCurrent(sequenceName), 1L);
      Assert.assertEquals(sequenceNext(sequenceName), 2L);
      Assert.assertEquals(sequenceNext(sequenceName), 3L);
      Assert.assertEquals(sequenceNext(sequenceName), 4L);
      Assert.assertEquals(sequenceCurrent(sequenceName), 4L);
      Assert.assertEquals(sequenceReset(sequenceName), 0L);
    }
  }

  private long sequenceReset(String sequenceName) {
    return sequenceSql(sequenceName, "reset()");
  }

  private long sequenceNext(String sequenceName) {
    return sequenceSql(sequenceName, "next()");
  }

  private long sequenceCurrent(String sequenceName) {
    return sequenceSql(sequenceName, "current()");
  }

  private long sequenceSql(String sequenceName, String cmd) {
    try (YTResultSet ret =
        database.command("SELECT sequence('" + sequenceName + "')." + cmd + " as value")) {
      return ret.next().getProperty("value");
    }
  }

  @Test
  public void testFree() throws ExecutionException, InterruptedException {
    OSequenceLibrary sequenceManager = database.getMetadata().getSequenceLibrary();

    YTSequence seq = null;
    try {
      seq = sequenceManager.createSequence("seqSQLOrdered", YTSequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (YTDatabaseException exc) {
      Assert.fail("Unable to create sequence");
    }

    YTSequenceException err = null;
    try {
      sequenceManager.createSequence("seqSQLOrdered", YTSequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (YTSequenceException se) {
      err = se;
    } catch (YTDatabaseException exc) {
      Assert.fail("Unable to create sequence");
    }

    Assert.assertTrue(
        err == null || err.getMessage().toLowerCase(Locale.ENGLISH).contains("already exists"),
        "Creating two ordered sequences with same name doesn't throw an exception");

    YTSequence seqSame = sequenceManager.getSequence("seqSQLOrdered");
    Assert.assertEquals(seqSame, seq);

    testUsage(seq, FIRST_START);

    //
    try {
      database.begin();
      seq.updateParams(new YTSequence.CreateParams().setStart(SECOND_START).setCacheSize(13));
      database.commit();
    } catch (YTDatabaseException exc) {
      Assert.fail("Unable to update paramas");
    }
    testUsage(seq, SECOND_START);
  }

  private void testUsage(YTSequence seq, long reset)
      throws ExecutionException, InterruptedException {
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(seq.reset(), reset);
      Assert.assertEquals(seq.current(), reset);
      Assert.assertEquals(seq.next(), reset + 1L);
      Assert.assertEquals(seq.current(), reset + 1L);
      Assert.assertEquals(seq.next(), reset + 2L);
      Assert.assertEquals(seq.next(), reset + 3L);
      Assert.assertEquals(seq.next(), reset + 4L);
      Assert.assertEquals(seq.current(), reset + 4L);
      Assert.assertEquals(seq.reset(), reset);
    }
  }
}
