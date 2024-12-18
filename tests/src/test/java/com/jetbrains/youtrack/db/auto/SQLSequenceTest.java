package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.exception.SequenceException;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.SequenceLibrary;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 3/5/2015
 */
@Test(groups = "SqlSequence")
public class SQLSequenceTest extends BaseDBTest {

  private static final long FIRST_START = Sequence.DEFAULT_START;
  private static final long SECOND_START = 31;

  @Parameters(value = "remote")
  public SQLSequenceTest(boolean remote) {
    super(remote);
  }

  @Test
  public void trivialTest() {
    testSequence("seqSQL1", Sequence.SEQUENCE_TYPE.ORDERED);
    testSequence("seqSQL2", Sequence.SEQUENCE_TYPE.CACHED);
  }

  private void testSequence(String sequenceName, Sequence.SEQUENCE_TYPE sequenceType) {

    db.command("CREATE SEQUENCE " + sequenceName + " TYPE " + sequenceType).close();

    CommandExecutionException err = null;
    try {
      db.command("CREATE SEQUENCE " + sequenceName + " TYPE " + sequenceType).close();
    } catch (CommandExecutionException se) {
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
    try (ResultSet ret =
        db.command("SELECT sequence('" + sequenceName + "')." + cmd + " as value")) {
      return ret.next().getProperty("value");
    }
  }

  @Test
  public void testFree() throws ExecutionException, InterruptedException {
    SequenceLibrary sequenceManager = db.getMetadata().getSequenceLibrary();

    Sequence seq = null;
    try {
      seq = sequenceManager.createSequence("seqSQLOrdered", Sequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (DatabaseException exc) {
      Assert.fail("Unable to create sequence");
    }

    SequenceException err = null;
    try {
      sequenceManager.createSequence("seqSQLOrdered", Sequence.SEQUENCE_TYPE.ORDERED, null);
    } catch (SequenceException se) {
      err = se;
    } catch (DatabaseException exc) {
      Assert.fail("Unable to create sequence");
    }

    Assert.assertTrue(
        err == null || err.getMessage().toLowerCase(Locale.ENGLISH).contains("already exists"),
        "Creating two ordered sequences with same name doesn't throw an exception");

    Sequence seqSame = sequenceManager.getSequence("seqSQLOrdered");
    Assert.assertEquals(seqSame, seq);

    testUsage(seq, FIRST_START);

    //
    try {
      db.begin();
      seq.updateParams(db, new Sequence.CreateParams().setStart(SECOND_START).setCacheSize(13));
      db.commit();
    } catch (DatabaseException exc) {
      Assert.fail("Unable to update paramas");
    }
    testUsage(seq, SECOND_START);
  }

  private void testUsage(Sequence seq, long reset)
      throws ExecutionException, InterruptedException {
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(seq.reset(db), reset);
      Assert.assertEquals(seq.current(db), reset);
      Assert.assertEquals(seq.next(db), reset + 1L);
      Assert.assertEquals(seq.current(db), reset + 1L);
      Assert.assertEquals(seq.next(db), reset + 2L);
      Assert.assertEquals(seq.next(db), reset + 3L);
      Assert.assertEquals(seq.next(db), reset + 4L);
      Assert.assertEquals(seq.current(db), reset + 4L);
      Assert.assertEquals(seq.reset(db), reset);
    }
  }
}
