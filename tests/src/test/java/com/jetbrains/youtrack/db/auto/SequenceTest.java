package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.internal.core.exception.SequenceException;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence.SEQUENCE_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.SequenceLibrary;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 3/2/2015
 */
@Test(groups = "sequence")
public class SequenceTest extends BaseDBTest {

  private static final int CACHE_SIZE = 40;
  private static final long FIRST_START = Sequence.DEFAULT_START;
  private static final long SECOND_START = 31;

  @Parameters(value = "remote")
  public SequenceTest(boolean remote) {
    super(remote);
  }

  @Test
  public void trivialTest() throws ExecutionException, InterruptedException {
    testSequence("seq1", SEQUENCE_TYPE.ORDERED);
    testSequence("seq2", SEQUENCE_TYPE.CACHED);
  }

  private void testSequence(String sequenceName, SEQUENCE_TYPE sequenceType)
      throws ExecutionException, InterruptedException {
    SequenceLibrary sequenceLibrary = db.getMetadata().getSequenceLibrary();

    Sequence seq = sequenceLibrary.createSequence(sequenceName, sequenceType, null);

    SequenceException err = null;
    try {
      sequenceLibrary.createSequence(sequenceName, sequenceType, null);
    } catch (SequenceException se) {
      err = se;
    }
    Assert.assertTrue(
        err == null || err.getMessage().toLowerCase(Locale.ENGLISH).contains("already exists"),
        "Creating a second "
            + sequenceType.toString()
            + " sequences with same name doesn't throw an exception");

    Sequence seqSame = sequenceLibrary.getSequence(sequenceName);
    Assert.assertEquals(seqSame, seq);

    // Doing it twice to check everything works after reset
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(seq.next(db), 1L);
      Assert.assertEquals(seq.current(db), 1L);
      Assert.assertEquals(seq.next(db), 2L);
      Assert.assertEquals(seq.next(db), 3L);
      Assert.assertEquals(seq.next(db), 4L);
      Assert.assertEquals(seq.current(db), 4L);
      Assert.assertEquals(seq.reset(db), 0L);
    }
  }

  @Test
  public void testOrdered() throws ExecutionException, InterruptedException {
    SequenceLibrary sequenceManager = db.getMetadata().getSequenceLibrary();

    Sequence seq = sequenceManager.createSequence("seqOrdered", SEQUENCE_TYPE.ORDERED, null);

    SequenceException err = null;
    try {
      sequenceManager.createSequence("seqOrdered", SEQUENCE_TYPE.ORDERED, null);
    } catch (SequenceException se) {
      err = se;
    }
    Assert.assertTrue(
        err == null || err.getMessage().toLowerCase(Locale.ENGLISH).contains("already exists"),
        "Creating two ordered sequences with same name doesn't throw an exception");

    Sequence seqSame = sequenceManager.getSequence("seqOrdered");
    Assert.assertEquals(seqSame, seq);

    testUsage(seq, FIRST_START);

    //
    db.begin();
    seq.updateParams(db, new Sequence.CreateParams().setStart(SECOND_START).setCacheSize(13));
    db.commit();

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
