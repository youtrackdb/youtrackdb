package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.core.metadata.sequence.YTSequence;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropSequenceStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    String name = "testPlain";
    try {
      db.getMetadata()
          .getSequenceLibrary()
          .createSequence(name, YTSequence.SEQUENCE_TYPE.CACHED, new YTSequence.CreateParams());
    } catch (YTDatabaseException exc) {
      Assert.fail("Creating sequence failed");
    }

    Assert.assertNotNull(db.getMetadata().getSequenceLibrary().getSequence(name));
    db.begin();
    YTResultSet result = db.command("drop sequence " + name);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop sequence", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    db.commit();

    Assert.assertNull(db.getMetadata().getSequenceLibrary().getSequence(name));
  }

  @Test
  public void testNonExisting() {
    String name = "testNonExisting";
    OSequenceLibrary lib = db.getMetadata().getSequenceLibrary();
    Assert.assertNull(lib.getSequence(name));
    try {
      YTResultSet result = db.command("drop sequence " + name);
      Assert.fail();
    } catch (YTCommandExecutionException ex1) {

    } catch (Exception ex1) {
      Assert.fail();
    }
  }

  @Test
  public void testNonExistingWithIfExists() {
    String name = "testNonExistingWithIfExists";
    OSequenceLibrary lib = db.getMetadata().getSequenceLibrary();
    Assert.assertNull(lib.getSequence(name));

    YTResultSet result = db.command("drop sequence " + name + " if exists");
    Assert.assertFalse(result.hasNext());

    try {
      db.getMetadata()
          .getSequenceLibrary()
          .createSequence(name, YTSequence.SEQUENCE_TYPE.CACHED, new YTSequence.CreateParams());
    } catch (YTDatabaseException exc) {
      Assert.fail("Creating sequence failed");
    }

    Assert.assertNotNull(db.getMetadata().getSequenceLibrary().getSequence(name));
    db.begin();
    result = db.command("drop sequence " + name + " if exists");
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop sequence", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    db.commit();

    Assert.assertNull(db.getMetadata().getSequenceLibrary().getSequence(name));
  }
}
