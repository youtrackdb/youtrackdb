package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.SequenceLibrary;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DropDBSequenceStatementExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {
    String name = "testPlain";
    try {
      db.getMetadata()
          .getSequenceLibrary()
          .createSequence(name, DBSequence.SEQUENCE_TYPE.CACHED, new DBSequence.CreateParams());
    } catch (DatabaseException exc) {
      Assert.fail("Creating sequence failed");
    }

    Assert.assertNotNull(db.getMetadata().getSequenceLibrary().getSequence(name));
    db.begin();
    ResultSet result = db.command("drop sequence " + name);
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertEquals("drop sequence", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    db.commit();

    Assert.assertNull(db.getMetadata().getSequenceLibrary().getSequence(name));
  }

  @Test
  public void testNonExisting() {
    String name = "testNonExisting";
    SequenceLibrary lib = db.getMetadata().getSequenceLibrary();
    Assert.assertNull(lib.getSequence(name));
    try {
      ResultSet result = db.command("drop sequence " + name);
      Assert.fail();
    } catch (CommandExecutionException ex1) {

    } catch (Exception ex1) {
      Assert.fail();
    }
  }

  @Test
  public void testNonExistingWithIfExists() {
    String name = "testNonExistingWithIfExists";
    SequenceLibrary lib = db.getMetadata().getSequenceLibrary();
    Assert.assertNull(lib.getSequence(name));

    ResultSet result = db.command("drop sequence " + name + " if exists");
    Assert.assertFalse(result.hasNext());

    try {
      db.getMetadata()
          .getSequenceLibrary()
          .createSequence(name, DBSequence.SEQUENCE_TYPE.CACHED, new DBSequence.CreateParams());
    } catch (DatabaseException exc) {
      Assert.fail("Creating sequence failed");
    }

    Assert.assertNotNull(db.getMetadata().getSequenceLibrary().getSequence(name));
    db.begin();
    result = db.command("drop sequence " + name + " if exists");
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertEquals("drop sequence", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    db.commit();

    Assert.assertNull(db.getMetadata().getSequenceLibrary().getSequence(name));
  }
}
