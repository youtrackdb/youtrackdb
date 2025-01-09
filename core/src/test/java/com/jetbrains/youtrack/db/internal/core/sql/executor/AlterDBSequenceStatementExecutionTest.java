package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AlterDBSequenceStatementExecutionTest extends DbTestBase {

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }

  @Test
  public void testSetIncrement() {
    String sequenceName = "testSetStart";
    db.executeInTx(
        () -> {
          try {
            db.getMetadata()
                .getSequenceLibrary()
                .createSequence(
                    sequenceName, DBSequence.SEQUENCE_TYPE.ORDERED, new DBSequence.CreateParams());
          } catch (DatabaseException exc) {
            Assert.fail("Failed to create sequence");
          }
        });

    db.begin();
    ResultSet result = db.command("alter sequence " + sequenceName + " increment 20");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals((Object) 20, next.getProperty("increment"));
    result.close();
    db.commit();

    db.executeInTx(
        () -> {
          DBSequence seq = db.getMetadata().getSequenceLibrary().getSequence(sequenceName);
          Assert.assertNotNull(seq);
          try {
            Assert.assertEquals(20, seq.next());
          } catch (DatabaseException exc) {
            Assert.fail("Failed to call next");
          }
        });
  }
}
