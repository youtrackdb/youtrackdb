package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.YTSequence;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OAlterSequenceStatementExecutionTest extends DBTestBase {

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(YTGlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
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
                    sequenceName, YTSequence.SEQUENCE_TYPE.ORDERED, new YTSequence.CreateParams());
          } catch (ODatabaseException exc) {
            Assert.fail("Failed to create sequence");
          }
        });

    db.begin();
    OResultSet result = db.command("alter sequence " + sequenceName + " increment 20");
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals((Object) 20, next.getProperty("increment"));
    result.close();
    db.commit();

    db.executeInTx(
        () -> {
          YTSequence seq = db.getMetadata().getSequenceLibrary().getSequence(sequenceName);
          Assert.assertNotNull(seq);
          try {
            Assert.assertEquals(20, seq.next());
          } catch (ODatabaseException exc) {
            Assert.fail("Failed to call next");
          }
        });
  }
}
