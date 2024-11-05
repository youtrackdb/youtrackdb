package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author edegtyarenko
 * @since 11.03.13 12:00
 */
@Test(groups = {"crud"})
public class RecordMetadataTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public RecordMetadataTest(boolean remote) {
    super(remote);
  }

  private static void assetORIDEquals(ORID actual, ORID expected) {
    assertEquals(actual.getClusterId(), expected.getClusterId());
    assertEquals(actual.getClusterPosition(), expected.getClusterPosition());
  }

  public void testGetRecordMetadata() {

    final ODocument doc = new ODocument();

    for (int i = 0; i < 5; i++) {
      database.begin();
      doc.field("field", i);
      database.save(doc, database.getClusterNameById(database.getDefaultClusterId()));
      database.commit();

      final ORecordMetadata metadata = database.getRecordMetadata(doc.getIdentity());
      assetORIDEquals(doc.getIdentity(), metadata.getRecordId());
      assertEquals(doc.getVersion(), metadata.getVersion());
    }
  }
}
