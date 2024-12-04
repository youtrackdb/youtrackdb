package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 11.03.13 12:00
 */
@Test
public class RecordMetadataTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public RecordMetadataTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  private static void assetORIDEquals(YTRID actual, YTRID expected) {
    assertEquals(actual.getClusterId(), expected.getClusterId());
    assertEquals(actual.getClusterPosition(), expected.getClusterPosition());
  }

  public void testGetRecordMetadata() {

    YTDocument doc = new YTDocument();
    for (int i = 0; i < 5; i++) {
      database.begin();
      if (!doc.getIdentity().isNew()) {
        doc = database.bindToSession(doc);
      }

      doc.field("field", i);
      database.save(doc, database.getClusterNameById(database.getDefaultClusterId()));
      database.commit();

      final ORecordMetadata metadata = database.getRecordMetadata(doc.getIdentity());
      assetORIDEquals(doc.getIdentity(), metadata.getRecordId());
      assertEquals(database.bindToSession(doc).getVersion(), metadata.getVersion());
    }
  }
}
