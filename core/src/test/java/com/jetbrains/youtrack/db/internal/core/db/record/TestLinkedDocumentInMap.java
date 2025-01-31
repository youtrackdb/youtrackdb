package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestLinkedDocumentInMap extends DbTestBase {

  @Test
  public void testLinkedValue() {
    db.getMetadata().getSchema().createClass("PersonTest");
    db.command("delete from PersonTest").close();

    db.begin();
    var jaimeDoc = (EntityImpl) db.newEntity("PersonTest");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();
    db.commit();

    db.begin();
    jaimeDoc = db.bindToSession(jaimeDoc);
    var tyrionDoc = (EntityImpl) db.newEntity("PersonTest");
    tyrionDoc.updateFromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":[{\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}]}");
    tyrionDoc.save();
    db.commit();

    tyrionDoc = db.bindToSession(tyrionDoc);
    List<Map<String, Identifiable>> res = tyrionDoc.field("emergency_contact");
    var doc = res.get(0);
    Assert.assertTrue(((RecordId) doc.get("contact").getIdentity()).isValid());

    reOpen("admin", "adminpwd");
    try (var result = db.query("select from " + tyrionDoc.getIdentity())) {
      res = result.next().getProperty("emergency_contact");
      doc = res.get(0);
      Assert.assertTrue(((RecordId) doc.get("contact").getIdentity()).isValid());
    }
  }
}
