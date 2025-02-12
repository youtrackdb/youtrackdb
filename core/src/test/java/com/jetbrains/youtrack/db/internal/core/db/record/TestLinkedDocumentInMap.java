package com.jetbrains.youtrack.db.internal.core.db.record;

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
    session.getMetadata().getSchema().createClass("PersonTest");
    session.command("delete from PersonTest").close();

    session.begin();
    var jaimeDoc = (EntityImpl) session.newEntity("PersonTest");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();
    session.commit();

    session.begin();
    jaimeDoc = session.bindToSession(jaimeDoc);
    var tyrionDoc = (EntityImpl) session.newEntity("PersonTest");
    tyrionDoc.updateFromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":[{\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}]}");
    tyrionDoc.save();
    session.commit();

    tyrionDoc = session.bindToSession(tyrionDoc);
    List<Map<String, Identifiable>> res = tyrionDoc.field("emergency_contact");
    var doc = res.get(0);
    Assert.assertTrue(((RecordId) doc.get("contact").getIdentity()).isValid());

    reOpen("admin", "adminpwd");
    try (var result = session.query("select from " + tyrionDoc.getIdentity())) {
      res = result.next().getProperty("emergency_contact");
      doc = res.get(0);
      Assert.assertTrue(((RecordId) doc.get("contact").getIdentity()).isValid());
    }
  }
}
