package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestLinkedDocumentInMap extends DBTestBase {

  @Test
  public void testLinkedValue() {
    db.getMetadata().getSchema().createClass("PersonTest");
    db.command("delete from PersonTest").close();

    db.begin();
    YTDocument jaimeDoc = new YTDocument("PersonTest");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();
    db.commit();

    db.begin();
    jaimeDoc = db.bindToSession(jaimeDoc);
    YTDocument tyrionDoc = new YTDocument("PersonTest");
    tyrionDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":[{\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}]}");
    tyrionDoc.save();
    db.commit();

    tyrionDoc = db.bindToSession(tyrionDoc);
    List<Map<String, YTIdentifiable>> res = tyrionDoc.field("emergency_contact");
    Map<String, YTIdentifiable> doc = res.get(0);
    Assert.assertTrue(doc.get("contact").getIdentity().isValid());

    reOpen("admin", "adminpwd");
    try (YTResultSet result = db.query("select from " + tyrionDoc.getIdentity())) {
      res = result.next().getProperty("emergency_contact");
      doc = res.get(0);
      Assert.assertTrue(doc.get("contact").getIdentity().isValid());
    }
  }
}
