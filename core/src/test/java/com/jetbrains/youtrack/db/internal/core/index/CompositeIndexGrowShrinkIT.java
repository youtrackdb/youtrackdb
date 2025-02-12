package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.Arrays;
import java.util.Random;
import org.junit.Test;

public class CompositeIndexGrowShrinkIT extends DbTestBase {

  private final Random random = new Random();

  public String randomText() {
    var str = "";
    var count = random.nextInt(10);
    for (var i = 0; i < count; i++) {
      str += random.nextInt(10000) + " ";
    }
    return str;
  }

  @Test
  public void testCompositeGrowShirnk() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("CompositeIndex");
    clazz.createProperty(session, "id", PropertyType.INTEGER);
    clazz.createProperty(session, "bar", PropertyType.INTEGER);
    clazz.createProperty(session, "tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    clazz.createProperty(session, "name", PropertyType.STRING);

    session.command(
            "create index CompositeIndex_id_tags_name on CompositeIndex (id, tags, name) NOTUNIQUE")
        .close();
    for (var i = 0; i < 150000; i++) {
      var rec = session.newEntity("CompositeIndex");
      rec.setProperty("id", i);
      rec.setProperty("bar", i);
      rec.setProperty(
          "tags",
          Arrays.asList(
              "soem long and more complex tezxt just un case it may be important", "two"));
      rec.setProperty("name", "name" + i);
      rec.save();
    }
    session.command("delete from CompositeIndex").close();
  }

  @Test
  public void testCompositeGrowDrop() {

    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("CompositeIndex");
    clazz.createProperty(session, "id", PropertyType.INTEGER);
    clazz.createProperty(session, "bar", PropertyType.INTEGER);
    clazz.createProperty(session, "tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    clazz.createProperty(session, "name", PropertyType.STRING);

    session.command(
            "create index CompositeIndex_id_tags_name on CompositeIndex (id, tags, name) NOTUNIQUE")
        .close();

    for (var i = 0; i < 150000; i++) {
      var rec = session.newEntity("CompositeIndex");
      rec.setProperty("id", i);
      rec.setProperty("bar", i);
      rec.setProperty(
          "tags",
          Arrays.asList(
              "soem long and more complex tezxt just un case it may be important", "two"));
      rec.setProperty("name", "name" + i);
      rec.save();
    }
    session.command("drop index CompositeIndex_id_tags_name").close();
  }
}
