package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.record.Entity;
import java.util.Arrays;
import java.util.Random;
import org.junit.Test;

public class CompositeIndexGrowShrinkIT extends DbTestBase {

  private final Random random = new Random();

  public String randomText() {
    String str = "";
    int count = random.nextInt(10);
    for (int i = 0; i < count; i++) {
      str += random.nextInt(10000) + " ";
    }
    return str;
  }

  @Test
  public void testCompositeGrowShirnk() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("CompositeIndex");
    clazz.createProperty(db, "id", PropertyType.INTEGER);
    clazz.createProperty(db, "bar", PropertyType.INTEGER);
    clazz.createProperty(db, "tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    clazz.createProperty(db, "name", PropertyType.STRING);

    db.command(
            "create index CompositeIndex_id_tags_name on CompositeIndex (id, tags, name) NOTUNIQUE")
        .close();
    for (int i = 0; i < 150000; i++) {
      Entity rec = db.newEntity("CompositeIndex");
      rec.setProperty("id", i);
      rec.setProperty("bar", i);
      rec.setProperty(
          "tags",
          Arrays.asList(
              "soem long and more complex tezxt just un case it may be important", "two"));
      rec.setProperty("name", "name" + i);
      rec.save();
    }
    db.command("delete from CompositeIndex").close();
  }

  @Test
  public void testCompositeGrowDrop() {

    final Schema schema = db.getMetadata().getSchema();
    SchemaClass clazz = schema.createClass("CompositeIndex");
    clazz.createProperty(db, "id", PropertyType.INTEGER);
    clazz.createProperty(db, "bar", PropertyType.INTEGER);
    clazz.createProperty(db, "tags", PropertyType.EMBEDDEDLIST, PropertyType.STRING);
    clazz.createProperty(db, "name", PropertyType.STRING);

    db.command(
            "create index CompositeIndex_id_tags_name on CompositeIndex (id, tags, name) NOTUNIQUE")
        .close();

    for (int i = 0; i < 150000; i++) {
      Entity rec = db.newEntity("CompositeIndex");
      rec.setProperty("id", i);
      rec.setProperty("bar", i);
      rec.setProperty(
          "tags",
          Arrays.asList(
              "soem long and more complex tezxt just un case it may be important", "two"));
      rec.setProperty("name", "name" + i);
      rec.save();
    }
    db.command("drop index CompositeIndex_id_tags_name").close();
  }
}
