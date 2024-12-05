package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import java.util.Arrays;
import java.util.Random;
import org.junit.Test;

public class OCompositeIndexGrowShrinkIT extends DBTestBase {

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
    final YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.createClass("CompositeIndex");
    clazz.createProperty(db, "id", YTType.INTEGER);
    clazz.createProperty(db, "bar", YTType.INTEGER);
    clazz.createProperty(db, "tags", YTType.EMBEDDEDLIST, YTType.STRING);
    clazz.createProperty(db, "name", YTType.STRING);

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

    final YTSchema schema = db.getMetadata().getSchema();
    YTClass clazz = schema.createClass("CompositeIndex");
    clazz.createProperty(db, "id", YTType.INTEGER);
    clazz.createProperty(db, "bar", YTType.INTEGER);
    clazz.createProperty(db, "tags", YTType.EMBEDDEDLIST, YTType.STRING);
    clazz.createProperty(db, "name", YTType.STRING);

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
