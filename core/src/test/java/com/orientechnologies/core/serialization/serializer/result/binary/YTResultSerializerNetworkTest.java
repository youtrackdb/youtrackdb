package com.orientechnologies.core.serialization.serializer.result.binary;

import com.orientechnologies.core.db.ODatabaseType;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.db.YouTrackDBConfig;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.core.serialization.serializer.result.binary.OResultSerializerNetwork;
import com.orientechnologies.core.sql.executor.YTResultInternal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class YTResultSerializerNetworkTest {

  @Test
  public void test() {
    try (var orientDB = new YouTrackDB("memory", YouTrackDBConfig.defaultConfig())) {
      orientDB.createIfNotExists("test", ODatabaseType.MEMORY, "admin", "admin", "admin");
      try (var db = (YTDatabaseSessionInternal) orientDB.open("test", "admin", "admin")) {
        OResultSerializerNetwork serializer = new OResultSerializerNetwork();

        YTResultInternal original = new YTResultInternal(db);
        original.setProperty("string", "foo");
        original.setProperty("integer", 12);
        original.setProperty("float", 12.4f);
        original.setProperty("double", 12.4d);
        original.setProperty("boolean", true);
        original.setProperty("rid", new YTRecordId("#12:0"));

        YTResultInternal embeddedProj = new YTResultInternal(db);
        embeddedProj.setProperty("name", "bar");
        original.setProperty("embeddedProj", embeddedProj);

        List list = new ArrayList();
        list.add("foo");
        list.add("bar");
        original.setProperty("list", list);

        Set set = new HashSet<>();
        set.add("foox");
        set.add("barx");
        original.setProperty("set", "set");

        BytesContainer bytes = new BytesContainer();
        serializer.serialize(original, bytes);

        bytes.offset = 0;
        YTResultInternal deserialized = serializer.deserialize(db, bytes);
        Assert.assertEquals(original, deserialized);
      }
    }
  }
}
