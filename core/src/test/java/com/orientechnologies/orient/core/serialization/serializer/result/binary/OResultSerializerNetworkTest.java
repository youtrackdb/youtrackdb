package com.orientechnologies.orient.core.serialization.serializer.result.binary;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OResultSerializerNetworkTest {

  @Test
  public void test() {
    try (var orientDB = new OxygenDB("memory", OxygenDBConfig.defaultConfig())) {
      orientDB.createIfNotExists("test", ODatabaseType.MEMORY, "admin", "admin", "admin");
      try (var db = (ODatabaseSessionInternal) orientDB.open("test", "admin", "admin")) {
        OResultSerializerNetwork serializer = new OResultSerializerNetwork();

        OResultInternal original = new OResultInternal(db);
        original.setProperty("string", "foo");
        original.setProperty("integer", 12);
        original.setProperty("float", 12.4f);
        original.setProperty("double", 12.4d);
        original.setProperty("boolean", true);
        original.setProperty("rid", new ORecordId("#12:0"));

        OResultInternal embeddedProj = new OResultInternal(db);
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
        OResultInternal deserialized = serializer.deserialize(db, bytes);
        Assert.assertEquals(original, deserialized);
      }
    }
  }
}
