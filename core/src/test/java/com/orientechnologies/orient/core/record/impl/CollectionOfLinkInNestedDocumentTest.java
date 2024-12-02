package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OList;
import com.orientechnologies.orient.core.db.record.OMap;
import com.orientechnologies.orient.core.db.record.OSet;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.Set;
import org.apache.commons.collections4.SetUtils;
import org.junit.Test;

public class CollectionOfLinkInNestedDocumentTest extends BaseMemoryDatabase {

  @Test
  public void nestedLinkSet() {
    ODocument doc1 = new ODocument();
    doc1.field("value", "item 1");
    ODocument doc2 = new ODocument();
    doc2.field("value", "item 2");
    ODocument nested = new ODocument();
    OSet set = new OSet(nested);
    set.add(doc1);
    set.add(doc2);

    nested.field("set", set);

    db.begin();
    ODocument base = new ODocument();
    base.field("nested", nested, OType.EMBEDDED);
    OIdentifiable id = db.save(base);
    db.commit();

    ODocument base1 = db.load(id.getIdentity());
    ODocument nest1 = base1.field("nested");
    assertNotNull(nest1);

    assertEquals(SetUtils.hashSet(doc1.getIdentity(), doc2.getIdentity()),
        nest1.<Set<OIdentifiable>>field("set"));
  }

  @Test
  public void nestedLinkList() {
    ODocument doc1 = new ODocument();
    doc1.field("value", "item 1");
    ODocument doc2 = new ODocument();
    doc2.field("value", "item 2");
    ODocument nested = new ODocument();
    OList list = new OList(nested);
    list.add(doc1);
    list.add(doc2);

    nested.field("list", list);

    db.begin();
    ODocument base = new ODocument();
    base.field("nested", nested, OType.EMBEDDED);
    OIdentifiable id = db.save(base, db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    ODocument base1 = db.load(id.getIdentity());
    ODocument nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(list, nest1.field("list"));
  }

  @Test
  public void nestedLinkMap() {
    ODocument doc1 = new ODocument();
    doc1.field("value", "item 1");
    ODocument doc2 = new ODocument();
    doc2.field("value", "item 2");
    ODocument nested = new ODocument();
    OMap map = new OMap(nested);
    map.put("record1", doc1);
    map.put("record2", doc2);

    nested.field("map", map);

    db.begin();
    ODocument base = new ODocument();
    base.field("nested", nested, OType.EMBEDDED);
    OIdentifiable id = db.save(base, db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    ODocument base1 = db.load(id.getIdentity());
    ODocument nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(map, nest1.field("map"));
  }
}
