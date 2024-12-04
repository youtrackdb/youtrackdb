package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.OList;
import com.orientechnologies.orient.core.db.record.OMap;
import com.orientechnologies.orient.core.db.record.OSet;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import java.util.Set;
import org.apache.commons.collections4.SetUtils;
import org.junit.Test;

public class CollectionOfLinkInNestedDocumentTest extends DBTestBase {

  @Test
  public void nestedLinkSet() {
    YTDocument doc1 = new YTDocument();
    doc1.field("value", "item 1");
    YTDocument doc2 = new YTDocument();
    doc2.field("value", "item 2");
    YTDocument nested = new YTDocument();
    OSet set = new OSet(nested);
    set.add(doc1);
    set.add(doc2);

    nested.field("set", set);

    db.begin();
    YTDocument base = new YTDocument();
    base.field("nested", nested, YTType.EMBEDDED);
    YTIdentifiable id = db.save(base);
    db.commit();

    YTDocument base1 = db.load(id.getIdentity());
    YTDocument nest1 = base1.field("nested");
    assertNotNull(nest1);

    assertEquals(SetUtils.hashSet(doc1.getIdentity(), doc2.getIdentity()),
        nest1.<Set<YTIdentifiable>>field("set"));
  }

  @Test
  public void nestedLinkList() {
    YTDocument doc1 = new YTDocument();
    doc1.field("value", "item 1");
    YTDocument doc2 = new YTDocument();
    doc2.field("value", "item 2");
    YTDocument nested = new YTDocument();
    OList list = new OList(nested);
    list.add(doc1);
    list.add(doc2);

    nested.field("list", list);

    db.begin();
    YTDocument base = new YTDocument();
    base.field("nested", nested, YTType.EMBEDDED);
    YTIdentifiable id = db.save(base, db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    YTDocument base1 = db.load(id.getIdentity());
    YTDocument nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(list, nest1.field("list"));
  }

  @Test
  public void nestedLinkMap() {
    YTDocument doc1 = new YTDocument();
    doc1.field("value", "item 1");
    YTDocument doc2 = new YTDocument();
    doc2.field("value", "item 2");
    YTDocument nested = new YTDocument();
    OMap map = new OMap(nested);
    map.put("record1", doc1);
    map.put("record2", doc2);

    nested.field("map", map);

    db.begin();
    YTDocument base = new YTDocument();
    base.field("nested", nested, YTType.EMBEDDED);
    YTIdentifiable id = db.save(base, db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    YTDocument base1 = db.load(id.getIdentity());
    YTDocument nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(map, nest1.field("map"));
  }
}
