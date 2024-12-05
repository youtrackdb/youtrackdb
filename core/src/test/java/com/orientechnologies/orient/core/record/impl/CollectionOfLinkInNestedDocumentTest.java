package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.LinkList;
import com.orientechnologies.orient.core.db.record.LinkMap;
import com.orientechnologies.orient.core.db.record.LinkSet;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import java.util.Set;
import org.apache.commons.collections4.SetUtils;
import org.junit.Test;

public class CollectionOfLinkInNestedDocumentTest extends DBTestBase {

  @Test
  public void nestedLinkSet() {
    YTEntityImpl doc1 = new YTEntityImpl();
    doc1.field("value", "item 1");
    YTEntityImpl doc2 = new YTEntityImpl();
    doc2.field("value", "item 2");
    YTEntityImpl nested = new YTEntityImpl();
    LinkSet set = new LinkSet(nested);
    set.add(doc1);
    set.add(doc2);

    nested.field("set", set);

    db.begin();
    YTEntityImpl base = new YTEntityImpl();
    base.field("nested", nested, YTType.EMBEDDED);
    YTIdentifiable id = db.save(base);
    db.commit();

    YTEntityImpl base1 = db.load(id.getIdentity());
    YTEntityImpl nest1 = base1.field("nested");
    assertNotNull(nest1);

    assertEquals(SetUtils.hashSet(doc1.getIdentity(), doc2.getIdentity()),
        nest1.<Set<YTIdentifiable>>field("set"));
  }

  @Test
  public void nestedLinkList() {
    YTEntityImpl doc1 = new YTEntityImpl();
    doc1.field("value", "item 1");
    YTEntityImpl doc2 = new YTEntityImpl();
    doc2.field("value", "item 2");
    YTEntityImpl nested = new YTEntityImpl();
    LinkList list = new LinkList(nested);
    list.add(doc1);
    list.add(doc2);

    nested.field("list", list);

    db.begin();
    YTEntityImpl base = new YTEntityImpl();
    base.field("nested", nested, YTType.EMBEDDED);
    YTIdentifiable id = db.save(base, db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    YTEntityImpl base1 = db.load(id.getIdentity());
    YTEntityImpl nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(list, nest1.field("list"));
  }

  @Test
  public void nestedLinkMap() {
    YTEntityImpl doc1 = new YTEntityImpl();
    doc1.field("value", "item 1");
    YTEntityImpl doc2 = new YTEntityImpl();
    doc2.field("value", "item 2");
    YTEntityImpl nested = new YTEntityImpl();
    LinkMap map = new LinkMap(nested);
    map.put("record1", doc1);
    map.put("record2", doc2);

    nested.field("map", map);

    db.begin();
    YTEntityImpl base = new YTEntityImpl();
    base.field("nested", nested, YTType.EMBEDDED);
    YTIdentifiable id = db.save(base, db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    YTEntityImpl base1 = db.load(id.getIdentity());
    YTEntityImpl nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(map, nest1.field("map"));
  }
}
