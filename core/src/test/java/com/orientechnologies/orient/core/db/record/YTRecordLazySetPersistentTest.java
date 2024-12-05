package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class YTRecordLazySetPersistentTest extends DBTestBase {

  @Test
  public void test1() {
    YTRID orid1;
    YTRID orid2;

    db.activateOnCurrentThread();
    db.begin();
    {
      YTEntityImpl doc1 = new YTEntityImpl();
      doc1.field("linkset", new HashSet<YTEntityImpl>());
      Set<YTEntityImpl> linkset = doc1.field("linkset");
      YTEntityImpl doc2 = new YTEntityImpl();
      doc2.save(db.getClusterNameById(db.getDefaultClusterId()));
      orid2 = doc2.getIdentity();
      linkset.add(doc2);
      doc1.save(db.getClusterNameById(db.getDefaultClusterId()));
      orid1 = doc1.getIdentity();
      assertNotNull(orid1);
    }
    db.commit();

    db.begin();
    {
      YTEntityImpl doc1 = db.load(orid1);
      assertNotNull(doc1);
      Set<YTIdentifiable> linkset = doc1.field("linkset");
      assertNotNull(linkset);
      assertEquals(1, linkset.size());

      YTEntityImpl doc2 = db.load(orid2);
      assertNotNull(doc2);

      assertEquals(orid2, linkset.iterator().next().getIdentity());
      assertEquals(orid2, doc2.getIdentity());

      linkset.remove(doc2);
      assertEquals(0, linkset.size()); // AssertionError: expected:<0> but was:<1>
    }
    db.commit();
  }

  @Test
  public void test2() {
    YTRID orid1;
    YTRID orid2;

    db.activateOnCurrentThread();
    db.begin();
    {
      YTEntityImpl doc1 = new YTEntityImpl();
      doc1.field("linkset", new HashSet<YTIdentifiable>());
      Set<YTIdentifiable> linkset = doc1.field("linkset");
      YTEntityImpl doc2 = new YTEntityImpl();
      doc2.save(db.getClusterNameById(db.getDefaultClusterId()));
      orid2 = doc2.getIdentity();
      linkset.add(doc2);
      doc1.save(db.getClusterNameById(db.getDefaultClusterId()));
      orid1 = doc1.getIdentity();
      assertNotNull(orid1);
    }
    db.commit();

    db.begin();
    {
      YTEntityImpl doc1 = db.load(orid1);
      assertNotNull(doc1);
      Set<YTIdentifiable> linkset = doc1.field("linkset");

      assertNotNull(linkset);
      assertEquals(1, linkset.size());

      YTEntityImpl doc2 = db.load(orid2);
      assertNotNull(doc2);

      assertEquals(orid2, linkset.iterator().next().getIdentity());
      assertEquals(orid2, doc2.getIdentity());

      linkset.remove(doc2);
      assertEquals(0, linkset.size()); // AssertionError: expected:<0> but was:<1>
    }
    db.commit();
  }
}
