package com.jetbrains.youtrack.db.internal.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class RecordLazySetPersistentTest extends DBTestBase {

  @Test
  public void test1() {
    YTRID orid1;
    YTRID orid2;

    db.activateOnCurrentThread();
    db.begin();
    {
      EntityImpl doc1 = new EntityImpl();
      doc1.field("linkset", new HashSet<EntityImpl>());
      Set<EntityImpl> linkset = doc1.field("linkset");
      EntityImpl doc2 = new EntityImpl();
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
      EntityImpl doc1 = db.load(orid1);
      assertNotNull(doc1);
      Set<YTIdentifiable> linkset = doc1.field("linkset");
      assertNotNull(linkset);
      assertEquals(1, linkset.size());

      EntityImpl doc2 = db.load(orid2);
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
      EntityImpl doc1 = new EntityImpl();
      doc1.field("linkset", new HashSet<YTIdentifiable>());
      Set<YTIdentifiable> linkset = doc1.field("linkset");
      EntityImpl doc2 = new EntityImpl();
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
      EntityImpl doc1 = db.load(orid1);
      assertNotNull(doc1);
      Set<YTIdentifiable> linkset = doc1.field("linkset");

      assertNotNull(linkset);
      assertEquals(1, linkset.size());

      EntityImpl doc2 = db.load(orid2);
      assertNotNull(doc2);

      assertEquals(orid2, linkset.iterator().next().getIdentity());
      assertEquals(orid2, doc2.getIdentity());

      linkset.remove(doc2);
      assertEquals(0, linkset.size()); // AssertionError: expected:<0> but was:<1>
    }
    db.commit();
  }
}
