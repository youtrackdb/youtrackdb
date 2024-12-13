package com.jetbrains.youtrack.db.internal.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class RecordLazySetPersistentTest extends DbTestBase {

  @Test
  public void test1() {
    RID orid1;
    RID orid2;

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
      Set<Identifiable> linkset = doc1.field("linkset");
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
    RID orid1;
    RID orid2;

    db.activateOnCurrentThread();
    db.begin();
    {
      EntityImpl doc1 = new EntityImpl();
      doc1.field("linkset", new HashSet<Identifiable>());
      Set<Identifiable> linkset = doc1.field("linkset");
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
      Set<Identifiable> linkset = doc1.field("linkset");

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
