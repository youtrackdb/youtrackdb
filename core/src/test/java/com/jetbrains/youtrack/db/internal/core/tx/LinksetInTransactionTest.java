package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.record.Entity;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class LinksetInTransactionTest extends DbTestBase {

  @Test
  public void test() {

    db.createClass("WithLinks").createProperty(db, "links", PropertyType.LINKSET);
    db.createClass("Linked");

    db.begin();
    /* A link must already be there */
    var withLinks1 = db.newInstance("WithLinks");
    var link1 = db.newInstance("Linked");
    link1.save();
    Set set = new HashSet<>();
    set.add(link1);
    withLinks1.setProperty("links", set);
    withLinks1.save();
    db.commit();

    /* Only in transaction - without transaction all OK */
    db.begin();
    withLinks1 = db.bindToSession(withLinks1);
    link1 = db.bindToSession(link1);

    /* Add a new linked record */
    var link2 = db.newInstance("Linked");
    link2.save();
    Set links = withLinks1.getProperty("links");
    links.add(link2);
    withLinks1.save();

    /* Remove all from LinkSet - if only link2 removed all OK */
    links = withLinks1.getProperty("links");
    links.remove(link1);
    links = withLinks1.getProperty("links");
    links.remove(link2);
    withLinks1.save();

    /* All seems OK before commit */
    links = withLinks1.getProperty("links");
    Assert.assertEquals(0, links.size());
    links = withLinks1.getProperty("links");
    Assert.assertEquals(0, links.size());
    db.commit();

    withLinks1 = db.bindToSession(withLinks1);
    links = withLinks1.getProperty("links");
    /* Initial record was removed */
    Assert.assertFalse(links.contains(link1));
    /* Fails: why is link2 still in the set? */
    Assert.assertFalse(links.contains(link2));
  }
}
