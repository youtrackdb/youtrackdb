package com.orientechnologies.orient.object.enhancement;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.object.db.OObjectDatabaseTxInternal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 18.05.12
 */
public class OObjectEntitySerializerTest {

  private OObjectDatabaseTxInternal databaseTx;

  @Before
  public void setUp() throws Exception {
    databaseTx = new OObjectDatabaseTxInternal("memory:OObjectEntitySerializerTest");
    databaseTx.create();

    databaseTx.getEntityManager().registerEntityClass(ExactEntity.class);
  }

  @After
  public void tearDown() {
    databaseTx.drop();
  }

  @Test
  public void testCallbacksHierarchy() {
    databaseTx.begin();
    ExactEntity entity = new ExactEntity();
    databaseTx.save(entity);
    databaseTx.commit();

    assertTrue(entity.callbackExecuted());
  }

  @Test
  public void testCallbacksHierarchyUpdate() {
    databaseTx.begin();
    ExactEntity entity = new ExactEntity();
    entity = databaseTx.save(entity);
    databaseTx.commit();

    databaseTx.begin();
    entity.reset();
    databaseTx.save(entity);
    databaseTx.commit();

    assertTrue(entity.callbackExecuted());
  }
}
