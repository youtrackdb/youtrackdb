package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class BaseMemoryInternalDatabase {

  protected ODatabaseSessionInternal db;
  protected OxygenDB context;
  @Rule
  public TestName name = new TestName();

  @Before
  public void beforeTest() {
    context = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig());
    context
        .execute(
            "create database "
                + name.getMethodName()
                + " memory users(admin identified by 'adminpwd' role admin) ")
        .close();
    db = (ODatabaseSessionInternal) context.open(name.getMethodName(), "admin", "adminpwd");
  }

  @After
  public void afterTest() {
    db.close();
    context.drop(name.getMethodName());
    context.close();
  }
}
