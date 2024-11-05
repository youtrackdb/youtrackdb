package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;

/** @author Wouter de Vaal */
public class OLazyCollectionUtil {

  /** Gets the current thread database as a ODatabasePojoAbstract, wrapping it where necessary. */
  protected static OObjectDatabaseTxInternal getDatabase() {
    ODatabaseInternal<?> databaseOwner =
        ODatabaseRecordThreadLocal.instance().get().getDatabaseOwner();
    if (databaseOwner instanceof OObjectDatabaseTxInternal) {
      return (OObjectDatabaseTxInternal) databaseOwner;
    } else if (databaseOwner instanceof ODatabaseSessionInternal) {
      return new OObjectDatabaseTxInternal((ODatabaseSessionInternal) databaseOwner);
    }
    throw new IllegalStateException("Current database not of expected type");
  }
}
