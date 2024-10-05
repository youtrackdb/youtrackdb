package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

/** @author Wouter de Vaal */
public class OLazyCollectionUtil {

  /** Gets the current thread database as a ODatabasePojoAbstract, wrapping it where necessary. */
  protected static OObjectDatabaseTxInternal getDatabase() {
    ODatabaseInternal<?> databaseOwner =
        ODatabaseRecordThreadLocal.instance().get().getDatabaseOwner();
    if (databaseOwner instanceof OObjectDatabaseTxInternal) {
      return (OObjectDatabaseTxInternal) databaseOwner;
    } else if (databaseOwner instanceof ODatabaseDocumentInternal) {
      return new OObjectDatabaseTxInternal((ODatabaseDocumentInternal) databaseOwner);
    }
    throw new IllegalStateException("Current database not of expected type");
  }
}
