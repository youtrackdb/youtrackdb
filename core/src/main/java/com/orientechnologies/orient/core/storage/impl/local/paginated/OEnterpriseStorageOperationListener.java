package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import java.util.List;

/**
 *
 */
public interface OEnterpriseStorageOperationListener {

  void onCommit(List<ORecordOperation> operations);

  void onRollback();

  void onRead();
}
