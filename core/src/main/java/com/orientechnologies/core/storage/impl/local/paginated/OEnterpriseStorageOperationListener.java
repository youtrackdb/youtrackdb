package com.orientechnologies.core.storage.impl.local.paginated;

import com.orientechnologies.core.db.record.ORecordOperation;
import java.util.List;

/**
 *
 */
public interface OEnterpriseStorageOperationListener {

  void onCommit(List<ORecordOperation> operations);

  void onRollback();

  void onRead();
}
