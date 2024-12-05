package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import java.util.List;

/**
 *
 */
public interface OEnterpriseStorageOperationListener {

  void onCommit(List<ORecordOperation> operations);

  void onRollback();

  void onRead();
}
