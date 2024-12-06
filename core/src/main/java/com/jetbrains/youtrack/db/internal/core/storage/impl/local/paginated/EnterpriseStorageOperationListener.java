package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import java.util.List;

/**
 *
 */
public interface EnterpriseStorageOperationListener {

  void onCommit(List<RecordOperation> operations);

  void onRollback();

  void onRead();
}
