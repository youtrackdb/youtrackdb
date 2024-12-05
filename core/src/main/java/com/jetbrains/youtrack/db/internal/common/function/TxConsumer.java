package com.jetbrains.youtrack.db.internal.common.function;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface TxConsumer {

  void accept(final OAtomicOperation atomicOperation) throws Exception;
}
