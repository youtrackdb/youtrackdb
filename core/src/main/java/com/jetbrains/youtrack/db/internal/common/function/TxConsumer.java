package com.jetbrains.youtrack.db.internal.common.function;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;

public interface TxConsumer {

  void accept(final AtomicOperation atomicOperation) throws Exception;
}
