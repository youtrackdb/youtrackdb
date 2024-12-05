package com.orientechnologies.common.function;

import com.orientechnologies.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface TxConsumer {

  void accept(final OAtomicOperation atomicOperation) throws Exception;
}
