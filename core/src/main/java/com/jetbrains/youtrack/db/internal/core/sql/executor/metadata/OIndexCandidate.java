package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.List;
import java.util.Optional;

public interface OIndexCandidate {

  String getName();

  Optional<OIndexCandidate> invert();

  Operation getOperation();

  Optional<OIndexCandidate> normalize(OCommandContext ctx);

  List<YTProperty> properties();
}
