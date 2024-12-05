package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.List;
import java.util.Optional;

public interface OIndexCandidate {

  String getName();

  Optional<OIndexCandidate> invert();

  Operation getOperation();

  Optional<OIndexCandidate> normalize(CommandContext ctx);

  List<YTProperty> properties();
}
