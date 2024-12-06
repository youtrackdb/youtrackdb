package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Property;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder.Operation;
import java.util.List;
import java.util.Optional;

public interface IndexCandidate {

  String getName();

  Optional<IndexCandidate> invert();

  Operation getOperation();

  Optional<IndexCandidate> normalize(CommandContext ctx);

  List<Property> properties();
}
