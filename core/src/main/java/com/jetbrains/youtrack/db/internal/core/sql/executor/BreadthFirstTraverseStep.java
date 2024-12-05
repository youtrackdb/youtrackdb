package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OInteger;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OTraverseProjectionItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OWhereClause;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class BreadthFirstTraverseStep extends AbstractTraverseStep {

  public BreadthFirstTraverseStep(
      List<OTraverseProjectionItem> projections,
      OWhereClause whileClause,
      OInteger maxDepth,
      CommandContext ctx,
      boolean profilingEnabled) {
    super(projections, whileClause, maxDepth, ctx, profilingEnabled);
  }

  @Override
  protected void fetchNextEntryPoints(
      ExecutionStream nextN, CommandContext ctx, List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    // Doing max batch of 100 entry points for now
    while (nextN.hasNext(ctx) && entryPoints.size() < 100) {
      YTResult item = toTraverseResult(ctx.getDatabase(), nextN.next(ctx));
      if (item != null) {
        List<YTRID> stack = new ArrayList<>();
        item.getIdentity().ifPresent(stack::add);
        ((YTResultInternal) item).setMetadata("$stack", stack);
        List<YTIdentifiable> path = new ArrayList<>();
        path.add(item.getIdentity().get());
        ((YTResultInternal) item).setMetadata("$path", path);
        if (item.isEntity() && !traversed.contains(item.getEntity().get().getIdentity())) {
          tryAddEntryPoint(item, ctx, entryPoints, traversed);
        }
      }
    }
  }

  private YTResult toTraverseResult(YTDatabaseSessionInternal db, YTResult item) {
    YTTraverseResult res = null;
    if (item instanceof YTTraverseResult) {
      res = (YTTraverseResult) item;
    } else if (item.isEntity() && item.getEntity().get().getIdentity().isPersistent()) {
      res = new YTTraverseResult(db, item.getEntity().get());
      res.depth = 0;
      res.setMetadata("$depth", 0);
    } else if (item.getPropertyNames().size() == 1) {
      Object val = item.getProperty(item.getPropertyNames().iterator().next());
      if (val instanceof YTIdentifiable) {
        res = new YTTraverseResult(db, (YTIdentifiable) val);
        res.depth = 0;
        res.setMetadata("$depth", 0);
      }
    } else {
      res = new YTTraverseResult(db);
      for (String key : item.getPropertyNames()) {
        res.setProperty(key, item.getProperty(key));
      }
      for (String md : item.getMetadataKeys()) {
        res.setMetadata(md, item.getMetadata(md));
      }
    }

    return res;
  }

  @Override
  protected void fetchNextResults(
      CommandContext ctx, List<YTResult> results, List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (!entryPoints.isEmpty()) {
      YTTraverseResult item = (YTTraverseResult) entryPoints.remove(0);
      results.add(item);
      for (OTraverseProjectionItem proj : projections) {
        Object nextStep = proj.execute(item, ctx);
        if (this.maxDepth == null || this.maxDepth.getValue().intValue() > item.depth) {
          addNextEntryPoints(
              nextStep,
              item.depth + 1,
              (List<YTIdentifiable>) item.getMetadata("$path"),
              ctx,
              entryPoints,
              traversed);
        }
      }
    }
  }

  private void addNextEntryPoints(
      Object nextStep,
      int depth,
      List<YTIdentifiable> path,
      CommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (nextStep instanceof YTIdentifiable) {
      addNextEntryPoints(((YTIdentifiable) nextStep), depth, path, ctx, entryPoints, traversed);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(
          ((Iterable) nextStep).iterator(), depth, path, ctx, entryPoints, traversed);
    } else if (nextStep instanceof Map) {
      addNextEntryPoints(
          ((Map) nextStep).values().iterator(), depth, path, ctx, entryPoints, traversed);
    } else if (nextStep instanceof YTResult) {
      addNextEntryPoints(((YTResult) nextStep), depth, path, ctx, entryPoints, traversed);
    }
  }

  private void addNextEntryPoints(
      Iterator nextStep,
      int depth,
      List<YTIdentifiable> path,
      CommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, path, ctx, entryPoints, traversed);
    }
  }

  private void addNextEntryPoints(
      YTIdentifiable nextStep,
      int depth,
      List<YTIdentifiable> path,
      CommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (traversed.contains(nextStep.getIdentity())) {
      return;
    }
    YTTraverseResult res = new YTTraverseResult(ctx.getDatabase(), nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);

    List<YTIdentifiable> newPath = new ArrayList<>();
    newPath.addAll(path);
    newPath.add(res.getIdentity().get());
    res.setMetadata("$path", newPath);

    List reverseStack = new ArrayList();
    reverseStack.addAll(newPath);
    Collections.reverse(reverseStack);
    List newStack = new ArrayList();
    newStack.addAll(reverseStack);
    res.setMetadata("$stack", newStack);

    tryAddEntryPoint(res, ctx, entryPoints, traversed);
  }

  private void addNextEntryPoints(
      YTResult nextStep,
      int depth,
      List<YTIdentifiable> path,
      CommandContext ctx,
      List<YTResult> entryPoints,
      Set<YTRID> traversed) {
    if (!nextStep.isEntity()) {
      return;
    }
    if (traversed.contains(nextStep.getEntity().get().getIdentity())) {
      return;
    }
    if (nextStep instanceof YTTraverseResult) {
      ((YTTraverseResult) nextStep).depth = depth;
      ((YTTraverseResult) nextStep).setMetadata("$depth", depth);

      List<YTIdentifiable> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(nextStep.getIdentity().get());
      ((YTTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      List newStack = new ArrayList();
      newStack.addAll(reverseStack);
      ((YTTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(nextStep, ctx, entryPoints, traversed);
    } else {
      YTTraverseResult res = new YTTraverseResult(ctx.getDatabase(), nextStep.getEntity().get());
      res.depth = depth;
      res.setMetadata("$depth", depth);

      List<YTIdentifiable> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(nextStep.getIdentity().get());
      ((YTTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      ArrayDeque newStack = new ArrayDeque();
      newStack.addAll(reverseStack);
      ((YTTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(res, ctx, entryPoints, traversed);
    }
  }

  private void tryAddEntryPoint(
      YTResult res, CommandContext ctx, List<YTResult> entryPoints, Set<YTRID> traversed) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      entryPoints.add(res);
    }
    traversed.add(res.getEntity().get().getIdentity());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ BREADTH-FIRST TRAVERSE \n");
    if (whileClause != null) {
      result.append(spaces);
      result.append("WHILE " + whileClause);
    }
    return result.toString();
  }
}
