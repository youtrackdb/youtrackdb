package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public class CartesianProductStep extends AbstractExecutionStep {

  private final List<OInternalExecutionPlan> subPlans = new ArrayList<>();

  public CartesianProductStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Stream<YTResult[]> stream = null;
    YTResult[] productTuple = new YTResult[this.subPlans.size()];

    for (int i = 0; i < this.subPlans.size(); i++) {
      OInternalExecutionPlan ep = this.subPlans.get(i);
      final int pos = i;
      if (stream == null) {
        ExecutionStream es = ep.start();
        stream =
            es.stream(ctx)
                .map(
                    (value) -> {
                      productTuple[pos] = value;
                      return productTuple;
                    })
                .onClose(() -> es.close(ctx));
      } else {
        stream =
            stream.flatMap(
                (val) -> {
                  ExecutionStream es = ep.start();
                  return es.stream(ctx)
                      .map(
                          (value) -> {
                            val[pos] = value;
                            return val;
                          })
                      .onClose(() -> es.close(ctx));
                });
      }
    }
    assert stream != null;
    var db = ctx.getDatabase();
    Stream<YTResult> finalStream = stream.map(path -> produceResult(db, path));
    return ExecutionStream.resultIterator(finalStream.iterator())
        .onClose((context) -> finalStream.close());
  }

  private static YTResult produceResult(YTDatabaseSessionInternal db, YTResult[] path) {

    YTResultInternal nextRecord = new YTResultInternal(db);

    for (YTResult res : path) {
      for (String s : res.getPropertyNames()) {
        nextRecord.setProperty(s, res.getProperty(s));
      }
    }
    return nextRecord;
  }

  public void addSubPlan(OInternalExecutionPlan subPlan) {
    this.subPlans.add(subPlan);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    String ind = ExecutionStepInternal.getIndent(depth, indent);

    int[] blockSizes = new int[subPlans.size()];

    for (int i = 0; i < subPlans.size(); i++) {
      OInternalExecutionPlan currentPlan = subPlans.get(subPlans.size() - 1 - i);
      String partial = currentPlan.prettyPrint(0, indent);

      String[] partials = partial.split("\n");
      blockSizes[subPlans.size() - 1 - i] = partials.length + 2;
      result.insert(0, "+-------------------------\n");
      for (int j = 0; j < partials.length; j++) {
        String p = partials[partials.length - 1 - j];
        if (!result.isEmpty()) {
          result.insert(0, appendPipe(p) + "\n");
        } else {
          result = new StringBuilder(appendPipe(p));
        }
      }
      result.insert(0, "+-------------------------\n");
    }
    result = new StringBuilder(addArrows(result.toString(), blockSizes));
    result.append(foot(blockSizes));
    result.insert(0, ind);
    result = new StringBuilder(result.toString().replaceAll("\n", "\n" + ind));
    result.insert(0, head(depth, indent) + "\n");
    return result.toString();
  }

  private String addArrows(String input, int[] blockSizes) {
    StringBuilder result = new StringBuilder();
    String[] rows = input.split("\n");
    int rowNum = 0;
    for (int block = 0; block < blockSizes.length; block++) {
      int blockSize = blockSizes[block];
      for (int subRow = 0; subRow < blockSize; subRow++) {
        for (int col = 0; col < blockSizes.length * 3; col++) {
          if (isHorizontalRow(col, subRow, block, blockSize)) {
            result.append("-");
          } else if (isPlus(col, subRow, block, blockSize)) {
            result.append("+");
          } else if (isVerticalRow(col, subRow, block, blockSize)) {
            result.append("|");
          } else {
            result.append(" ");
          }
        }
        result.append(rows[rowNum]);
        result.append("\n");
        rowNum++;
      }
    }

    return result.toString();
  }

  private boolean isHorizontalRow(int col, int subRow, int block, int blockSize) {
    if (col < block * 3 + 2) {
      return false;
    }
    return subRow == blockSize / 2;
  }

  private boolean isPlus(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      return subRow == blockSize / 2;
    }
    return false;
  }

  private boolean isVerticalRow(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      return subRow > blockSize / 2;
    } else {
      return col < block * 3 + 1 && col % 3 == 1;
    }
  }

  private String head(int depth, int indent) {
    String ind = ExecutionStepInternal.getIndent(depth, indent);
    String result = ind + "+ CARTESIAN PRODUCT";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  private String foot(int[] blockSizes) {
    return " V ".repeat(blockSizes.length);
  }

  private String appendPipe(String p) {
    return "| " + p;
  }
}
