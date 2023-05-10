/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.samza.metrics;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.samza.runtime.KeyedTimerData;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SamzaOutputGBKMetricOp is a {@link Op} that emits & maintains default metrics for output
 * PCollection for GroupByKey. It emits the output throughput and maintains avg output time for
 * output PCollection per windowId (). It is also responsible for emitting latency metric per
 * windowId once the watermark passes the end of window timestamp.
 *
 * <p>Assumes that {@code SamzaOutputGBKMetricOp#processWatermark(Instant, OpEmitter)} is exclusive
 * of {@code SamzaOutputGBKMetricOp#processElement(Instant, OpEmitter)}. Specifically, the
 * processWatermark method assumes that no calls to processElement will be made during its
 * execution, and vice versa.
 *
 * @param <T> The type of the elements in the output PCollection.
 */
public class SamzaOutputGBKMetricOp<T> implements Op<T, T, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOutputGBKMetricOp.class);
  // Unique name of the PTransform this MetricOp is associated with
  private final String transformFullName;
  private final SamzaTransformMetricRegistry samzaTransformMetricRegistry;
  // Name or identifier of the PCollection which PTransform is processing
  private final String pValue;
  // Counters for keeping sum of arrival time and count of elements per windowId
  @SuppressFBWarnings("SE_BAD_FIELD")
  private final ConcurrentHashMap<BoundedWindow, BigInteger> sumOfTimestampsPerWindowId;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final ConcurrentHashMap<BoundedWindow, Long> sumOfCountPerWindowId;
  // Name of the task, for logging purpose
  private transient String task;

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<Void>> timerRegistry,
      OpEmitter<T> emitter) {
    // for logging / debugging purposes
    this.task = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    // Register the transform with SamzaTransformMetricRegistry
    samzaTransformMetricRegistry.register(transformFullName, pValue, context);
  }

  // Some fields are initialized in open() method, which is called after the constructor.
  @SuppressWarnings("initialization.fields.uninitialized")
  public SamzaOutputGBKMetricOp(
      String pValue,
      String transformFullName,
      SamzaTransformMetricRegistry samzaTransformMetricRegistry) {
    this.pValue = pValue;
    this.transformFullName = transformFullName;
    this.samzaTransformMetricRegistry = samzaTransformMetricRegistry;
    this.sumOfTimestampsPerWindowId = new ConcurrentHashMap<>();
    this.sumOfCountPerWindowId = new ConcurrentHashMap<>();
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    // one element can belong to multiple windows
    for (BoundedWindow windowId : inputElement.getWindows()) {
      // Atomic updates to counts
      sumOfCountPerWindowId.compute(
          windowId,
          (key, value) -> {
            value = value == null ? Long.valueOf(0) : value;
            return ++value;
          });
      // Atomic updates to sum of arrival timestamps
      sumOfTimestampsPerWindowId.compute(
          windowId,
          (key, value) -> {
            value = value == null ? BigInteger.ZERO : value;
            return value.add(BigInteger.valueOf(System.nanoTime()));
          });
    }
    // update the output throughput metric
    samzaTransformMetricRegistry
        .getTransformMetrics()
        .getTransformOutputThroughput(transformFullName)
        .inc();
    emitter.emitElement(inputElement);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    final List<BoundedWindow> closedWindows = new ArrayList<>();
    sumOfCountPerWindowId.keySet().stream()
        .filter(windowId -> watermark.isAfter(windowId.maxTimestamp())) // window is closed
        .forEach(
            windowId -> {
              // In case if BigInteger overflows for long we only retain the last 64 bits of the sum
              long sumOfTimestamps =
                  sumOfTimestampsPerWindowId.get(windowId) != null
                      ? sumOfTimestampsPerWindowId.get(windowId).longValue()
                      : 0L;
              long count = sumOfCountPerWindowId.get(windowId);
              closedWindows.add(windowId);

              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Processing Output Watermark for Transform: {}, WindowId:{}, count: {}, sumOfTimestamps: {}, task: {}",
                    transformFullName,
                    windowId,
                    count,
                    sumOfTimestamps,
                    task);
              }

              // Compute the latency if there is some data for the window
              if (sumOfTimestamps > 0 && count > 0) {
                samzaTransformMetricRegistry.emitLatencyMetric(
                    transformFullName, windowId, Math.floorDiv(sumOfTimestamps, count), task);
              }
            });

    // Clean up the closed windows
    sumOfCountPerWindowId.keySet().removeAll(closedWindows);
    sumOfTimestampsPerWindowId.keySet().removeAll(closedWindows);

    // update the watermark progress metric
    samzaTransformMetricRegistry
        .getTransformMetrics()
        .getTransformWatermarkProgress(transformFullName)
        .set(watermark.getMillis());

    emitter.emitWatermark(watermark);
  }
}
