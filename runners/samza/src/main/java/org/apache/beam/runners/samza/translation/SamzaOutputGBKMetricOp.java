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
package org.apache.beam.runners.samza.translation;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SamzaOutputGBKMetricOp is a {@link SamzaMetricOp} that emits & maintains default metrics for
 * output PCollection for GroupByKey. It emits the output throughput and maintains avg output time
 * for output PCollection per windowId (). It is also responsible for emitting latency metric per
 * windowId once the watermark passes the end of window timestamp.
 *
 * <p>Assumes that {@code SamzaOutputGBKMetricOp#processWatermark(Instant, OpEmitter)} is exclusive
 * of {@code SamzaOutputGBKMetricOp#processElement(Instant, OpEmitter)}. Specifically, the
 * processWatermark method assumes that no calls to processElement will be made during its
 * execution, and vice versa.
 *
 * @param <T> The type of the elements in the output PCollection.
 */
public class SamzaOutputGBKMetricOp<T> extends SamzaMetricOp<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOutputGBKMetricOp.class);
  // Counters for keeping sum of arrival time and count of elements per windowId
  private Map<BoundedWindow, BigInteger> sumOfTimestampsPerWindowId;
  private Map<BoundedWindow, Long> sumOfCountPerWindowId;

  public SamzaOutputGBKMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    super(pValue, transformFullName, samzaOpMetricRegistry);
    this.sumOfTimestampsPerWindowId = new HashMap<>();
    this.sumOfCountPerWindowId = new HashMap<>();
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    // one element can belong to multiple windows
    for (BoundedWindow windowId : inputElement.getWindows()) {
      updateCounters(windowId);
    }

    samzaOpMetricRegistry
        .getTransformMetrics()
        .getTransformOutputThroughput(transformFullName)
        .inc();
    emitter.emitElement(inputElement);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    List<BoundedWindow> closedWindows = new ArrayList<>();
    sumOfCountPerWindowId.keySet().stream()
        .filter(windowId -> watermark.isAfter(windowId.maxTimestamp())) // window is closed
        .forEach(
            windowId -> {
              long sumOfTimestamps = sumOfTimestampsPerWindowId.get(windowId).longValue();
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

              // if the window is closed and there is some data
              if (sumOfTimestamps > 0 && count > 0) {
                samzaOpMetricRegistry.emitLatencyMetric(
                    transformFullName, windowId, Math.floorDiv(sumOfTimestamps, count), task);
              }
            });

    // remove the closed windows
    sumOfCountPerWindowId.keySet().removeAll(closedWindows);
    sumOfTimestampsPerWindowId.keySet().removeAll(closedWindows);

    // update the watermark progress metric
    samzaOpMetricRegistry
        .getTransformMetrics()
        .getTransformWatermarkProgress(transformFullName)
        .set(watermark.getMillis());

    super.processWatermark(watermark, emitter);
  }

  private synchronized void updateCounters(BoundedWindow windowId) {
    BigInteger sumTimestampsForId =
        sumOfTimestampsPerWindowId.getOrDefault(windowId, BigInteger.ZERO);
    sumOfTimestampsPerWindowId.put(
        windowId, sumTimestampsForId.add(BigInteger.valueOf(System.nanoTime())));
    Long count = sumOfCountPerWindowId.getOrDefault(windowId, 0L);
    sumOfCountPerWindowId.put(windowId, count + 1);
  }
}
