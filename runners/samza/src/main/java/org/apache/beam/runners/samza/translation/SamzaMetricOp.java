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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.samza.runtime.KeyedTimerData;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Instant;

public abstract class SamzaMetricOp<T> implements Op<T, T, Void> {
  private int count;
  private long sumOfTimestamps;
  private final String pValue;
  private final String transformFullName;
  private MetricsRegistry metricsRegistry;
  private final SamzaOpMetricRegistry samzaOpMetricRegistry;

  // transformName -> pValue/pCollection -> Map<watermarkId, avgArrivalTime>
  ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>>

  public SamzaMetricOp(String pValue, String transformFullName) {
    this.count = 0;
    this.sumOfTimestamps = 0L;
    this.pValue = pValue;
    this.transformFullName = transformFullName;
    this.samzaOpMetricRegistry = samzaOpMetricRegistry;
  }

  @Override
  public void open(Config config, Context context, Scheduler<KeyedTimerData<Void>> timerRegistry,
      OpEmitter<T> emitter) {
    // read config to switch to per container metrics on demand
    this.metricsRegistry = context.getContainerContext().getContainerMetricsRegistry();


  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    count++;
    try {
      // sum of arrival time - overflow exception sensitive
      sumOfTimestamps = Math.addExact(sumOfTimestamps, System.currentTimeMillis());
    } catch ()
    emitter.emitElement(inputElement);

    // KV<?,?> x = (KV<?, ?>) inputElement.getValue();
    // System.out.println(String.format("[%s for %s] Element=%s Time=%s", pValue, transformFullName, x.getKey(), System.currentTimeMillis()));
    // sum of count of elements


  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    long avg = Math.floorDiv(sumOfTimestamps, count);
    // Update MetricOp Registry with counters
    samzaOpMetricRegistry.updateAvgStartTimeMap(transformFullName, pValue, watermark.getMillis(), avg);
    // reset all counters
    count = 0;
    sumOfTimestamps = 0L;
    // emit the metric
    samzaOpMetricRegistry.emitLatencyMetric(transformFullName, watermark.getMillis());
    Op.super.processWatermark(watermark, emitter);
  }
}
