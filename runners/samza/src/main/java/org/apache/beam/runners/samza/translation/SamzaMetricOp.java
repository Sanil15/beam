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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.runtime.KeyedTimerData;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.util.SamzaOpUtils;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;

/**
 * MetricOp for default throughput, latency & watermark progress metric per transform for Beam Samza
 * Runner. A MetricOp can be either attached to Input PCollection or Output PCollection of a
 * PTransform.
 *
 * <p>A MetricOp is created per primitive PTransform per PCollection its across its inputs &
 * outputs. 1. An independent MetricOp is created and attached to each input PCollection to the
 * PTransform. 2. An independent MetricOp is created and attached to each input PCollection to the
 * PTransform.
 *
 * <p>Each concrete MetricOp is responsible for following metrics computation: 1. Throughput: Emit
 * the number of elements processed in the PCollection 2. Watermark Progress: Emit the watermark
 * progress of the PCollection 3. Latency: Maintain the avg arrival time per watermark across
 * elements it processes, compute & emit the latency
 *
 * @param <T> type of the message
 */
public abstract class SamzaMetricOp<T> implements Op<T, T, Void> {
  // Unique name of the PTransform this MetricOp is associated with
  protected final String transformFullName;
  protected final SamzaOpMetricRegistry samzaOpMetricRegistry;
  // Name or identifier of the PCollection which Ptraform is processing
  protected final String pValue;
  // List of input PValue(s) for all PCollections processing the PTransform
  protected List<String> transformInputs;
  // List of output PValue(s) for all PCollections processing the PTransform
  protected List<String> transformOutputs;
  // Name of the task, for logging purpose
  protected String task;

  public SamzaMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    this.transformFullName = transformFullName;
    this.samzaOpMetricRegistry = samzaOpMetricRegistry;
    this.pValue = pValue;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<Void>> timerRegistry,
      OpEmitter<T> emitter) {
    final Map.Entry<String, String> transformInputOutput =
        SamzaOpUtils.deserializeTransformIOMap(config).get(transformFullName);
    this.transformInputs = ioFunc(transformInputOutput.getKey()).get();
    this.transformOutputs = ioFunc(transformInputOutput.getValue()).get();
    this.task = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    // Register the transform with SamzaOpMetricRegistry
    samzaOpMetricRegistry.register(transformFullName, pValue, context);
  }

  private static Supplier<List<String>> ioFunc(String ioList) {
    return () ->
        Arrays.stream(ioList.split(SamzaOpUtils.TRANSFORM_IO_MAP_DELIMITER))
            .filter(item -> !item.isEmpty())
            .collect(Collectors.toList());
  }
}
