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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.samza.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SamzaOpMetricRegistry is a registry that maintains the metrics for each transform. It maintains
 * the average arrival time for each PCollection for a primitive transform.
 *
 * <p>For a non-data shuffling primitive transform, the average arrival time is calculated per
 * watermark, per PCollection {@link org.apache.beam.sdk.values.PValue}.
 *
 * <p>For data-shuffling i.e. GroupByKey, the average arrival time is calculated per windowId {@link
 * org.apache.beam.sdk.transforms.windowing.BoundedWindow}.
 */
public class SamzaOpMetricRegistry implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOpMetricRegistry.class);

  // TransformName -> PValue for pCollection -> Map<WatermarkId, AvgArrivalTime>
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>>>
      avgArrivalTimeMap;
  // TransformName -> Map<WindowId, AvgArrivalTime>
  @SuppressFBWarnings("SE_BAD_FIELD")
  private ConcurrentHashMap<String, ConcurrentHashMap<BoundedWindow, Long>> avgArrivalTimeMapForGbk;

  // Per Transform Metrics for each primitive transform
  private final BeamTransformMetrics transformMetrics;

  public SamzaOpMetricRegistry() {
    this.avgArrivalTimeMap = new ConcurrentHashMap<>();
    this.avgArrivalTimeMapForGbk = new ConcurrentHashMap<>();
    this.transformMetrics = new BeamTransformMetrics();
  }

  public void register(String transformFullName, String pValue, Context ctx) {
    transformMetrics.register(transformFullName, ctx);
    avgArrivalTimeMap.putIfAbsent(transformFullName, new ConcurrentHashMap<>());
    avgArrivalTimeMap.get(transformFullName).putIfAbsent(pValue, new ConcurrentHashMap<>());
    // Populate GBK map as well since we cannot distinguish a window and non-window operator using
    // transformName
    avgArrivalTimeMapForGbk.putIfAbsent(transformFullName, new ConcurrentHashMap<>());
  }

  BeamTransformMetrics getTransformMetrics() {
    return transformMetrics;
  }

  protected void updateArrivalTimeMap(String transformName, BoundedWindow windowId, long avg) {
    avgArrivalTimeMapForGbk.get(transformName).put(windowId, avg);
  }

  protected void updateArrivalTimeMap(
      String transformName, String pValue, long watermark, long avg) {
    avgArrivalTimeMap.get(transformName).get(pValue).put(watermark, avg);
    // remove any stale entries which are lesser than the watermark
    avgArrivalTimeMap
        .get(transformName)
        .get(pValue)
        .entrySet()
        .removeIf(entry -> entry.getKey() < watermark);
  }

  void emitLatencyMetric(
      String transformName, BoundedWindow w, long avgArrivalEndTime, String taskName) {
    Long avgArrivalStartTime = avgArrivalTimeMapForGbk.get(transformName).remove(w);
    if (avgArrivalStartTime != null && avgArrivalStartTime > 0 && avgArrivalEndTime > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            String.format(
                "Success Emit Metric TransformName %s for: %s and window: %s for task: %s",
                transformName, transformName, w, taskName));
      }
      avgArrivalTimeMapForGbk.get(transformName).remove(w);
      transformMetrics
          .getTransformLatencyMetric(transformName)
          .update(avgArrivalEndTime - avgArrivalStartTime);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            String.format(
                "Start Time: [%s] or End Time: [%s] found is 0/null for: %s and windowId: %s for task: %s",
                avgArrivalStartTime, avgArrivalEndTime, transformName, w, taskName));
      }
    }
  }

  // TODO: check the case where input has no elements, only watermarks are propogated
  void emitLatencyMetric(
      String transformName,
      List<String> transformInputs,
      List<String> transformOutputs,
      Long watermark,
      String taskName) {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartMap =
        avgArrivalTimeMap.get(transformName);

    if (!transformInputs.isEmpty() && !transformOutputs.isEmpty()) { // skip the io operators
      List<ConcurrentHashMap<Long, Long>> tmp =
          transformInputs.stream()
              .map(avgStartMap::get)
              .filter(x -> x != null)
              .collect(Collectors.toList());

      List<Long> inputPValueStartTimes =
          tmp.stream()
              .map(startTimeMap -> startTimeMap.remove(watermark)) // replace get with remove
              .filter(x -> x != null)
              .collect(Collectors.toList());

      List<Long> outputPValueStartTimes =
          transformOutputs.stream()
              .map(avgStartMap::get)
              .map(startTimeMap -> startTimeMap.remove(watermark)) // replace get with remove
              .filter(x -> x != null)
              .collect(Collectors.toList());

      if (!inputPValueStartTimes.isEmpty() && !outputPValueStartTimes.isEmpty()) {
        Long startTime = Collections.min(inputPValueStartTimes);
        Long endTime = Collections.max(outputPValueStartTimes);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              String.format(
                  "Success Emit Metric TransformName %s for: %s and watermark: %s for task: %s",
                  transformName, transformName, watermark, taskName));
        }
        Long avgLatency = endTime - startTime;
        transformMetrics.getTransformLatencyMetric(transformName).update(avgLatency);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              String.format(
                  "Start Time: [%s] or End Time: [%s] found is null for: %s and watermark: %s for task: %s",
                  inputPValueStartTimes,
                  outputPValueStartTimes,
                  transformName,
                  watermark,
                  taskName));
        }
      }
    }
  }
}
