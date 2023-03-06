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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Per Transform Metric Holder
public class SamzaOpMetricRegistry implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOpMetricRegistry.class);

  // transformName -> pValue/pCollection -> Map<watermarkId, avgArrivalTime>
  private final ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartTimeMapPerTranform;

  // transformName -> List<inputPCollections>,List<outputPCollections>
  //  private final Map.Entry<String, String> transformNameToInputOutput;

  // tput metrics:
  //private final ConcurrentHashMap<String, Counter> transformInputThroughPut;
  //private final ConcurrentHashMap<String, Counter> transformOutputThroughPut;

  public SamzaOpMetricRegistry(Map<String, String> config) {
    avgStartTimeMapPerTranform = new ConcurrentHashMap<>();

    // init this metric map
//    transformNameToInputOutput
//        .keySet()
//        .forEach(
//            transform -> {
//              avgStartTimeMapPerTranform.put(transform, new ConcurrentHashMap<>());
//              tranformToLatency.put(
//                  transform, new Timer(transform + "-handle-message-ms"));
//            });
  }

  protected void updateAvgStartTimeMap(String transformName, String pValue, long watermark, long avg) {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartTimeMap = avgStartTimeMapPerTranform.get(transformName);
    if (!avgStartTimeMap.containsKey(pValue)) {
      avgStartTimeMap.put(pValue, new ConcurrentHashMap<>());
    }
    avgStartTimeMap.get(pValue).put(watermark, avg);
  }

  protected void emitLatencyMetric(String transformName, Long watermark) {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartTimeMap = avgStartTimeMapPerTranform.get(transformName);

    List<String> inputPValue = Arrays.stream(transformNameToInputOutput.get(transformName).getKey().split(",")).filter(item-> !item.isEmpty()).collect(Collectors.toList());
    List<String> outputPValue = Arrays.stream(transformNameToInputOutput.get(transformName).getValue().split(",")).filter(item-> !item.isEmpty()).collect(Collectors.toList());

    if (!inputPValue.isEmpty() && !outputPValue.isEmpty()) { // skip the io operators
      List<Long> inputPValueStartTimes =
          inputPValue.stream().map(avgStartTimeMap::get).map(startTimeMap -> startTimeMap.get(watermark)).collect(Collectors.toList());

      List<Long> outputPValueStartTimes =
          outputPValue.stream().map(avgStartTimeMap::get).map(startTimeMap -> startTimeMap.get(watermark)).collect(Collectors.toList());

      Long startTime = Collections.min(inputPValueStartTimes);
      Long endTime = Collections.max(outputPValueStartTimes);
      Long avgLatency = startTime - endTime;
      // todo remove the entries for the watermark from inmemory map
      transformNameToLatency.get(transformName).set(avgLatency);
    } else {
      LOG.info("Water in IO Transform {} for watermark {}", transformName, watermark);
    }
  }
}
