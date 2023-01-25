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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class knows all the translators from a primitive BEAM transform to a Samza operator. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SamzaPipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPipelineTranslator.class);

  private static final Map<String, TransformTranslator<?>> TRANSLATORS = loadTranslators();

  private static Map<String, TransformTranslator<?>> loadTranslators() {
    Map<String, TransformTranslator<?>> translators = new HashMap<>();
    for (SamzaTranslatorRegistrar registrar : ServiceLoader.load(SamzaTranslatorRegistrar.class)) {
      translators.putAll(registrar.getTransformTranslators());
    }
    return ImmutableMap.copyOf(translators);
  }

  private SamzaPipelineTranslator() {}

  public static void translate(Pipeline pipeline, TranslationContext ctx) {
    final TransformVisitorFn translateFn =
        new TransformVisitorFn() {

          @Override
          public <T extends PTransform<?, ?>> void apply(
              T transform,
              TransformHierarchy.Node node,
              Pipeline pipeline,
              TransformTranslator<T> translator) {
            ctx.setCurrentTransform(node.toAppliedPTransform(pipeline));

            translator.translate(transform, node, ctx);

            ctx.clearCurrentTransform();
          }
        };
    final SamzaPipelineVisitor visitor = new SamzaPipelineVisitor(translateFn);
    pipeline.traverseTopologically(visitor);
  }

  public static void createConfig(
      Pipeline pipeline,
      SamzaPipelineOptions options,
      Map<PValue, String> idMap,
      Set<String> nonUniqueStateIds,
      ConfigBuilder configBuilder) {
    final ConfigContext ctx = new ConfigContext(idMap, nonUniqueStateIds, options);

    final Map<String, Map.Entry<String, String>> pTransformToInputOutputMap = new HashMap<>();

    final TransformVisitorFn configFn =
        new TransformVisitorFn() {
          @Override
          public <T extends PTransform<?, ?>> void apply(
              T transform,
              TransformHierarchy.Node node,
              Pipeline pipeline,
              TransformTranslator<T> translator) {

            ctx.setCurrentTransform(node.toAppliedPTransform(pipeline));

            if (translator instanceof TransformConfigGenerator) {
              TransformConfigGenerator<T> configGenerator =
                  (TransformConfigGenerator<T>) translator;
              configBuilder.putAll(configGenerator.createConfig(transform, node, ctx));
            }

            List<String> inputs =
                node.getInputs().values().stream()
                    .map(x -> x.getName())
                    .collect(Collectors.toList());
            List<String> outputs =
                node.getOutputs().values().stream()
                    .map(x -> x.getName())
                    .collect(Collectors.toList());
            pTransformToInputOutputMap.put(
                node.getFullName(),
                new AbstractMap.SimpleEntry<>(String.join(",", inputs), String.join(",", outputs)));
            ctx.clearCurrentTransform();
          }
        };

    final SamzaPipelineVisitor visitor = new SamzaPipelineVisitor(configFn);
    pipeline.traverseTopologically(visitor);
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(
          new SimpleModule().addSerializer(Map.Entry.class, new MapEntrySerializer()));
      configBuilder.put(
          SamzaRunner.BEAM_TRANSFORMS_WITH_IO,
          objectMapper.writeValueAsString(pTransformToInputOutputMap));
    } catch (IOException e) {
      LOG.error(
          "Unable to serialize {} using {}",
          SamzaRunner.BEAM_TRANSFORMS_WITH_IO,
          pTransformToInputOutputMap);
    }
  }

  private interface TransformVisitorFn {
    <T extends PTransform<?, ?>> void apply(
        T transform,
        TransformHierarchy.Node node,
        Pipeline pipeline,
        TransformTranslator<T> translator);
  }

  private static class SamzaPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
    private final TransformVisitorFn visitorFn;

    private SamzaPipelineVisitor(TransformVisitorFn visitorFn) {
      this.visitorFn = visitorFn;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      final PTransform<?, ?> transform = node.getTransform();
      final String urn = getUrnForTransform(transform);
      if (canTranslate(urn, transform)) {
        applyTransform(transform, node, TRANSLATORS.get(urn));
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      final PTransform<?, ?> transform = node.getTransform();
      final String urn = getUrnForTransform(transform);
      checkArgument(
          canTranslate(urn, transform),
          String.format("Unsupported transform class: %s. Node: %s", transform, node));

      applyTransform(transform, node, TRANSLATORS.get(urn));
    }

    private <T extends PTransform<?, ?>> void applyTransform(
        T transform, TransformHierarchy.Node node, TransformTranslator<?> translator) {

      @SuppressWarnings("unchecked")
      final TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
      visitorFn.apply(transform, node, getPipeline(), typedTranslator);
    }

    private static boolean canTranslate(String urn, PTransform<?, ?> transform) {
      if (!TRANSLATORS.containsKey(urn)) {
        return false;
      } else if (urn.equals(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN)) {
        // According to BEAM, Combines with side inputs are translated as generic composites
        return ((Combine.PerKey) transform).getSideInputs().isEmpty();
      } else {
        return true;
      }
    }

    private static String getUrnForTransform(PTransform<?, ?> transform) {
      return transform == null ? null : PTransformTranslation.urnForTransformOrNull(transform);
    }
  }

  /** Registers Samza translators. */
  @AutoService(SamzaTranslatorRegistrar.class)
  public static class SamzaTranslators implements SamzaTranslatorRegistrar {

    @Override
    public Map<String, TransformTranslator<?>> getTransformTranslators() {
      return ImmutableMap.<String, TransformTranslator<?>>builder()
          .put(PTransformTranslation.READ_TRANSFORM_URN, new ReadTranslator<>())
          .put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslator<>())
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoBoundMultiTranslator<>())
          .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator<>())
          .put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, new GroupByKeyTranslator<>())
          .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslator<>())
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionsTranslator<>())
          .put(SamzaPublishView.SAMZA_PUBLISH_VIEW_URN, new SamzaPublishViewTranslator<>())
          .put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator())
          .put(ExecutableStage.URN, new ParDoBoundMultiTranslator<>())
          .put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, new SamzaTestStreamTranslator())
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN,
              new SplittableParDoTranslators.ProcessKeyedElements<>())
          .build();
    }
  }

  /** Registers classes specialized to the Samza runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class SamzaTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.of(
          SamzaPublishView.class, new SamzaPublishView.SamzaPublishViewPayloadTranslator());
    }
  }

  private static final class MapEntrySerializer extends JsonSerializer<Map.Entry> {
    @Override
    public void serialize(Map.Entry value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeObjectField("left", value.getKey());
      gen.writeObjectField("right", value.getValue());
      gen.writeEndObject();
    }
  }

  public static final class MapEntryDeserializer extends JsonDeserializer<Map.Entry> {
    @Override
    public Map.Entry deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JacksonException {
      JsonNode node = jp.getCodec().readTree(jp);
      String key = node.get("left").textValue();
      String value = node.get("right").textValue();
      return new AbstractMap.SimpleEntry<>(key, value);
    }
  }
}
