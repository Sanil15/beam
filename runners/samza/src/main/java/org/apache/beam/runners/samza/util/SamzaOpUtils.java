package org.apache.beam.runners.samza.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Map;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.runners.samza.translation.SamzaPipelineTranslator;
import org.apache.samza.config.Config;

public class SamzaOpUtils {
  public static Map<String, Map.Entry<String, String>> getTransformToIOMap(Config config) {
    TypeReference<Map<String, Map.Entry<String, String>>> typeRef =
        new TypeReference<Map<String, Map.Entry<String,String>>>() {};
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(
          new SimpleModule().addDeserializer(Map.Entry.class, new SamzaPipelineTranslator.MapEntryDeserializer()));
      // read the BeamTransformIoMap from config deserialize
      return objectMapper.readValue(config.get(SamzaRunner.BEAM_TRANSFORMS_WITH_IO), typeRef);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Cannot deserialize %s from the configs", SamzaRunner.BEAM_TRANSFORMS_WITH_IO), e);
    }
  }
}
