package net.explorviz.trace.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.quarkus.jackson.ObjectMapperCustomizer;
import javax.inject.Singleton;

/**
 * Jackson object mapper settings. Disables serialization of getters/setters and only relies on
 * field.
 */
@Singleton
public class RegisterCustomModuleCustomizer implements ObjectMapperCustomizer {

  public void customize(ObjectMapper mapper) {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
  }
}
