package ru.yandex.practicum.collector;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SensorEvent.Climate.class, name = "CLIMATE_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = SensorEvent.Light.class, name = "LIGHT_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = SensorEvent.Motion.class, name = "MOTION_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = SensorEvent.Switch.class, name = "SWITCH_SENSOR_EVENT"),
        @JsonSubTypes.Type(value = SensorEvent.Temperature.class, name = "TEMPERATURE_SENSOR_EVENT")
})
public sealed interface SensorEvent {
    String id();

    String hubId();

    Instant timestamp();

    record Climate(@NotBlank String id, @NotBlank String hubId, Instant timestamp,
                   @NotNull Integer temperatureC, @NotNull Integer humidity,
                   @NotNull Integer co2Level) implements SensorEvent {
    }

    record Light(@NotBlank String id, @NotBlank String hubId, Instant timestamp,
                 int linkQuality, int luminosity) implements SensorEvent {
    }

    record Motion(@NotBlank String id, @NotBlank String hubId, Instant timestamp,
                  @NotNull Integer linkQuality, @NotNull Boolean motion,
                  @NotNull Integer voltage) implements SensorEvent {
    }

    record Switch(@NotBlank String id, @NotBlank String hubId, Instant timestamp,
                  @NotNull Boolean state) implements SensorEvent {
    }

    record Temperature(@NotBlank String id, @NotBlank String hubId, Instant timestamp,
                       @NotNull Integer temperatureC, @NotNull Integer temperatureF) implements SensorEvent {
    }
}
