package ru.yandex.practicum.collector;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;

import java.time.Instant;
import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = HubEvent.DeviceAdded.class, name = "DEVICE_ADDED"),
        @JsonSubTypes.Type(value = HubEvent.DeviceRemoved.class, name = "DEVICE_REMOVED"),
        @JsonSubTypes.Type(value = HubEvent.ScenarioAdded.class, name = "SCENARIO_ADDED"),
        @JsonSubTypes.Type(value = HubEvent.ScenarioRemoved.class, name = "SCENARIO_REMOVED")
})
public sealed interface HubEvent {
    String hubId();

    Instant timestamp();

    record DeviceAdded(@NotBlank String hubId, Instant timestamp, @NotBlank String id,
                       @NotNull DeviceTypeAvro deviceType) implements HubEvent {
    }

    record DeviceRemoved(@NotBlank String hubId, Instant timestamp, @NotBlank String id) implements HubEvent {
    }

    record ScenarioAdded(@NotBlank String hubId, Instant timestamp, @NotBlank @Size(min = 3) String name,
                         @NotEmpty List<@NotNull @Valid Condition> conditions,
                         @NotEmpty List<@NotNull @Valid Action> actions) implements HubEvent {
    }

    record ScenarioRemoved(@NotBlank String hubId, Instant timestamp,
                           @NotBlank @Size(min = 3) String name) implements HubEvent {
    }

    record Condition(@NotBlank String sensorId, @NotNull ConditionTypeAvro type,
                     @NotNull ConditionOperationAvro operation, JsonNode value) {
        @AssertTrue(message = "value must be null, a boolean or a 32-bit integer")
        public boolean isValueValid() {
            return value == null || value.isNull() || value.isBoolean()
                    || (value.isIntegralNumber() && value.canConvertToInt());
        }

        Object avroValue() {
            if (value == null || value.isNull()) {
                return null;
            }
            if (value.isBoolean()) {
                return value.booleanValue();
            }
            return value.intValue();
        }
    }

    record Action(@NotBlank String sensorId, @NotNull ActionTypeAvro type, Integer value) {
    }
}
