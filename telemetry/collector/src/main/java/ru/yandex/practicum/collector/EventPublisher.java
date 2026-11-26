package ru.yandex.practicum.collector;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
public class EventPublisher {
    private final KafkaTemplate<String, SpecificRecord> kafka;
    private final String sensorTopic;
    private final String hubTopic;

    public EventPublisher(KafkaTemplate<String, SpecificRecord> kafka,
                          @Value("${collector.kafka.sensor-topic}") String sensorTopic,
                          @Value("${collector.kafka.hub-topic}") String hubTopic) {
        this.kafka = kafka;
        this.sensorTopic = sensorTopic;
        this.hubTopic = hubTopic;
    }

    public CompletableFuture<Void> publish(SensorEvent event) {
        Instant timestamp = Optional.ofNullable(event.timestamp()).orElseGet(Instant::now);
        SpecificRecord payload = switch (event) {
            case SensorEvent.Climate climate ->
                    new ClimateSensorAvro(climate.temperatureC(), climate.humidity(), climate.co2Level());
            case SensorEvent.Light light -> new LightSensorAvro(light.linkQuality(), light.luminosity());
            case SensorEvent.Motion motion -> new MotionSensorAvro(motion.linkQuality(), motion.motion(), motion.voltage());
            case SensorEvent.Switch toggle -> new SwitchSensorAvro(toggle.state());
            case SensorEvent.Temperature temperature -> new TemperatureSensorAvro(
                    temperature.id(), temperature.hubId(), timestamp, temperature.temperatureC(), temperature.temperatureF());
        };
        return send(sensorTopic, event.hubId(), new SensorEventAvro(event.id(), event.hubId(), timestamp, payload));
    }

    public CompletableFuture<Void> publish(HubEvent event) {
        Instant timestamp = Optional.ofNullable(event.timestamp()).orElseGet(Instant::now);
        SpecificRecord payload = switch (event) {
            case HubEvent.DeviceAdded added -> new DeviceAddedEventAvro(added.id(), added.deviceType());
            case HubEvent.DeviceRemoved removed -> new DeviceRemovedEventAvro(removed.id());
            case HubEvent.ScenarioAdded added -> new ScenarioAddedEventAvro(
                    added.name(),
                    added.conditions().stream().map(condition -> new ScenarioConditionAvro(
                            condition.sensorId(), condition.type(), condition.operation(), condition.avroValue())).toList(),
                    added.actions().stream().map(action -> new DeviceActionAvro(
                            action.sensorId(), action.type(), action.value())).toList());
            case HubEvent.ScenarioRemoved removed -> new ScenarioRemovedEventAvro(removed.name());
        };
        return send(hubTopic, event.hubId(), new HubEventAvro(event.hubId(), timestamp, payload));
    }

    private CompletableFuture<Void> send(String topic, String hubId, SpecificRecord event) {
        try {
            return kafka.send(topic, hubId, event).handle((result, failure) -> {
                if (failure != null) {
                    throw unavailable(failure);
                }
                return null;
            });
        } catch (KafkaException | org.apache.kafka.common.KafkaException failure) {
            throw unavailable(failure);
        }
    }

    private ResponseStatusException unavailable(Throwable failure) {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka publication failed", failure);
    }
}
