package ru.yandex.practicum.collector;

import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping(value = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
public class EventController {
    private final EventPublisher publisher;

    public EventController(EventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/sensors")
    public CompletableFuture<Void> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        return publisher.publish(event);
    }

    @PostMapping("/hubs")
    public CompletableFuture<Void> collectHubEvent(@Valid @RequestBody HubEvent event) {
        return publisher.publish(event);
    }
}
