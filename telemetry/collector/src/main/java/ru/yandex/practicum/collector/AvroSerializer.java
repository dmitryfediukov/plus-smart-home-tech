package ru.yandex.practicum.collector;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializer implements Serializer<SpecificRecord> {
    @Override
    public byte[] serialize(String topic, SpecificRecord value) {
        if (value == null) {
            return null;
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bytes, null);
            new SpecificDatumWriter<SpecificRecord>(value.getSchema()).write(value, encoder);
            encoder.flush();
            return bytes.toByteArray();
        } catch (IOException | RuntimeException failure) {
            throw new SerializationException("Cannot serialize Avro record for " + topic, failure);
        }
    }
}
