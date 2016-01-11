package org.eventreducer.redis;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.commons.net.ntp.TimeStamp;
import org.eventreducer.Event;
import org.eventreducer.Journal;
import org.eventreducer.Serializable;
import org.eventreducer.Serializer;
import org.eventreducer.hlc.PhysicalTimeProvider;
import org.redisson.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.core.RMap;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class RedisJournal extends Journal {


    private final RedissonClient client;
    private final RMap<UUID, Event> storage;

    public RedisJournal(PhysicalTimeProvider physicalTimeProvider, RedissonClient client, String prefix) {
        super(physicalTimeProvider);
        this.client = client;

        JsonJacksonCodec jsonJacksonCodec = new JsonJacksonCodec() {

            @Override
            protected ObjectMapper initObjectMapper() {
                ObjectMapper objectMapper = super.initObjectMapper();
                SimpleModule module;
                module = new SimpleModule("RedisJournal", new Version(1,0,0,null, "org.eventreducer.redis", "RedisJournal"));

                module.addSerializer(new TimestampSerializer());
                module.addSerializer(new SerializerSerializer());
                module.addDeserializer(TimeStamp.class, new TimestampDeserializer());
                module.setMixInAnnotation(Serializable.class, SerializableMixin.class);
                objectMapper.registerModule(module);

                return objectMapper;
            }
        };

        storage = client.getMap(prefix + "_eventreducer_journal", jsonJacksonCodec);
    }

    public static abstract class SerializableMixin {

        @JsonIgnore
        private Serializer serializer;

        @JsonProperty("@hash")
        public abstract <T extends Serializable> Serializer<T> entitySerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException;

    }

    static class SerializerSerializer extends StdSerializer<Serializer> {
        protected SerializerSerializer() {
            super(Serializer.class);
        }

        @Override
        public void serialize(Serializer value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeBinary(value.hash());
        }

        @Override
        public void serializeWithType(Serializer value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            serialize(value, gen, serializers);
        }
    }

    static class TimestampSerializer extends StdSerializer<TimeStamp> {

        protected TimestampSerializer() {
            super(TimeStamp.class);
        }

        @Override
        public void serialize(TimeStamp value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(value.toString());
        }

        @Override
        public void serializeWithType(TimeStamp value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            gen.writeString(value.toString());
        }
    }

    static class TimestampDeserializer extends StdDeserializer<TimeStamp> {
        public TimestampDeserializer() {
            super(TimeStamp.class);
        }

        @Override
        public TimeStamp deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return new TimeStamp(p.readValueAs(String.class));
        }

        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return new TimeStamp(p.readValueAs(String.class));
        }
    }

    @Override
    protected void journal(List<Event> events) {
        events.stream().
                forEachOrdered(event -> storage.put(event.uuid(), event));
    }

    @Override
    public long size() {
        return storage.size();
    }

}
