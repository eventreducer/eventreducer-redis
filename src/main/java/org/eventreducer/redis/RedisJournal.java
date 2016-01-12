package org.eventreducer.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eventreducer.Event;
import org.eventreducer.IndexFactory;
import org.eventreducer.Journal;
import org.eventreducer.hlc.PhysicalTimeProvider;
import org.eventreducer.json.EventReducerModule;
import org.redisson.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.core.RMap;

import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

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
                objectMapper.registerModule(new EventReducerModule());
                return objectMapper;
            }
        };

        storage = client.getMap(prefix + "_eventreducer_journal", jsonJacksonCodec);
    }

    @Override
    public void prepareIndices(IndexFactory indexFactory) {
        StreamSupport.stream(storage.values().spliterator(), true).forEach(event -> {
            try {
                event.entitySerializer().index(indexFactory, event);
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            }
        });

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
