package org.eventreducer.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eventreducer.*;
import org.eventreducer.hlc.PhysicalTimeProvider;
import org.eventreducer.json.EventReducerModule;
import org.redisson.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.core.RMap;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisJournal extends Journal {


    private final RedissonClient client;
    private final RMap<UUID, Event> storage;
    private final RMap<UUID, Command> commands;

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
        commands = client.getMap(prefix + "_eventreducer_commands", jsonJacksonCodec);
    }

    @Override
    public Optional<Event> findEvent(UUID uuid) {
        if (storage.containsKey(uuid)) {
            return Optional.of(storage.get(uuid));
        }
        return Optional.empty();
    }

    @Override
    public Optional<Command> findCommand(UUID uuid) {
        if (commands.containsKey(uuid)) {
            return Optional.of(commands.get(uuid));
        }
        return Optional.empty();
    }


    @Override
    public Iterator<Event> eventIterator(Class<? extends Event> klass) {
        return storage.values().stream().filter(v -> klass.isAssignableFrom(v.getClass())).iterator();
    }

    @Override
    public Iterator<Command> commandIterator(Class<? extends Command> klass) {
        return commands.values().stream().filter(v -> klass.isAssignableFrom(v.getClass())).iterator();
    }


    @Override
    protected long journal(Command command, Stream<Event> events) {
        commands.put(command.uuid(), command);
        return events.map(event -> storage.put(event.uuid(), event)).count();
    }


    @Override
    public long size(Class<? extends Serializable> klass) {
        return storage.values().stream().filter(e -> klass.isAssignableFrom(e.getClass())).collect(Collectors.toList()).size() +
                commands.values().stream().filter(e -> klass.isAssignableFrom(e.getClass())).collect(Collectors.toList()).size();
    }

    @Override
    public Stream<Event> events(Command command) {
        return storage.values().stream().filter(e -> e.command().uuid().equals(command.uuid()));
    }

}
