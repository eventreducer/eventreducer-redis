package org.eventreducer.redis;

import lombok.SneakyThrows;
import org.eventreducer.Lock;
import org.eventreducer.LockFactory;
import org.redisson.RedissonClient;
import org.redisson.core.RLock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RedisLockFactory extends LockFactory {

    private final RedissonClient client;
    private final String prefix;

    public RedisLockFactory(RedissonClient client, String prefix) {
        this.client = client;
        this.prefix = prefix;
    }

    @Override
    @SneakyThrows
    public org.eventreducer.Lock lock(Object lock) {
        RLock l = client.getLock(this.prefix + "_" + lock.toString() + "_eventreducer_lock");
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Void> future1 = new CompletableFuture<>();
        new Thread(() -> {
            l.lock();
            future1.complete(null);
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
            }
            l.unlock();
        }).start();
        future1.get();
        return new MemoryLock(future, l);
    }

    static class MemoryLock implements Lock {
        private final CompletableFuture<Void> future;
        private final RLock lock;

        public MemoryLock(CompletableFuture<Void> future, RLock l) {
            this.future = future;
            lock = l;
        }

        @Override
        public void unlock() {
            future.complete(null);
        }

        @Override
        public boolean isLocked() {
            return lock.isLocked();
        }

    }

}
