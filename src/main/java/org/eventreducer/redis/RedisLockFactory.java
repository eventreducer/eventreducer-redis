package org.eventreducer.redis;

import lombok.SneakyThrows;
import org.eventreducer.Lock;
import org.eventreducer.LockFactory;
import org.redisson.RedissonClient;
import org.redisson.core.RSemaphore;

import java.util.concurrent.Semaphore;

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
        RSemaphore semaphore = client.getSemaphore(this.prefix + "_" + lock.toString() + "_eventreducer_semaphore");
        semaphore.setPermits(1);
        semaphore.acquire();
        return new MemoryLock(semaphore);
    }

    static class MemoryLock implements Lock {
        private final RSemaphore semaphore;

        public MemoryLock(RSemaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void unlock() {
            semaphore.release();
            semaphore.deleteAsync();
        }

    }

}
