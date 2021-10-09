package cp1.solution;

import cp1.base.ResourceId;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

class TransactionsState {
    private ConcurrentMap<Thread, Transaction> activeTransactions;
    private ConcurrentMap<ResourceId, Thread> currentUserOfResource;

    TransactionsState() {
        this.activeTransactions = new ConcurrentHashMap<>();
        this.currentUserOfResource = new ConcurrentHashMap<>();
    }

    void addActiveTransaction(Thread thread, Transaction transaction) {
        activeTransactions.put(thread, transaction);
    }

    void addCurrentUser(ResourceId rid, Thread thread) {
        currentUserOfResource.put(rid, thread);
    }

    boolean isInUse(ResourceId rid) {
        return currentUserOfResource.containsKey(rid);
    }

    boolean isTransactionActive(Thread thread) {
        return activeTransactions.containsKey(thread);
    }

    Transaction getThreadsTransaction(Thread thread) {
        return activeTransactions.getOrDefault(thread, null);
    }

    Thread getIdOfUser(ResourceId rid) {
        return currentUserOfResource.getOrDefault(rid, null);
    }

    Transaction getTransaction(ResourceId rid) {
        Thread tid = getIdOfUser(rid);
        if (tid == null) {
            return null;
        }
        return getThreadsTransaction(tid);
    }

    void removeTransaction(Thread thread) {
        activeTransactions.remove(thread);
    }

    void freeResources(
            ConcurrentMap<ResourceId, Semaphore> resourceMutex,
            ConcurrentMap<ResourceId, Semaphore> forResource,
            Collection<PerformedOperation> operations) {
        boolean interrupted = false;
        ResourceId rid;

        for (PerformedOperation operation : operations) {
            rid = operation.getResourceId();
            try {
                interrupted = Thread.interrupted();
                resourceMutex.get(rid).acquire();
            } catch (InterruptedException e) {
                interrupted = true;
            }

            //  Current thread still uses resource of id [rid].
            if (currentUserOfResource.getOrDefault(rid, null) ==
                    Thread.currentThread()) {
                currentUserOfResource.remove(rid);
                if (forResource.get(rid).hasQueuedThreads()) {
                    forResource.get(rid).release();
                } else {
                    resourceMutex.get(rid).release();
                }
            } else {
                resourceMutex.get(rid).release();
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}