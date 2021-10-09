package cp1.solution;

import cp1.base.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

public class TransactionManager implements cp1.base.TransactionManager {
    private final LocalTimeProvider timeProvider;
    private final ConcurrentMap<ResourceId, Resource> resources;
    private final TransactionsState transactionsState;
    private final ConcurrentMap<ResourceId, Semaphore> forResource;
    private final ConcurrentMap<ResourceId, Semaphore> resourceMutex;
    private final DeadlockSolver deadlockSolver;

    TransactionManager(
            Collection<Resource> resources,
            LocalTimeProvider timeProvider
    ) {
        this.timeProvider = timeProvider;
        this.transactionsState = new TransactionsState();
        this.resources = new ConcurrentHashMap<>();
        this.forResource = new ConcurrentHashMap<>();
        this.resourceMutex = new ConcurrentHashMap<>();
        for (Resource resource: resources) {
            this.resources.put(resource.getId(), resource);
            this.forResource.put(resource.getId(), new Semaphore(0, true));
            this.resourceMutex.put(resource.getId(), new Semaphore(1, true));
        }
        deadlockSolver = new DeadlockSolver(transactionsState);
    }

    @Override
    public void startTransaction()
            throws AnotherTransactionActiveException {
        if (isTransactionActive()) {
            throw new AnotherTransactionActiveException();
        }

        transactionsState.addActiveTransaction(Thread.currentThread(),
                new Transaction(Thread.currentThread(), timeProvider));
    }

    private void gainAccess(ResourceId rid)
            throws InterruptedException, ActiveTransactionAborted {
        try {
            checkIfAbortedOrInterrupted();
            transactionsState.addCurrentUser(rid, Thread.currentThread());
        }
        catch (InterruptedException e) {
            if (forResource.get(rid).hasQueuedThreads()) {
                forResource.get(rid).release();
            }
            throw e;
        }
        finally {
            resourceMutex.get(rid).release();
        }
    }

    private void applyForAccess(Transaction myTransaction, ResourceId rid)
            throws InterruptedException, ActiveTransactionAborted {
        resourceMutex.get(rid).acquire();
        // Thread did not use resource of id [rid] before in this transaction.
        if (transactionsState.getIdOfUser(rid) != Thread.currentThread()) {
            if (transactionsState.isInUse(rid)) {
                deadlockSolver.cancelIfDeadlock(myTransaction, rid);
                resourceMutex.get(rid).release();

                try {
                    checkIfAbortedOrInterrupted();
                    forResource.get(rid).acquire();
                }
                finally {
                    deadlockSolver.changeTransactionWaitingStatus(myTransaction, false);
                }
            }

            gainAccess(rid);
        }
        else {
            resourceMutex.get(rid).release();
        }
    }

    @Override
    public void operateOnResourceInCurrentTransaction(
            ResourceId rid,
            ResourceOperation operation
    ) throws
            NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        }
        if (!resources.containsKey(rid)) {
            throw new UnknownResourceIdException(rid);
        }
        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }

        Transaction myTransaction =
                transactionsState.getThreadsTransaction(Thread.currentThread());

        applyForAccess(myTransaction, rid);

        checkIfAbortedOrInterrupted();
        operation.execute(resources.get(rid));
        try {
            checkIfAbortedOrInterrupted();
        }
        catch (InterruptedException | ActiveTransactionAborted e) {
            // If thread was interrupted during operation, operation has to be undone
            // not to change resourse state.
            operation.undo(resources.get(rid));
            throw e;
        }
        myTransaction.addOperation(rid, operation);
    }

    private void checkIfAbortedOrInterrupted()
            throws InterruptedException, ActiveTransactionAborted {
        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    @Override
    public void commitCurrentTransaction()
            throws
            NoActiveTransactionException,
            ActiveTransactionAborted {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        }
        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }

        Transaction commitedTransaction =
                transactionsState.getThreadsTransaction(Thread.currentThread());
        transactionsState.removeTransaction(Thread.currentThread());

        Stack<PerformedOperation> operations =
                commitedTransaction.getOperations();

        transactionsState.freeResources(resourceMutex, forResource, operations);
    }

    @Override
    public void rollbackCurrentTransaction() {
        if (transactionsState.isTransactionActive(Thread.currentThread())) {
            Transaction rollbackedTransaction =
                    transactionsState.getThreadsTransaction(Thread.currentThread());
            transactionsState.removeTransaction(Thread.currentThread());

            Stack<PerformedOperation> operations =
                    rollbackedTransaction.getOperations();
            if (operations != null) {
                Set<PerformedOperation> operationSet = new HashSet<>();

                while (!operations.empty()) {
                    PerformedOperation currentOperation = operations.pop();
                    operationSet.add(currentOperation);

                    currentOperation.rollback(resources);
                }

                transactionsState.freeResources(resourceMutex, forResource, operationSet);
            }
        }
    }

    @Override
    public boolean isTransactionActive() {
        return transactionsState.isTransactionActive(Thread.currentThread());
    }

    @Override
    public boolean isTransactionAborted() {
        return isTransactionActive() &&
                transactionsState.getThreadsTransaction(Thread.currentThread()).getIsCancelled();
    }
}