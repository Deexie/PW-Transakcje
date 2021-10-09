package cp1.solution;

import cp1.base.LocalTimeProvider;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import java.util.Stack;

class Transaction {
    private boolean isCancelled;
    private Thread thread;
    private Stack<PerformedOperation> operations;
    private boolean isWaiting;
    private ResourceId neededResource;
    private long creationDate;

    Transaction(Thread thread, LocalTimeProvider timeProvider) {
        this.thread = thread;
        this.isCancelled = false;
        this.operations = new Stack<>();
        this.isWaiting = false;
        this.creationDate = timeProvider.getTime();
    }

    void setNeededResource(ResourceId id) {
        this.neededResource = id;
    }

    boolean getIsCancelled() {
        return isCancelled;
    }

    boolean getIsWaiting() {
        return isWaiting;
    }

    void setIsWaiting(boolean value) {
        isWaiting = value;
    }

    void addOperation(ResourceId rid, ResourceOperation operation) {
        operations.add(new PerformedOperation(rid, operation));
    }

    ResourceId getNeededResource() {
        return neededResource;
    }

    Long getThreadId() {
        return thread.getId();
    }

    long getCreationTime() {
        return creationDate;
    }

    void cancel() {
        isCancelled = true;
        thread.interrupt();
    }

    Stack<PerformedOperation> getOperations() {
        if (operations.empty()) {
            return null;
        }

        return operations;
    }
}
