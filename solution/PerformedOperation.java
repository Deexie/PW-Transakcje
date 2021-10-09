package cp1.solution;

import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import java.util.concurrent.ConcurrentMap;

class PerformedOperation {
    private ResourceId id;
    private ResourceOperation operation;

    PerformedOperation(ResourceId rid, ResourceOperation operation) {
        this.id = rid;
        this.operation = operation;
    }

    ResourceId getResourceId() {
        return id;
    }

    void rollback(ConcurrentMap<ResourceId, Resource> resources) {
        operation.undo(resources.get(id));
    }
}
