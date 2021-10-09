package cp1.solution;

import cp1.base.ResourceId;

import java.util.HashSet;
import java.util.Set;

class DeadlockSolver {
    private TransactionsState transactionsState;

    DeadlockSolver(TransactionsState transactionsState) {
        this.transactionsState = transactionsState;
    }

    synchronized void cancelIfDeadlock(Transaction myTransaction, ResourceId rid) {
        changeTransactionWaitingStatus(myTransaction, true);
        myTransaction.setNeededResource(rid);

        Set<Transaction> transactions = checkIfDeadlock(myTransaction);
        if (transactions != null) {
            Transaction latest = getLatestTransaction(transactions);
            latest.cancel();
        }
    }

    synchronized private Set<Transaction> checkIfDeadlock(Transaction myTransaction) {
        Set<Transaction> deadlockLoop = new HashSet<>();
        Transaction currentTransaction = myTransaction;

        do {
            //  Current thread did not cause deadlock.
            if (currentTransaction == null || currentTransaction.getIsCancelled() ||
                    !currentTransaction.getIsWaiting()) {
                return null;
            }
            deadlockLoop.add(currentTransaction);

            currentTransaction =
                    transactionsState.getTransaction(currentTransaction.getNeededResource());
        }
        while (currentTransaction != myTransaction);

        return deadlockLoop;
    }



    synchronized private Transaction getLatestTransaction(Set<Transaction> transactions) {
        Transaction latest = null;
        for (Transaction transaction: transactions) {
            if (latest == null) {
                latest = transaction;
            }
            else if (checkIfLater(latest, transaction)) {
                latest = transaction;
            }
        }

        return latest;
    }

    synchronized private boolean checkIfLater(Transaction latest, Transaction transaction) {
        return (transaction.getCreationTime() > latest.getCreationTime() ||
                (transaction.getCreationTime() == latest.getCreationTime() &&
                transaction.getThreadId() > latest.getThreadId()));
    }

    synchronized void changeTransactionWaitingStatus
            (Transaction myTransaction, boolean status) {
        myTransaction.setIsWaiting(status);
    }
}
