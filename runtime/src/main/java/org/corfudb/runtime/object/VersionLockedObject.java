package org.corfudb.runtime.object;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.*;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * The version locked object keeps track of where -- in the history of updates -- is the current state of the underying object.
 *
 * it maintains a shallow undo-log, back to the earliest open-transaction's snapshot.
 * it also maintains an optimistic undo-log, for the current open-transaction, if any.
 *
 * <p>
 * Created by mwei on 11/13/16.
 */
@Slf4j
public class VersionLockedObject<T> {

    /**
     * The actual underlying object.
     */
    T object;

    /** The version of the underlying object.
     *
     */
    long version;

    /** A lock, which controls access to modifications to
     * the object. Any access to unsafe methods should
     * obtain the lock.
     */
    private StampedLock lock;

    /** The stream view this object is backed by.
     *
     */
    private IStreamView sv;

    /** If the object reflects optimistic updates, the
     * context which made those updates.
     */
    private AbstractTransactionalContext modifyingContext;

    /** An undo log, which records undo entries for the object.
     *
     */
    private final Deque<SMREntry> undoLog;

    /** An optimistic undo log, which records undo entries for
     * optimistic changes to the object.
     */
    private final Deque<SMREntry> optimisticUndoLog;

    /** True, if the object is optimistically modified.
     *
     */
    private boolean optimisticallyModified;

    /** True, if optimistic changes to this object can be undone.
     *
     */
    private boolean optimisticallyUndoable;

    /** The number of optimistic changes made to this object.
     *
     */
    private int optimisticVersion;


    /** The upcall map for this object. */
    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    /** The undo record function map for this object. */
    private final Map<String, IUndoRecordFunction<T>> undoRecordFunctionMap;

    /** The undo target map for this object. */
    private final Map<String, IUndoFunction<T>> undoFunctionMap;

    public VersionLockedObject(T obj, long version, IStreamView sv,
                  Map<String, ICorfuSMRUpcallTarget<T>> upcallTargets,
                  Map<String, IUndoRecordFunction<T>> undoRecordTargets,
                  Map<String, IUndoFunction<T>> undoTargets)
    {
        this.object = obj;
        this.version = version;
        this.sv = sv;

        this.undoLog = new LinkedList<>();
        this.optimisticUndoLog = new LinkedList<>();

        this.optimisticallyUndoable = true;
        this.optimisticallyModified = false;

        this.optimisticVersion = 0;

        this.upcallTargetMap = upcallTargets;
        this.undoRecordFunctionMap = undoRecordTargets;
        this.undoFunctionMap = undoTargets;
        lock = new StampedLock();
    }

    public int getOptimisticVersionUnsafe() {
        return this.optimisticVersion;
    }

    public void clearOptimisticVersionUnsafe() {
        this.optimisticVersion = 0;
    }

    public void optimisticVersionIncrementUnsafe() {
        this.optimisticVersion++;
    }

    /** Commits all optimistic changes as a new version.
     *
     */
    public void optimisticCommitUnsafe(long version) {
        // TODO: validate the caller actually has a write lock.
        // TODO: merge the optimistic undo log into the undo log
        optimisticUndoLog.clear();
        optimisticVersion = 0;
        optimisticallyModified = false;
        optimisticallyUndoable = true;
        this.version = version;
        // TODO: fix the stream view pointer seek, for now
        // read will read the tx commit entry.
        sv.next();
        modifyingContext = null;
    }

    /** Rollback any optimistic changes, if possible.
     *  Unsafe, requires that the caller has acquired a write lock.
     */
    public void optimisticRollbackUnsafe() {
        // TODO: validate the caller actually has a write lock.

        if (!optimisticallyModified) {
            log.debug("nothing to roll");
            return;
        }
        if (!optimisticallyUndoable) {
            throw new NoRollbackException();
        }

        // The undo log is a stack, where the last entry applied
        // is at the front, which is the same order stream() returns
        // entries.
        while (optimisticUndoLog.size() > 0) {

            // first, check if the current entry is undo-able
            SMREntry x = optimisticUndoLog.peekFirst();
            if (!x.isUndoable()) {
                optimisticallyUndoable = false;
                throw new NoRollbackException(x);
            }

            // now, actually remove the undo entry and apply the undo
            optimisticUndoLog.pollFirst();
            undoFunctionMap.get(x.getSMRMethod())
                    .doUndo(object, x.getUndoRecord(), x.getSMRArguments());

            // update the version immediately, in case this while-loop gets aborted in the middle
            optimisticVersion--;
        };

        // this should be zero already
        if (optimisticVersion > 0 || optimisticUndoLog.size() > 0)
            log.warn("rollback did not empty the optimistic undo sz={} version={}", optimisticUndoLog.size(), optimisticVersion);

        // todo: the following two statements should not be necessary
        optimisticUndoLog.clear();
        optimisticVersion = 0;

        optimisticallyModified = false;
        modifyingContext = null;
    }

    /** Roll the object back to the supplied version if possible.
     * This function may roll back to a point prior to the requested version.
     * Otherwise, throws a NoRollbackException.
     *
     * Unsafe, requires that the caller has acquired a write lock.
     *
     * @param  version              The version to rollback to.
     * @throws NoRollbackException  If the object cannot be rolled back to
     *                              the supplied version.
     */
    public void rollbackUnsafe(long version) {
        // If we're already at or before the given version, there's
        // nothing to do
        if (this.version <= version) {
            return;
        }

        // If we don't have an undo log, we can't roll back.
        if (undoLog.size() == 0) {
            throw new NoRollbackException();
        }

        while (undoLog.size() > 0) {
            SMREntry undoRecord = undoLog.pollFirst();

            // Make sure the record is undoable.
            // This should never happen, but if
            // for some reason the undo log contains an
            // undoable entry, clear the log and throw an
            // exception.
            if (!undoRecord.isUndoable()) {
                undoLog.clear();
                throw new NoRollbackException();
            }

            // Apply the undo record.
            undoFunctionMap.get(undoRecord.getSMRMethod())
                    .doUndo(object, undoRecord.getUndoRecord(),
                                    undoRecord.getSMRArguments());

            this.version = undoRecord.getEntry().getGlobalAddress();


            // check if we rolled back to the requested version
            if (this.version <= version) {
                return;
            }
        }

        throw new NoRollbackException();
    }

    /** Calculate the number of undo records we need to keep,
     * possibly cleaning up the undo log, and return whether
     * we need to keep this undo record.
     *
     * @return  True, if an undo record is needed in the undo log,
     *          False otherwise.
     */
    public boolean needUndoRecordUnsafe() {
        // Now get the oldest transaction in the context set.
        long oldestVersion = TransactionalContext.getOldestSnapshot();

        // If there are no active transactions, or all active transactions
        // are after this object's version, we can just drop everything.
        if (oldestVersion == -1L || oldestVersion > version) {
            undoLog.clear();
            return false;
        }

        // remove anything older than the oldest version we need
        while (undoLog.size() > 0 &&
                undoLog.getLast().getEntry().getGlobalAddress() < oldestVersion) {
            undoLog.pollLast();
        }

        return true;
    }

    /** Apply an SMR update to the object, possibly optimistically,
     * if set.
     * @param entry         The entry to apply.
     * @param isOptimistic  Whether the update is optimistic or not.
     * @return              The upcall result, if available.
     */
    public Object applyUpdateUnsafe(SMREntry entry, boolean isOptimistic) {
        // TODO: validate the caller actually has a write lock.

        ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(entry.getSMRMethod());
        if (target == null) {
            throw new RuntimeException("Unknown upcall " + entry.getSMRMethod());
        }

        // generate an undo record
        IUndoRecordFunction<T> undoRecordTarget =
                undoRecordFunctionMap
                        .get(entry.getSMRMethod());
        if (undoRecordTarget == null)
            log.debug("undo not available {}", entry);
        if (undoRecordTarget != null) {
            // calculate the undo record if it doesn't exist.
            if (!entry.isUndoable()) {
                entry.setUndoRecord(undoRecordTarget
                        .getUndoRecord(object, entry.getSMRArguments()));
                entry.setUndoable(true);
            }
        }

        // Here we maintain the optimistic undo-log
        if (isOptimistic) {
            if (undoRecordTarget == null) {
                optimisticUndoLog.clear();
                optimisticallyUndoable = false;
            } else if (optimisticallyUndoable) {
                optimisticUndoLog.addFirst(entry);
                optimisticallyModified = true;
                optimisticVersionIncrementUnsafe();
            }
        }

        // Here we maintain the normal undo-log
        else {
            if (undoRecordTarget == null) {
                undoLog.clear();
            } else if (!undoLog.isEmpty() || needUndoRecordUnsafe()) {
                undoLog.addFirst(entry);
            }
        }

        // now invoke the upcall
        return target.upcall(object, entry.getSMRArguments());
    }

    /** Execute the given function under a write lock, not returning
     * anything.
     * @param writeFunction The function to execute under the write lock.
     */
    public void writeReturnVoid(BiConsumer<Long, T> writeFunction) {
        write((a,v) -> {writeFunction.accept(a,v); return null; });
    }

    /** Execute the given function under a write lock.
     *
     * @param writeFunction The function to execute under a write lock.
     * @param <R>           The type of the return value of the write function.
     * @return              The return value of the write function.
     */
    public <R> R write(BiFunction<Long, T, R> writeFunction) {
        long ts = lock.writeLock();
        try {
            return writeFunction.apply(ts, object);
        } finally {
            lock.unlock(ts);
        }
    }

    /** Return whether or not the object is locked for write.
     *
     * @return  True, if the object is locked for write.
     */
    public boolean isWriteLocked() {
        return lock.tryOptimisticRead() == 0;
    }

    public <R> R optimisticallyReadAndRetry(BiFunction<Long, T, R> readFunction) {
        long ts = lock.tryOptimisticRead();
        if (ts != 0) {
            R ret = readFunction.apply(version, object);
            if (lock.validate(ts)) {
                return ret;
            }
        }

        // Optimistic reading failed, retry with a full lock
        ts = lock.readLock();
        try {
            return readFunction.apply(version, object);
        } finally {
            lock.unlockRead(ts);
        }
    }

    public <R> R optimisticallyReadThenReadLockThenWriteOnFail
            (BiFunction<Long, T, R> readFunction,
             BiFunction<Long, T, R> retryWriteFunction
            ) {
        long ts = lock.tryOptimisticRead();
        if (ts != 0) {
            try {
                R ret = readFunction.apply(version, object);
                if (lock.validate(ts)) {
                    return ret;
                }
            } catch (ConcurrentModificationException cme) {
                // thrown by read function to force a full lock.
            }
        }
        // Optimistic reading failed, retry with a full lock
        ts = lock.readLock();
        try {
            try {
                return readFunction.apply(version, object);
            } finally {
                lock.unlock(ts);
            }
        } catch (ConcurrentModificationException cme) {
            // throw by read function to force a append lock...
        }
        // reading failed, retry with a full lock
        ts = lock.writeLock();
        try {
            return retryWriteFunction.apply(version, object);
        } finally {
            lock.unlock(ts);
        }
    }

    public void waitOnLock() {
        long ls = lock.readLock();
        lock.unlockRead(ls);
    }

    public T getObjectUnsafe() {
        return object;
    }

    public void setObjectUnsafe(T object) {
        this.object = object;
    }

    public long getVersionUnsafe() {
        return version;
    }

    public void setVersionUnsafe(long version) {
        this.version = version;
    }

    public AbstractTransactionalContext getModifyingContextUnsafe() {
        return this.modifyingContext;
    }

    public void setTXContextUnsafe
            (AbstractTransactionalContext context) {
        this.modifyingContext = context;
    }

    public IStreamView getStreamViewUnsafe() {
        return sv;
    }

    public boolean isOptimisticallyUndoableUnsafe() {
        return optimisticallyUndoable;
    }

    public boolean isOptimisticallyModifiedUnsafe() {
        return optimisticallyModified;
    }

    /** Reset the stream view backing this object.
     * This function also resets the undo log, since it's based
     * on the current position in the stream view.
     */
    public void resetStreamViewUnsafe() {
        sv.reset();
        undoLog.clear();
    }
}
