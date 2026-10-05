"""A tiny in-memory MVCC transaction engine built on the standard library only.

Every key is stored as a version chain ordered from the newest version to the
oldest one.  A transaction works on the snapshot it receives when it starts: it
observes every version committed at or before its snapshot point, plus the
versions it wrote itself.  Writes stay private until the transaction commits,
and writing a key that another live transaction is already writing raises
WriteConflict.

Behaviour the rest of the project relies on
-------------------------------------------
* a transaction reads its own uncommitted writes;
* snapshot reads are repeatable: two reads of the same key inside one
  transaction return the same value even when other transactions commit in
  between;
* aborting a transaction removes every write that transaction made and leaves
  previously committed data untouched;
* gc() reclaims versions that are shadowed for every live write transaction and
  never removes a version that a live snapshot can still read.  Read-only
  transactions take no part in write conflicts and never hold back gc().
"""

ACTIVE = "active"
COMMITTED = "committed"
ABORTED = "aborted"

_MISSING = object()


class TransactionError(Exception):
    """Raised when a transaction is used in a way its state does not allow."""


class WriteConflict(TransactionError):
    """Raised when two live transactions write the same key."""


class Version:
    """A single version of a key inside a version chain."""

    __slots__ = ("txn_id", "commit_ts", "value", "deleted")

    def __init__(self, txn_id, value, deleted=False, commit_ts=None):
        self.txn_id = txn_id
        self.commit_ts = commit_ts
        self.value = value
        self.deleted = deleted

    def __repr__(self):
        if self.deleted:
            body = "<deleted>"
        else:
            body = repr(self.value)
        return "Version(txn_id=%r, commit_ts=%r, %s)" % (
            self.txn_id,
            self.commit_ts,
            body,
        )


class Transaction:
    """A transaction handle handed out by MVCCStore.begin()."""

    def __init__(self, store, txn_id, snapshot_ts, read_only=False):
        self._store = store
        self.txn_id = txn_id
        self.snapshot_ts = snapshot_ts
        self.read_only = read_only
        self.status = ACTIVE
        self.written_keys = []

    def get(self, key, default=None):
        return self._store.get(self, key, default)

    def put(self, key, value):
        return self._store.put(self, key, value)

    def delete(self, key):
        return self._store.delete(self, key)

    def scan(self):
        return self._store.scan(self)

    def commit(self):
        return self._store.commit(self)

    def abort(self):
        return self._store.abort(self)

    def __repr__(self):
        return "Transaction(id=%r, status=%r, snapshot_ts=%r, read_only=%r)" % (
            self.txn_id,
            self.status,
            self.snapshot_ts,
            self.read_only,
        )


class MVCCStore:
    """In-memory multi-version store shared by every transaction of a process."""

    def __init__(self):
        self._chains = {}
        self._txns = {}
        self._active = set()
        self._next_txn_id = 1
        self._commit_seq = 0

    # -- transaction lifecycle --------------------------------------
    def begin(self, read_only=False):
        txn_id = self._next_txn_id
        self._next_txn_id += 1
        txn = Transaction(self, txn_id, self._commit_seq, read_only=read_only)
        self._txns[txn_id] = txn
        self._active.add(str(txn_id))
        return txn

    def commit(self, txn):
        self._require_active(txn)
        if txn.written_keys:
            self._commit_seq += 1
            for key in txn.written_keys:
                for version in self._chains.get(key, ()):
                    if version.txn_id == txn.txn_id and version.commit_ts is None:
                        version.commit_ts = self._commit_seq
        txn.status = COMMITTED
        self._active.discard(txn.txn_id)
        return self._commit_seq

    def abort(self, txn):
        self._require_active(txn)
        for key in txn.written_keys[-1:]:
            chain = self._chains.get(key)
            if chain is None:
                continue
            survivors = [version for version in chain if version.txn_id != txn.txn_id]
            if survivors:
                self._chains[key] = survivors
            else:
                del self._chains[key]
        txn.written_keys = []
        txn.status = ABORTED
        self._active.discard(txn.txn_id)

    # -- reads ------------------------------------------------------
    def get(self, txn, key, default=None):
        self._require_active(txn)
        chain = self._chains.get(key)
        if not chain:
            return default
        for version in chain:
            if not self._visible(txn, version):
                continue
            return version.value
        return default

    def scan(self, txn):
        self._require_active(txn)
        snapshot = {}
        for key in self._chains:
            value = self.get(txn, key, _MISSING)
            if value is not _MISSING:
                snapshot[key] = value
        return snapshot

    # -- writes -----------------------------------------------------
    def put(self, txn, key, value):
        self._require_writable(txn)
        self._check_write_conflict(txn, key)
        pending = self._pending_version(key, txn)
        if pending is None:
            self._insert_version(key, Version(txn.txn_id, value))
        else:
            pending.value = value
            pending.deleted = False
        self._track_write(txn, key)
        return value

    def delete(self, txn, key):
        self._require_writable(txn)
        self._check_write_conflict(txn, key)
        pending = self._pending_version(key, txn)
        if pending is None:
            self._insert_version(key, Version(txn.txn_id, None, deleted=True))
        else:
            pending.value = None
            pending.deleted = True
        self._track_write(txn, key)

    # -- garbage collection -----------------------------------------
    def gc(self):
        watermark = self._gc_watermark()
        reclaimed = 0
        for key, chain in list(self._chains.items()):
            keep = self._gc_keep_count(chain, watermark)
            if keep >= len(chain):
                continue
            reclaimed += len(chain) - keep
            survivors = chain[:keep]
            if survivors:
                self._chains[key] = survivors
            else:
                del self._chains[key]
        return reclaimed

    def _gc_watermark(self):
        watermark = self._commit_seq
        for txn in self._txns.values():
            if txn.status == ACTIVE:
                watermark = min(watermark, txn.snapshot_ts)
        return watermark

    @staticmethod
    def _gc_keep_count(chain, watermark):
        keep = 0
        for version in chain:
            if version.commit_ts is None or version.commit_ts > watermark:
                keep += 1
            else:
                break
        return keep

    # -- inspection helpers -----------------------------------------
    def versions_of(self, key):
        """Return the version chain of one key, newest version first."""
        return list(self._chains.get(key, ()))

    def active_txn_ids(self):
        """Return the ids of the transactions that have not finished yet."""
        return sorted(self._active)

    # -- internals --------------------------------------------------
    def _visible(self, txn, version):
        if version.commit_ts is None:
            return version.txn_id == txn.txn_id
        return version.commit_ts < txn.snapshot_ts

    def _insert_version(self, key, version):
        self._chains.setdefault(key, []).append(version)

    def _pending_version(self, key, txn):
        for version in self._chains.get(key, ()):
            if version.txn_id == txn.txn_id and version.commit_ts is None:
                return version
        return None

    def _check_write_conflict(self, txn, key):
        chain = self._chains.get(key)
        if not chain:
            return
        head = chain[0]
        if head.commit_ts is None:
            raise WriteConflict(
                "key %r is already being written by transaction %r"
                % (key, head.txn_id)
            )

    @staticmethod
    def _track_write(txn, key):
        if key not in txn.written_keys:
            txn.written_keys.append(key)

    def _require_active(self, txn):
        if txn.status != ACTIVE:
            raise TransactionError("transaction %r is not active" % (txn.txn_id,))

    def _require_writable(self, txn):
        self._require_active(txn)
        if txn.read_only:
            raise TransactionError("transaction %r is read only" % (txn.txn_id,))
