import unittest

from mvcc.core import MVCCStore, TransactionError, WriteConflict


class VisibilityTest(unittest.TestCase):
    def setUp(self):
        self.store = MVCCStore()

    def test_committed_value_is_visible_to_a_later_transaction(self):
        writer = self.store.begin()
        writer.put("account", 100)
        writer.commit()

        reader = self.store.begin()
        self.assertEqual(reader.get("account"), 100)

    def test_own_writes_are_visible_and_pending_writes_stay_private(self):
        seeded = self.store.begin()
        seeded.put("account", 10)
        seeded.commit()

        writer = self.store.begin()
        writer.put("account", 20)
        self.assertEqual(writer.get("account"), 20)

        observer = self.store.begin()
        self.assertEqual(observer.get("account"), 10)

        writer.commit()
        self.assertEqual(observer.get("account"), 10)

        later = self.store.begin()
        self.assertEqual(later.get("account"), 20)

    def test_repeated_writes_in_one_transaction_keep_the_last_value(self):
        txn = self.store.begin()
        txn.put("counter", 1)
        txn.put("counter", 2)
        self.assertEqual(txn.get("counter"), 2)
        txn.commit()

        reader = self.store.begin()
        self.assertEqual(reader.get("counter"), 2)
        self.assertEqual(
            [version.value for version in self.store.versions_of("counter")], [2]
        )

    def test_second_writer_of_a_pending_key_is_rejected(self):
        seeded = self.store.begin()
        seeded.put("slot", "free")
        seeded.commit()

        first = self.store.begin()
        first.put("slot", "taken")

        second = self.store.begin()
        with self.assertRaises(WriteConflict):
            second.put("slot", "stolen")

        first.commit()
        second.abort()

        reader = self.store.begin()
        self.assertEqual(reader.get("slot"), "taken")

    def test_deleted_keys_disappear_from_reads_and_scans(self):
        writer = self.store.begin()
        writer.put("temp", 1)
        writer.commit()

        remover = self.store.begin()
        remover.delete("temp")
        remover.commit()

        reader = self.store.begin()
        self.assertIsNone(reader.get("temp"))
        self.assertEqual(reader.get("temp", "gone"), "gone")
        self.assertEqual(reader.scan(), {})

        ghost = self.store.begin()
        ghost.delete("never-written")
        ghost.commit()

        after = self.store.begin()
        self.assertEqual(after.get("never-written", "gone"), "gone")
        self.assertEqual(after.scan(), {})

    def test_abort_undoes_every_write_of_the_transaction(self):
        keeper = self.store.begin()
        keeper.put("kept", "safe")
        keeper.commit()

        doomed = self.store.begin()
        doomed.put("first", 1)
        doomed.put("second", 2)
        doomed.delete("kept")
        doomed.abort()

        self.assertEqual(self.store.versions_of("first"), [])
        self.assertEqual(self.store.versions_of("second"), [])
        self.assertEqual(
            [version.value for version in self.store.versions_of("kept")], ["safe"]
        )
        with self.assertRaises(TransactionError):
            doomed.get("first")

        later = self.store.begin()
        later.put("first", 10)
        later.commit()

        reader = self.store.begin()
        self.assertEqual(reader.get("first"), 10)
        self.assertEqual(reader.get("kept"), "safe")

    def test_snapshot_reads_are_repeatable_across_commits_and_gc(self):
        one = self.store.begin()
        one.put("k", "v1")
        one.commit()

        two = self.store.begin()
        two.put("k", "v2")
        two.commit()

        reader = self.store.begin()
        self.assertEqual(reader.get("k"), "v2")

        three = self.store.begin()
        three.put("k", "v3")
        three.commit()

        self.assertEqual(self.store.gc(), 1)
        self.assertEqual(reader.get("k"), "v2")
        self.assertEqual(
            [version.value for version in self.store.versions_of("k")], ["v3", "v2"]
        )

        fresh = self.store.begin()
        self.assertEqual(fresh.get("k"), "v3")

    def test_read_only_transaction_does_not_hold_back_gc(self):
        one = self.store.begin()
        one.put("k", "v1")
        one.commit()

        reader = self.store.begin(read_only=True)
        self.assertEqual(self.store.active_txn_ids(), [reader.txn_id])

        two = self.store.begin()
        two.put("k", "v2")
        two.commit()

        three = self.store.begin()
        three.put("k", "v3")
        three.commit()

        self.assertEqual(self.store.active_txn_ids(), [reader.txn_id])

        self.assertEqual(self.store.gc(), 2)
        self.assertEqual(
            [version.value for version in self.store.versions_of("k")], ["v3"]
        )

        fresh = self.store.begin()
        self.assertEqual(fresh.get("k"), "v3")


class TransactionStateTest(unittest.TestCase):
    def setUp(self):
        self.store = MVCCStore()

    def test_finished_or_read_only_transactions_reject_writes(self):
        txn = self.store.begin()
        txn.put("k", 1)
        txn.commit()

        with self.assertRaises(TransactionError):
            txn.put("k", 2)
        with self.assertRaises(TransactionError):
            txn.get("k")

        observer = self.store.begin(read_only=True)
        with self.assertRaises(TransactionError):
            observer.put("other", 1)
        with self.assertRaises(TransactionError):
            observer.delete("other")

        observer.commit()
        with self.assertRaises(TransactionError):
            self.store.abort(observer)


if __name__ == "__main__":
    unittest.main()
