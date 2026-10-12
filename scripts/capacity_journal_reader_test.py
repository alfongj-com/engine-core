"""Actual SQLite WAL lifecycle regressions; no nodes or incident files."""
from collections import Counter
import json
import hashlib
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import capacity_campaign as campaign


class JournalReaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='engine-journal-reader-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'live.sqlite'
        self.writer = sqlite3.connect(self.path)
        self.addCleanup(self.writer.close)
        self.writer.execute('PRAGMA journal_mode=WAL')
        self.writer.execute('PRAGMA wal_autocheckpoint=0')
        self.writer.executescript('''
            CREATE TABLE control(singleton INTEGER PRIMARY KEY, halted INTEGER, reason TEXT);
            INSERT INTO control VALUES(1,1,'Redis checkpoint mirror failed');
            CREATE TABLE chain_halts(chain_id TEXT PRIMARY KEY, reason TEXT);
            CREATE TABLE admissions(id TEXT PRIMARY KEY,kind TEXT,state TEXT,replay_key TEXT);
            CREATE TABLE attempts(sequence INTEGER PRIMARY KEY,id TEXT,replay_key TEXT,payload TEXT);
            CREATE TABLE terminal_evidence(sequence INTEGER PRIMARY KEY,id TEXT,evidence TEXT);
            INSERT INTO admissions VALUES('intent-1','eoa','admitted','evm:1:fixture:0');
            INSERT INTO attempts VALUES(1,'intent-1','evm:1:fixture:0','{}');
        ''')
        self.writer.commit()

    def checkpointed_without_sidecars(self):
        # A consistent synthetic fixture: truncate only after all committed frames
        # are checkpointed, then copy the database to a fresh path, not a hot copy.
        self.assertEqual(self.writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone(), (0, 0, 0))
        dst = self.root / 'closed.sqlite'
        shutil.copyfile(self.path, dst)
        self.assertEqual(dst.read_bytes()[18:20], b'\x02\x02')
        self.assertFalse(Path(str(dst) + '-wal').exists())
        self.assertFalse(Path(str(dst) + '-shm').exists())
        return dst

    def test_closed_checkpointed_wal_without_sidecars_is_readable(self):
        p = self.checkpointed_without_sidecars()
        with campaign.journal_reader(p) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone(), ('ok',))
            self.assertEqual(db.execute('SELECT id FROM admissions').fetchall(), [('intent-1',)])
            self.assertEqual(db.execute('PRAGMA query_only').fetchone(), (1,))
            self.assertEqual(db.execute('PRAGMA synchronous').fetchone(), (2,))
            self.assertEqual(db.execute('PRAGMA fullfsync').fetchone(), (1,))

    def test_absent_database_is_not_created(self):
        p = self.root / 'missing.sqlite'
        with self.assertRaises(sqlite3.OperationalError):
            with campaign.journal_reader(p):
                self.fail('missing ledger yielded a connection')
        self.assertFalse(p.exists())

    def test_sql_mutations_fail_and_rows_remain(self):
        with campaign.journal_reader(self.path) as db:
            for sql in ("DELETE FROM admissions", "INSERT INTO admissions VALUES('bad','eoa','admitted',NULL)",
                        'CREATE TABLE bad(value TEXT)'):
                with self.subTest(sql=sql), self.assertRaises(sqlite3.OperationalError):
                    db.execute(sql)
        self.assertEqual(self.writer.execute('SELECT id FROM admissions').fetchall(), [('intent-1',)])

    def test_live_committed_wal_visible_uncommitted_and_rollback_hidden(self):
        self.writer.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        self.writer.execute("INSERT INTO admissions VALUES('committed','eoa','admitted',NULL)")
        self.writer.commit()
        self.assertGreater(Path(str(self.path) + '-wal').stat().st_size, 0)
        self.writer.execute("INSERT INTO admissions VALUES('uncommitted','eoa','admitted',NULL)")
        with campaign.journal_reader(self.path) as db:
            self.assertEqual(db.execute('SELECT id FROM admissions ORDER BY id').fetchall(), [('committed',), ('intent-1',)])
        self.writer.rollback()
        with campaign.journal_reader(self.path) as db:
            self.assertEqual(db.execute('SELECT id FROM admissions ORDER BY id').fetchall(), [('committed',), ('intent-1',)])

    def test_connection_closes_on_normal_and_exception_paths(self):
        for exceptional in (False, True):
            handle = None
            try:
                with campaign.journal_reader(self.path) as handle:
                    if exceptional:
                        raise RuntimeError('fixture')
            except RuntimeError:
                pass
            with self.assertRaises(sqlite3.ProgrammingError):
                handle.execute('SELECT 1')

    def test_reader_transaction_is_one_snapshot_then_next_reader_sees_commit(self):
        with campaign.journal_reader(self.path) as db:
            db.execute('BEGIN')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM admissions').fetchone(), (1,))
            self.writer.execute("INSERT INTO admissions VALUES('next','eoa','admitted',NULL)")
            self.writer.commit()
            self.assertEqual(db.execute('SELECT COUNT(*) FROM admissions').fetchone(), (1,))
        with campaign.journal_reader(self.path) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM admissions').fetchone(), (2,))

    def test_actual_observer_and_fence_can_read_closed_ledger(self):
        p = self.checkpointed_without_sidecars()
        observer = campaign.JournalObserver(p, {'intent-1': 'evm'}, time.monotonic(), {})
        self.assertEqual(observer.poll(), {'evm': {'admitted': 1, 'attempted': 1, 'terminal': 0}})
        c = campaign.Campaign.__new__(campaign.Campaign)
        c.journal, c.report = p, {}
        self.assertTrue(c.capture_durable_fences())
        self.assertTrue(c.report['journal_halted'])
        self.assertEqual(c.report['journal_halt_category'], 'redis_checkpoint_mirror_failed')

    def test_actual_custody_backup_of_closed_ledger_keeps_original_verdict(self):
        c = campaign.Campaign.__new__(campaign.Campaign)
        c.journal, c.logs = self.checkpointed_without_sidecars(), self.root
        c.report = {'outcome': 'fail_closed_recovery_required', 'oracle': {'safety_pass': None}}
        c.children, c.profiles, c.proxies = {}, {}, {}
        c.errors, c.statuses = [], {'intent-1': 202}
        c.responses = {'evm': Counter({202: 1})}
        c.projection_namespace, c.redis_port = 'fixture', 1
        c.preserve_owned_anvil_history = mock.Mock()
        real_connect = sqlite3.connect
        backup_handles = []
        def connect(*args, **kwargs):
            handle = real_connect(*args, **kwargs)
            if args[0] == self.root / 'interrupted-custody.sqlite':
                backup_handles.append(handle)
            return handle
        with mock.patch.object(campaign, 'redis_command', return_value=0), mock.patch.object(campaign.sqlite3, 'connect', side_effect=connect):
            c.capture_interrupted_custody()
        evidence = c.report['custody']['journal']
        self.assertTrue(evidence['available'], evidence)
        self.assertEqual(len(backup_handles), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            backup_handles[0].execute('SELECT 1')
        self.assertEqual(evidence['sha256'], hashlib.sha256(Path(evidence['backup']).read_bytes()).hexdigest())
        self.assertEqual((evidence['admitted'], evidence['attempted_ids'], evidence['nonterminal']), (1, 1, 1))
        self.assertEqual(c.report['outcome'], 'fail_closed_recovery_required')
        self.assertIsNone(c.report['oracle']['safety_pass'])
        self.assertTrue(c.report['operator_review_required'])
        with campaign.journal_reader(Path(evidence['backup'])) as backup:
            self.assertEqual(backup.execute('SELECT reason FROM control').fetchone(), ('Redis checkpoint mirror failed',))


if __name__ == '__main__':
    unittest.main()
