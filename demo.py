"""A synthetic T3 database, for the tests and the README screenshot. No real data.

    python3 demo.py demo.sqlite
    python3 t3time.py --no-local --db demo=demo.sqlite --html demo.html --tz UTC
"""
import json
import os
import random
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta, timezone

# Only the tables and columns t3-time reads.
SCHEMA_SQL = """
create table projection_projects (project_id text primary key, title text);
create table orchestration_v2_projection_threads (thread_id text primary key, project_id text, title text,
    payload_json text);
create table orchestration_v2_projection_messages (message_id text primary key, thread_id text, role text,
    created_at text, payload_json text);
create table orchestration_v2_projection_runs (run_id text primary key, thread_id text, status text,
    requested_at text, completed_at text, payload_json text);
"""


def iso(d):
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (d.microsecond // 1000)


class FakeT3:
    def __init__(self, path):
        if os.path.exists(path):
            os.remove(path)  # always a fresh database
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA_SQL)
        self.n = 0

    def _id(self):
        self.n += 1
        return str(uuid.UUID(int=self.n))

    def project(self, title):
        pid = self._id()
        self.db.execute("insert into projection_projects values (?, ?)", (pid, title))
        return pid

    def thread(self, project_id, title, settled_at=None, unsettled_at=None):
        tid = self._id()
        payload = {"settledAt": settled_at and iso(settled_at), "unsettledAt": unsettled_at and iso(unsettled_at)}
        self.db.execute("insert into orchestration_v2_projection_threads values (?, ?, ?, ?)",
                        (tid, project_id, title, json.dumps(payload)))
        return tid

    def message(self, thread_id, at, text="please fix it", created_by="user", source="web", role="user", **extra):
        mid = self._id()
        payload = {"createdBy": created_by, "creationSource": source, "text": text, "attachments": [], **extra}
        self.db.execute("insert into orchestration_v2_projection_messages values (?, ?, ?, ?, ?)",
                        (mid, thread_id, role, iso(at), json.dumps(payload)))
        return mid

    def run(self, thread_id, requested, completed, started=None, status="completed"):
        payload = {"startedAt": iso(started or requested)}
        self.db.execute("insert into orchestration_v2_projection_runs values (?, ?, ?, ?, ?, ?)",
                        (self._id(), thread_id, status, iso(requested), completed and iso(completed),
                         json.dumps(payload)))

    def close(self):
        self.db.commit()
        self.db.close()


PROJECTS = {
    "api-server": ["Fix login redirect loop", "Add rate limiting to /search", "Migrate sessions to Redis",
                   "Flaky test in billing", "Upgrade to Postgres 17", "Explain the auth middleware"],
    "website": ["Dark mode for the docs", "Pricing page copy", "Broken links after the redesign",
                "Lighthouse score below 80"],
    "mobile-app": ["Push notifications on Android", "Crash on iOS 19 launch", "Offline cache for the feed"],
    "infra": ["Nightly backup check", "Move CI to the new runner", "TLS renewal failed"],
    "No project": ["What does this error mean", "Compare two charting libraries", "Draft a release note",
                   "Rename photos by date", "Summarise this RFC"],
}


def make_demo(path, seed=7, days=28, end=datetime(2026, 3, 27, 18, 0, tzinfo=timezone.utc)):
    rnd = random.Random(seed)
    t3 = FakeT3(path)
    threads = []
    for name, titles in PROJECTS.items():
        pid = t3.project(name)
        for title in titles:
            settled = end if rnd.random() < 0.8 else None
            threads.append(t3.thread(pid, title, settled_at=settled))
    weights = [rnd.random() ** 2 + 0.05 for _ in threads]
    start = end - timedelta(days=days)
    for d in range(days):
        day = start + timedelta(days=d)
        if day.weekday() >= 5 and rnd.random() < 0.6:
            continue
        for _ in range(rnd.choice([1, 2, 2, 3])):  # sessions
            at = day.replace(hour=rnd.choice([8, 9, 10, 13, 14, 15, 16, 20, 22]), minute=rnd.randrange(60))
            current = rnd.choices(threads, weights)[0]
            for _ in range(rnd.randint(2, 14)):
                if rnd.random() < 0.55:
                    current = rnd.choices(threads, weights)[0]
                t3.message(current, at)
                if rnd.random() < 0.6:
                    t3.run(current, at, at + timedelta(seconds=rnd.randint(20, 900)))
                at += timedelta(minutes=rnd.choice([1, 2, 3, 4, 6, 8, 12]), seconds=rnd.randrange(60))
    t3.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python3 demo.py OUT.sqlite")
    make_demo(sys.argv[1])
