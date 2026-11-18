import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone

import t3time
from demo import FakeT3, make_demo

HERE = os.path.dirname(os.path.abspath(__file__))
UTC = t3time.Clock("UTC")


def at(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def source(path, label="test"):
    return {"label": label, "kind": "db", "data": t3time.extract(path)}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "statev2.sqlite")
        self.t3 = FakeT3(self.path)
        self.project = self.t3.project("demo")

    def tearDown(self):
        self.tmp.cleanup()

    def analyze(self, **kw):
        self.t3.close()
        return t3time.analyze([source(self.path)], clock=kw.pop("clock", UTC), **kw)


class OwnMessages(Base):
    def test_only_your_own_messages_count(self):
        a = self.t3.thread(self.project, "A")
        t = at("2026-03-02T10:00:00")
        m = lambda minute, **kw: self.t3.message(a, t.replace(minute=minute), **kw)
        m(0, source="web")                                          # yours
        m(1, source="server")                                       # yours, queued or an answer to a question
        m(2, source="web", text="Continue where you left off.")     # your click
        m(3, source="server", text="Continue where you left off.")  # T3 after a usage-limit reset
        m(4, idleCompaction="onReturn")                             # automatic /compact
        m(5, notification="Resumed thread", text="Resumed thread")
        m(6, created_by="agent", source="mcp")                      # delegated thread
        m(7, text="  ")                                             # empty
        m(8, text="", attachments=[{"name": "x.png"}])              # just an image: yours
        m(9, role="assistant", created_by="agent", source="provider")
        r = self.analyze()
        self.assertEqual(r["totals"]["messages"], 4)


class ActiveTime(Base):
    def test_gap_model(self):
        a = self.t3.thread(self.project, "A")
        b = self.t3.thread(self.project, "B")
        self.t3.message(a, at("2026-03-02T10:00:00"))
        self.t3.message(b, at("2026-03-02T10:08:00"))
        self.t3.message(a, at("2026-03-02T10:40:00"))
        r = self.analyze(idle_min=15)
        mins = {th["title"]: th["your_min"] for th in r["threads"]}
        self.assertEqual(mins, {"A": 2.0, "B": 8.0})  # A: two session starts, 1 min each; B: the 8 min gap
        self.assertEqual(r["totals"]["sessions"], 2)
        self.assertEqual(r["totals"]["switches"], 1)
        self.assertEqual(r["totals"]["switch_share"], 1.0)
        self.assertAlmostEqual(r["totals"]["your_hours"], 10 / 60)
        self.assertEqual(r["projects"][0]["threads"], 2)

    def test_idle_setting(self):
        a = self.t3.thread(self.project, "A")
        self.t3.message(a, at("2026-03-02T10:00:00"))
        self.t3.message(a, at("2026-03-02T10:20:00"))
        r = self.analyze(idle_min=30)
        self.assertEqual(r["threads"][0]["your_min"], 21.0)


class AgentRuns(Base):
    def test_wrote_elsewhere_or_nowhere(self):
        a = self.t3.thread(self.project, "A")
        b = self.t3.thread(self.project, "B")
        self.t3.message(a, at("2026-03-02T10:00:00"))
        self.t3.run(a, at("2026-03-02T10:00:01"), at("2026-03-02T10:10:00"))  # B written meanwhile
        self.t3.message(b, at("2026-03-02T10:05:00"))
        self.t3.run(b, at("2026-03-02T10:05:01"), at("2026-03-02T10:06:00"), started=at("2026-03-02T10:05:30"))
        self.t3.message(a, at("2026-03-02T11:00:00"))
        self.t3.run(a, at("2026-03-02T11:00:01"), at("2026-03-02T11:20:00"))  # only A itself meanwhile
        self.t3.message(a, at("2026-03-02T11:05:00"))
        self.t3.run(a, at("2026-03-02T12:00:00"), None, status="running")    # unfinished: no agent time
        r = self.analyze()
        t = r["totals"]
        self.assertEqual((t["runs_wrote_elsewhere"], t["runs_wrote_nowhere"]), (1, 1))  # B's 30 s run is ignored
        agent = {th["title"]: th["agent_min"] for th in r["threads"]}
        self.assertAlmostEqual(agent["B"], 0.5)  # from startedAt, not requested_at
        self.assertAlmostEqual(agent["A"], (599 + 1199) / 60)

    def test_agent_only_threads_add_agent_time_only(self):
        a = self.t3.thread(self.project, "A")
        child = self.t3.thread(self.project, "delegated")
        self.t3.message(a, at("2026-03-02T10:00:00"))
        self.t3.message(child, at("2026-03-02T10:00:05"), created_by="agent", source="mcp")
        self.t3.run(child, at("2026-03-02T10:00:06"), at("2026-03-02T10:30:06"))
        r = self.analyze()
        self.assertEqual(r["totals"]["threads"], 1)
        self.assertAlmostEqual(r["totals"]["agent_hours"], 0.5)


class Calendar(Base):
    def test_since_drops_earlier_messages_and_runs(self):
        a = self.t3.thread(self.project, "A")
        self.t3.message(a, at("2026-03-01T23:59:00"))
        self.t3.run(a, at("2026-03-01T23:59:01"), at("2026-03-02T00:30:00"))
        self.t3.message(a, at("2026-03-02T00:01:00"))
        r = self.analyze(since=date(2026, 3, 2))
        self.assertEqual(r["totals"]["messages"], 1)
        self.assertEqual(r["threads"][0]["your_min"], 1.0)  # opens a session: the 2 min gap is gone
        self.assertEqual(r["totals"]["agent_hours"], 0)
        self.assertEqual(r["days"][0]["date"], "2026-03-02")

    def test_days_and_hours_follow_daylight_saving(self):
        a = self.t3.thread(self.project, "A")
        self.t3.message(a, at("2026-10-24T22:30:00"))  # 00:30 CEST on 25 Oct
        self.t3.message(a, at("2026-10-26T23:30:00"))  # 00:30 CET on 27 Oct
        r = self.analyze(clock=t3time.Clock("Europe/Bratislava"))
        self.assertEqual(r["hours"][0], 2)
        self.assertEqual([d["date"] for d in r["days"] if d["msgs"]], ["2026-10-25", "2026-10-27"])
        self.assertEqual(len(r["days"]), 3)  # the empty day in between is listed too


class Threads(Base):
    def test_settled_unless_unsettled_later(self):
        p = self.project
        self.t3.message(self.t3.thread(p, "done", settled_at=at("2026-03-02T12:00:00")), at("2026-03-02T10:00:00"))
        self.t3.message(self.t3.thread(p, "reopened", settled_at=at("2026-03-02T12:00:00"),
                                       unsettled_at=at("2026-03-02T13:00:00")), at("2026-03-02T10:01:00"))
        self.t3.message(self.t3.thread(p, "settled again", settled_at=at("2026-03-02T14:00:00"),
                                       unsettled_at=at("2026-03-02T13:00:00")), at("2026-03-02T10:02:00"))
        r = self.analyze()
        self.assertEqual({th["title"]: th["settled"] for th in r["threads"]},
                         {"done": True, "reopened": False, "settled again": True})

    def test_same_database_twice_counts_once(self):
        a = self.t3.thread(self.project, "A")
        self.t3.message(a, at("2026-03-02T10:00:00"))
        self.t3.message(a, at("2026-03-02T10:05:00"))
        self.t3.close()
        r = t3time.analyze([source(self.path, "one"), source(self.path, "two")], clock=UTC)
        self.assertEqual(r["totals"]["messages"], 2)
        self.assertEqual(r["threads"][0]["machines"], ["one", "two"])


class Schema(unittest.TestCase):
    def test_unknown_schema_names_what_is_missing(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "old.sqlite")
            db = sqlite3.connect(p)
            db.executescript("create table projection_projects (project_id text, title text);"
                             "create table orchestration_v2_projection_threads (thread_id text, title text);")
            db.close()
            with self.assertRaises(t3time.Problem) as cm:
                t3time.extract(p)
            msg = str(cm.exception)
            self.assertIn("column orchestration_v2_projection_threads.project_id", msg)
            self.assertIn("table orchestration_v2_projection_messages", msg)

    def test_missing_database(self):
        with self.assertRaises(t3time.Problem):
            t3time.extract("/nonexistent/statev2.sqlite")

    def test_cli_reports_problem_without_traceback(self):
        r = subprocess.run([sys.executable, os.path.join(HERE, "t3time.py"), "--no-local", "--db", "/nope.sqlite"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)
        self.assertNotIn("Traceback", r.stderr)
        self.assertIn("no T3 database at /nope.sqlite", r.stderr)

    def test_unwritable_html_is_one_line(self):
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "demo.sqlite")
            make_demo(db)
            r = subprocess.run([sys.executable, os.path.join(HERE, "t3time.py"), "--no-local", "--db", db,
                                "--db", "/nope.sqlite", "--html", "/nonexistent/x.html"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)
        self.assertNotIn("Traceback", r.stderr)
        self.assertIn("skipped /nope.sqlite", r.stderr)
        self.assertIn("cannot write /nonexistent/x.html", r.stderr)


class EndToEnd(unittest.TestCase):
    def test_demo_database_renders(self):
        with tempfile.TemporaryDirectory() as d:
            db, page = os.path.join(d, "demo.sqlite"), os.path.join(d, "demo.html")
            make_demo(db)
            out = subprocess.run([sys.executable, os.path.join(HERE, "t3time.py"), "--no-local", "--db",
                                  "demo=" + db, "--tz", "UTC", "--html", page], capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertIn("Your time", out.stdout)
            self.assertIn("api-server", out.stdout)
            with open(page, encoding="utf-8") as f:
                self.assertIn("prefers-color-scheme:dark", f.read())
            js = subprocess.run([sys.executable, os.path.join(HERE, "t3time.py"), "--no-local", "--db", db,
                                 "--tz", "UTC", "--json"], capture_output=True, text=True)
            r = json.loads(js.stdout)
            self.assertEqual(r["totals"]["messages"], sum(day["msgs"] for day in r["days"]))
            self.assertEqual(r["totals"]["messages"], sum(r["hours"]))
            self.assertAlmostEqual(r["totals"]["your_hours"] * 60, sum(t["your_min"] for t in r["threads"]))

    def test_extract_over_stdin_like_ssh(self):
        # --ssh runs exactly this on the other machine: the source on stdin, JSON back
        with tempfile.TemporaryDirectory() as d:
            db = os.path.join(d, "demo.sqlite")
            make_demo(db)
            with open(os.path.join(HERE, "t3time.py"), "rb") as f:
                out = subprocess.run([sys.executable, "-", "--extract", "--db", db], input=f.read(),
                                     capture_output=True, cwd=d)
            self.assertEqual(out.returncode, 0, out.stderr)
            data = json.loads(out.stdout)
            self.assertEqual(data["t3time"], t3time.FORMAT)
            self.assertGreater(data["own"], 50)


if __name__ == "__main__":
    unittest.main()
