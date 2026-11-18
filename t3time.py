#!/usr/bin/env python3
"""t3-time: how much of *your* time each T3 Code thread and project takes.

Reads T3 Code's local database read-only, once, and prints a summary. Nothing runs in the background.

    python3 t3time.py                         # this machine
    python3 t3time.py --ssh laptop --ssh box  # plus other machines (needs python3 there)
    python3 t3time.py --db pc=./statev2.sqlite --html time.html

Standard library only, Python 3.9+.
"""
import argparse
import bisect
import html
import json
import os
import socket
import sqlite3
import subprocess
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

FORMAT = 1                # version of the --extract JSON
FIRST_CREDIT = 60         # seconds credited to the message that opens a session
RUN_MIN = 60              # agent runs shorter than this are ignored in "while agents ran"
LONG_THREAD = 11          # messages that make a thread "long"
OPEN_MIN = 20             # minutes of your time that make an unsettled thread worth listing
DEFAULT_DB = os.path.join("~", ".t3", "userdata", "statev2.sqlite")
ESTIMATE = ("Hours are an estimate: a gap of up to {idle} min between two of your messages counts as active time "
            "(like WakaTime), because T3 does not record typing, focus or reading; expect roughly ±30 %. "
            "Message counts and times, their order, agent run times and settled state are exact.")

# What t3-time reads. A database without these is a T3 version it does not know.
SCHEMA = {
    "orchestration_v2_projection_threads": ["thread_id", "project_id", "title", "payload_json"],
    "orchestration_v2_projection_messages": ["message_id", "thread_id", "role", "created_at", "payload_json"],
    "orchestration_v2_projection_runs": ["run_id", "thread_id", "status", "requested_at", "completed_at",
                                         "payload_json"],
    "projection_projects": ["project_id", "title"],
}


class Problem(Exception):
    """An error shown to the user as one line, without a traceback."""


# ---------------------------------------------------------------- extraction (also runs on remote machines)

def parse_ts(s):
    """ISO time from the database -> unix seconds, tolerant of 'Z' and any number of fraction digits."""
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")
    main, sep, rest = s.partition(".")
    if sep:
        digits = len(rest) - len(rest.lstrip("0123456789"))
        frac, rest = rest[:digits], rest[digits:]
        s = main + "." + (frac + "000000")[:6] + rest
    d = datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


def loads(text):
    try:
        value = json.loads(text) if text else {}
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def is_own_message(p):
    """True for a message you wrote yourself, from a user-role message payload."""
    if p.get("createdBy") != "user" or p.get("idleCompaction") or p.get("notification"):
        return False  # agents, delegated threads, T3's automatic /compact and "Resumed thread" notices
    text = (p.get("text") or "").strip()
    if p.get("creationSource") == "server" and text == "Continue where you left off.":
        return False  # T3 resuming threads by itself after a usage-limit reset (the web button is you)
    return bool(text or p.get("attachments"))


def is_settled(p):
    settled, unsettled = p.get("settledAt"), p.get("unsettledAt")
    return bool(settled) and not (unsettled and parse_ts(unsettled) > parse_ts(settled))


def check_schema(db, path):
    tables = {r[0] for r in db.execute("select name from sqlite_master where type in ('table','view')")}
    missing = []
    for table, cols in SCHEMA.items():
        if table not in tables:
            missing.append("table " + table)
            continue
        have = {r[1] for r in db.execute("pragma table_info(%s)" % table)}
        missing += ["column %s.%s" % (table, c) for c in cols if c not in have]
    if missing:
        raise Problem("%s does not look like a T3 Code database t3-time knows (missing: %s). T3 has probably "
                      "changed its schema; please open an issue with your T3 version." % (path, ", ".join(missing)))


def extract(path):
    """Read one T3 database read-only -> plain data (what --extract prints)."""
    path = os.path.expanduser(path)
    if not os.path.isfile(path):
        raise Problem("no T3 database at %s" % path)
    try:
        db = sqlite3.connect("file:%s?mode=ro" % quote(os.path.abspath(path)), uri=True)
        check_schema(db, path)
        projects = dict(db.execute("select project_id, title from projection_projects"))
        threads = {}
        for tid, pid, title, payload in db.execute(
                "select thread_id, project_id, title, payload_json from orchestration_v2_projection_threads"):
            threads[tid] = {"id": tid, "project": projects.get(pid) or "No project", "title": title or "(untitled)",
                            "settled": is_settled(loads(payload)), "msgs": [], "runs": []}
        user_rows = 0
        for mid, tid, created, payload in db.execute(
                "select message_id, thread_id, created_at, payload_json "
                "from orchestration_v2_projection_messages where role = 'user'"):
            user_rows += 1
            if tid in threads and created and is_own_message(loads(payload)):
                threads[tid]["msgs"].append([mid, created])
        for rid, tid, status, requested, completed, payload in db.execute(
                "select run_id, thread_id, status, requested_at, completed_at, payload_json "
                "from orchestration_v2_projection_runs"):
            if tid in threads and (requested or completed):
                started = loads(payload).get("startedAt") or requested
                threads[tid]["runs"].append([rid, started, completed, status])
        db.close()
    except sqlite3.Error as e:
        raise Problem("cannot read %s: %s" % (path, e))
    kept = [t for t in threads.values() if t["msgs"] or t["runs"]]
    own = sum(len(t["msgs"]) for t in kept)
    return {"t3time": FORMAT, "host": socket.gethostname(), "user_rows": user_rows, "own": own, "threads": kept}


# ---------------------------------------------------------------- sources

def load_sources(args):
    jobs = []
    if not args.no_local:
        jobs.append(("local", socket.gethostname(), lambda: extract(DEFAULT_DB)))
    for spec in args.db:
        label, sep, path = spec.partition("=")
        if not sep or os.sep in label or "/" in label:
            label, path = spec, spec
        jobs.append(("db", label, lambda p=path: extract(p)))
    for host in args.ssh:
        jobs.append(("ssh", host, lambda h=host: extract_ssh(h)))
    if not jobs:
        raise Problem("nothing to read: drop --no-local or add --db / --ssh")
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [(kind, label, pool.submit(fn)) for kind, label, fn in jobs]
    sources, skipped = [], []
    for kind, label, fut in futures:
        try:
            data = fut.result()
        except Problem as e:
            skipped.append({"label": label, "reason": str(e)})
            print("t3-time: skipped %s: %s" % (label, e), file=sys.stderr)
            continue
        if data.get("user_rows") and not data.get("own"):
            print("t3-time: %s has user messages but none matched your own; T3 may have changed how it stores "
                  "them, please open an issue" % label, file=sys.stderr)
        sources.append({"label": label, "kind": kind, "data": data})
    if not sources:
        raise Problem("no database could be read")
    return sources, skipped


def extract_ssh(host):
    with open(os.path.abspath(__file__), "rb") as f:
        src = f.read()
    try:
        r = subprocess.run(["ssh", "-o", "ConnectTimeout=10", host, "python3", "-", "--extract"],
                           input=src, capture_output=True, timeout=300)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise Problem("ssh failed: %s" % e)
    if r.returncode != 0:
        err = r.stderr.decode(errors="replace").strip().splitlines()
        raise Problem(err[-1] if err else "ssh exited with %d" % r.returncode)
    try:
        data = json.loads(r.stdout)
    except ValueError:
        raise Problem("unexpected output from python3 on %s" % host)
    if data.get("t3time") != FORMAT:
        raise Problem("unexpected extract format from %s" % host)
    return data


# ---------------------------------------------------------------- the model

class Clock:
    """Turns unix seconds into local days and hours (zoneinfo, so DST is right on every day)."""

    def __init__(self, tz):
        self.name = tz or "local time"
        if tz:
            try:
                from zoneinfo import ZoneInfo
                self.zone = ZoneInfo(tz)
            except Exception:
                raise Problem("unknown time zone %r (on Windows: pip install tzdata)" % tz)
        else:
            self.zone = None  # the system's own rules, via the C library

    def local(self, t):
        return datetime.fromtimestamp(t, self.zone) if self.zone else datetime.fromtimestamp(t)

    def midnight(self, day):
        d = datetime(day.year, day.month, day.day)
        return (d.replace(tzinfo=self.zone) if self.zone else d).timestamp()


def analyze(sources, idle_min=15, since=None, clock=None):
    clock = clock or Clock(None)
    idle = idle_min * 60
    start = clock.midnight(since) if since else None
    multi = len(sources) > 1

    threads, seen_msgs, seen_runs = {}, set(), set()
    for src in sources:
        for t in src["data"]["threads"]:
            th = threads.setdefault(t["id"], {"title": t["title"], "project": t["project"], "settled": False,
                                              "machines": [], "msgs": [], "agent": 0.0, "runs": []})
            th["settled"] = th["settled"] or t["settled"]
            if src["label"] not in th["machines"]:
                th["machines"].append(src["label"])
            # The same machine given twice, or a thread copied between machines, counts once.
            for mid, created in t["msgs"]:
                ts = parse_ts(created)
                if mid not in seen_msgs and (start is None or ts >= start):
                    seen_msgs.add(mid)
                    th["msgs"].append(ts)
            for rid, started, completed, _status in t["runs"]:
                a, c = parse_ts(started), parse_ts(completed)
                if rid in seen_runs or a is None or c is None or (start is not None and a < start):
                    continue
                seen_runs.add(rid)
                th["agent"] += max(0.0, c - a)
                th["runs"].append((a, c))

    stream = sorted((ts, tid) for tid, th in threads.items() for ts in th["msgs"])
    times = [ts for ts, _ in stream]
    for th in threads.values():
        th["msgs"].sort()

    active = defaultdict(float)
    days = defaultdict(lambda: {"active": 0.0, "msgs": 0, "threads": set(), "switches": 0})
    hours = [0] * 24
    sessions = switches = 0
    prev = None
    for ts, tid in stream:
        lt = clock.local(ts)
        day = days[lt.date()]
        day["msgs"] += 1
        day["threads"].add(tid)
        hours[lt.hour] += 1
        if prev and ts - prev[0] <= idle:
            gap = ts - prev[0]
            if tid != prev[1]:
                switches += 1
                day["switches"] += 1
        else:
            gap = FIRST_CREDIT
            sessions += 1
        active[tid] += gap
        day["active"] += gap
        prev = (ts, tid)

    # While an agent ran for more than a minute in one of your threads: did you write elsewhere, or nowhere?
    elsewhere = nowhere = 0
    for tid, th in threads.items():
        own = th["msgs"]
        for a, c in th["runs"] if own else ():
            if c - a < RUN_MIN:
                continue
            total = bisect.bisect_left(times, c) - bisect.bisect_right(times, a)
            mine = bisect.bisect_left(own, c) - bisect.bisect_right(own, a)
            if total > mine:
                elsewhere += 1
            else:
                nowhere += 1

    rows = []
    for tid, th in threads.items():
        rows.append({"id": tid, "title": th["title"], "project": th["project"], "machines": th["machines"],
                     "settled": th["settled"], "your_min": active[tid] / 60, "msgs": len(th["msgs"]),
                     "agent_min": th["agent"] / 60,
                     "first": th["msgs"][0] if th["msgs"] else None, "last": th["msgs"][-1] if th["msgs"] else None})
    rows.sort(key=lambda r: (-r["your_min"], -r["agent_min"]))
    yours = [r for r in rows if r["msgs"]]

    total_min = sum(active.values()) / 60
    agent_min = sum(r["agent_min"] for r in rows)
    projects = defaultdict(lambda: {"your_min": 0.0, "agent_min": 0.0, "threads": 0, "msgs": 0, "settled": 0})
    for r in rows:
        p = projects[r["project"]]
        p["your_min"] += r["your_min"]
        p["agent_min"] += r["agent_min"]
        if r["msgs"]:
            p["threads"] += 1
            p["msgs"] += r["msgs"]
            p["settled"] += r["settled"]
    project_rows = [{"project": k, **v, "share": v["your_min"] / total_min if total_min else 0.0,
                     "msgs_per_thread": v["msgs"] / v["threads"] if v["threads"] else 0.0}
                    for k, v in projects.items() if v["threads"]]
    project_rows.sort(key=lambda p: -p["your_min"])

    day_rows = []
    if days:
        d, end = min(days), max(days)
        while d <= end:  # every calendar day in the range, including days without messages
            v = days.get(d)
            day_rows.append({"date": d.isoformat(), "your_min": v["active"] / 60 if v else 0.0,
                             "msgs": v["msgs"] if v else 0, "threads": len(v["threads"]) if v else 0,
                             "switches": v["switches"] if v else 0})
            d += timedelta(days=1)

    long_ = [r for r in yours if r["msgs"] >= LONG_THREAD]
    open_ = [r for r in yours if not r["settled"] and r["your_min"] > OPEN_MIN]
    follow_ups = len(stream) - sessions
    active_days = sum(1 for d in day_rows if d["msgs"])
    return {
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "timezone": clock.name, "idle_min": idle_min, "since": since.isoformat() if since else None,
        "multi": multi, "sources": [s["label"] for s in sources],
        "estimate": ESTIMATE.format(idle=idle_min),
        "totals": {
            "your_hours": total_min / 60, "agent_hours": agent_min / 60,
            "agent_ratio": agent_min / total_min if total_min else None,
            "agent_hours_not_yours": sum(r["agent_min"] for r in rows if not r["msgs"]) / 60,
            "messages": len(stream), "threads": len(yours), "sessions": sessions, "switches": switches,
            "switch_share": switches / follow_ups if follow_ups else None,
            "days": len(day_rows), "active_days": active_days,
            "hours_per_active_day": total_min / 60 / active_days if active_days else 0.0,
            "settled_threads": sum(r["settled"] for r in yours),
            "long_threads": len(long_), "long_hours": sum(r["your_min"] for r in long_) / 60,
            "long_share": sum(r["your_min"] for r in long_) / total_min if total_min else None,
            "open_threads": len(open_), "open_hours": sum(r["your_min"] for r in open_) / 60,
            "runs_wrote_elsewhere": elsewhere, "runs_wrote_nowhere": nowhere,
            "first": stream[0][0] if stream else None, "last": stream[-1][0] if stream else None,
        },
        "projects": project_rows, "days": day_rows, "hours": hours,
        "threads": yours, "open": open_[:10],
    }


# ---------------------------------------------------------------- output

def fh(h):
    return "%.1f h" % h


def pct(x):
    return "–" if x is None else "%.0f %%" % (100 * x)


def spark(values):
    blocks = " ▁▂▃▄▅▆▇█"
    top = max(values) if values else 0
    return "".join(blocks[0 if not v else max(1, round(8 * v / top))] for v in values) if top else ""


def cut(s, n):
    return s if len(s) <= n else s[:n - 1] + "…"


def render_text(r, top=10):
    t = r["totals"]
    out = []
    if not t["messages"]:
        out.append("No messages of yours found%s." % (" since " + r["since"] if r["since"] else ""))
        return "\n".join(out)
    days = r["days"]
    out.append("t3-time · %s · %s – %s (%d days) · %s" % (
        ", ".join(r["sources"]), days[0]["date"], days[-1]["date"], t["days"], r["timezone"]))
    out.append("")
    ratio = " (%.1f× your time)" % t["agent_ratio"] if t["agent_ratio"] else ""
    lines = [
        ("Your time", fh(t["your_hours"]), "%.1f h per active day, %d active days" % (t["hours_per_active_day"],
                                                                                    t["active_days"])),
        ("Agent time", fh(t["agent_hours"]), "agent runs added up" + ratio + (
            ", %s of it in threads you never wrote in" % fh(t["agent_hours_not_yours"])
            if t["agent_hours_not_yours"] >= 0.05 else "")),
        ("Your messages", "{:,}".format(t["messages"]), "in %d threads, %d sessions" % (t["threads"], t["sessions"])),
        ("Switching", pct(t["switch_share"]), "of your next messages in a session went to another thread"),
        ("Long threads", str(t["long_threads"]), "with %d+ messages took %s (%s of your time)" % (
            LONG_THREAD, fh(t["long_hours"]), pct(t["long_share"]))),
        ("Settled", "%d / %d" % (t["settled_threads"], t["threads"]),
         "threads are settled (ended, by you or an agent; not the same as \"it worked\")"),
        ("Left open", str(t["open_threads"]), "unsettled threads with over %d min of your time (%s)" % (
            OPEN_MIN, fh(t["open_hours"]))),
    ]
    for label, value, note in lines:
        out.append("  %-14s %9s  %s" % (label, value, note))
    out.append("  While an agent ran over a minute: you wrote in another thread %d×, wrote nowhere %d× "
               "(waiting, reading, or away)." % (t["runs_wrote_elsewhere"], t["runs_wrote_nowhere"]))

    out.append("")
    out.append("  %-30s %9s %6s %9s %8s %8s" % ("By project", "your time", "", "agents", "threads", "msg/thr"))
    for p in r["projects"]:
        out.append("  %-30s %9s %6s %9s %8d %8.1f" % (cut(p["project"], 30), fh(p["your_min"] / 60), pct(p["share"]),
                                                    fh(p["agent_min"] / 60), p["threads"], p["msgs_per_thread"]))

    busiest = max(days, key=lambda d: d["your_min"])
    out.append("")
    out.append("  By day     %s   busiest %s, %s" % (spark([d["your_min"] for d in days]), busiest["date"],
                                                     fh(busiest["your_min"] / 60)))
    out.append("  By hour    %s   messages per hour, 0–23" % spark(r["hours"]))

    out.append("")
    out.append("  %-40s %-22s %6s %5s %7s %8s" % ("Top threads", "project", "you", "msgs", "agents", "settled"))
    for th in r["threads"][:top]:
        where = th["project"] + (" · " + ", ".join(th["machines"]) if r["multi"] else "")
        out.append("  %-40s %-22s %5.0fm %5d %6.0fm %8s" % (
            cut(th["title"], 40), cut(where, 22), th["your_min"], th["msgs"], th["agent_min"],
            "yes" if th["settled"] else "no"))
    out.append("")
    out.append(r["estimate"])
    return "\n".join(out)


CSS = """
:root{color-scheme:light;--bg:#fcfcfb;--card:#f2f1ee;--line:#e4e2dd;--text:#0b0b0b;--text-2:#52514e;--muted:#7a7873;--series:#2a78d6;--tip:#ffffff}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--bg:#1a1a19;--card:#242422;--line:#353531;--text:#f0efec;--text-2:#c3c2b7;--muted:#8f8d85;--series:#3987e5;--tip:#2c2c2a}}
:root[data-theme="dark"]{color-scheme:dark;--bg:#1a1a19;--card:#242422;--line:#353531;--text:#f0efec;--text-2:#c3c2b7;--muted:#8f8d85;--series:#3987e5;--tip:#2c2c2a}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:14.5px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
.wrap{max-width:1040px;margin:0 auto;padding:32px 20px 48px}
h1{font-size:26px;margin:0 0 4px;letter-spacing:-.01em}
h2{font-size:17px;margin:34px 0 10px}
.sub{color:var(--text-2);margin:0 0 22px;max-width:780px}
.lead{font-size:17px;margin:0 0 22px;max-width:820px}
.facts{display:grid;grid-template-columns:1fr 1fr;gap:10px 28px;margin:0 0 28px;padding:0}
.facts li{list-style:none;color:var(--text-2);border-top:1px solid var(--line);padding-top:8px}
.facts b{color:var(--text);font-variant-numeric:tabular-nums}
.chart{background:var(--card);border-radius:16px;padding:18px 18px 10px}
.chart .t{font-weight:600;margin-bottom:2px}
.chart .s{color:var(--text-2);font-size:13px;margin-bottom:14px}
.row{display:grid;grid-template-columns:220px 1fr 120px;align-items:center;gap:12px;padding:3px 0;position:relative;outline:none}
.lab{text-align:right;font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.track{height:16px}
.bar{height:100%;background:var(--series);border-radius:0 4px 4px 0;min-width:2px}
.val{font-variant-numeric:tabular-nums;font-size:14px}
.val span{color:var(--text-2);margin-left:6px}
.tip{display:none;position:absolute;left:232px;top:100%;z-index:5;background:var(--tip);border:1px solid var(--line);border-radius:10px;padding:9px 12px;font-size:13px;box-shadow:0 6px 24px rgba(0,0,0,.14);max-width:560px}
.tip .m{color:var(--text-2)}
.row:hover .tip,.row:focus .tip,.col:hover .tip,.col:focus .tip{display:block}
.row:hover .bar,.row:focus .bar,.col:hover .cb,.col:focus .cb{filter:brightness(1.12)}
.two{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:24px}
.cols{display:flex;align-items:flex-end;gap:2px;height:150px;padding-top:6px}
.col{flex:1;min-width:0;height:100%;display:flex;flex-direction:column;position:relative;outline:none}
.plot{flex:1;display:flex;align-items:flex-end}
.cb{width:100%;background:var(--series);border-radius:3px 3px 0 0;min-height:1px}
.cb.z{background:var(--line)}
.cl{font-size:11px;color:var(--muted);text-align:center;height:16px;margin-top:4px;font-variant-numeric:tabular-nums;white-space:nowrap}
.col .tip{left:50%;transform:translateX(-50%);top:auto;bottom:calc(100% + 4px);white-space:nowrap}
table{border-collapse:collapse;width:100%;font-size:14px}
th{text-align:left;color:var(--text-2);font-weight:600;font-size:12.5px;border-bottom:1px solid var(--line);padding:7px 8px}
td{border-bottom:1px solid var(--line);padding:8px;vertical-align:top}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
td .d{color:var(--text-2);font-size:12.5px}
.box{background:var(--card);border-radius:14px;padding:14px 16px}
.box ul{margin:6px 0;padding-left:20px}.box li{margin:4px 0}
footer{color:var(--muted);font-size:12.5px;margin-top:28px}
@media (max-width:720px){.row{grid-template-columns:110px 1fr 80px}.val span{display:none}.two,.facts{grid-template-columns:1fr}.tip{left:0}th.hide,td.hide{display:none}}
"""


def render_html(r, top=15):
    e = html.escape
    t = r["totals"]
    if not t["messages"]:
        body = "<h1>Your time in T3 Code</h1><p class='sub'>No messages of yours found.</p>"
        return page(body)
    days, projects = r["days"], r["projects"]
    pmax = max(p["your_min"] for p in projects) or 1
    proj = "".join(
        '<div class="row" tabindex="0"><div class="lab">%s</div><div class="track"><div class="bar" style="width:%.1f%%">'
        '</div></div><div class="val">%s<span>%s</span></div><div class="tip"><b>%s</b><br>your time %s, agents ran %s'
        '<br><span class="m">%d threads, %d of your messages (%.1f per thread), %d settled</span></div></div>' % (
            e(p["project"]), 100 * p["your_min"] / pmax, fh(p["your_min"] / 60), pct(p["share"]), e(p["project"]),
            fh(p["your_min"] / 60), fh(p["agent_min"] / 60), p["threads"], p["msgs"], p["msgs_per_thread"],
            p["settled"])
        for p in projects)

    dmax = max(d["your_min"] for d in days) or 1
    step = max(1, round(len(days) / 8))
    day_cols = "".join(
        '<div class="col" tabindex="0"><div class="plot"><div class="cb%s" style="height:%.1f%%"></div></div><div class="cl">%s</div>'
        '<div class="tip"><b>%s</b> %s<br><span class="m">%d threads, %d messages, %d switches</span></div></div>' % (
            "" if d["msgs"] else " z", 100 * d["your_min"] / dmax, short_day(d["date"]) if i % step == 0 else "",
            short_day(d["date"], True), fh(d["your_min"] / 60), d["threads"], d["msgs"], d["switches"])
        for i, d in enumerate(days))
    hmax = max(r["hours"]) or 1
    hour_cols = "".join(
        '<div class="col" tabindex="0"><div class="plot"><div class="cb" style="height:%.1f%%"></div></div><div class="cl">%s</div>'
        '<div class="tip"><b>%d:00–%d:59</b><br><span class="m">%d messages</span></div></div>' % (
            100 * n / hmax, h if h % 3 == 0 else "", h, h, n)
        for h, n in enumerate(r["hours"]))

    rows = "".join(
        '<tr><td>%s<div class="d">%s</div></td><td class="n">%.0f</td><td class="n">%d</td><td class="n hide">%.0f</td>'
        '<td class="n">%s</td></tr>' % (
            e(cut(th["title"], 80)), e(th["project"] + (" · " + ", ".join(th["machines"]) if r["multi"] else "")),
            th["your_min"], th["msgs"], th["agent_min"], "yes" if th["settled"] else "no")
        for th in r["threads"][:top])
    open_list = ", ".join(e(cut(th["title"], 50)) for th in r["open"][:3])
    busiest = max(days, key=lambda d: d["your_min"])
    ratio = (", %.1f× your time" % t["agent_ratio"]) if t["agent_ratio"] else ""
    skipped = "".join(" · skipped %s (%s)" % (e(s["label"]), e(s["reason"])) for s in r.get("skipped", []))
    not_yours = " (%s here)" % fh(t["agent_hours_not_yours"]) if t["agent_hours_not_yours"] >= 0.05 else ""
    night = sum(n for h, n in enumerate(r["hours"]) if h >= 23 or h < 5)

    body = f"""
<h1>Your time in T3 Code</h1>
<p class="sub">{e(", ".join(r["sources"]))} · {short_day(days[0]["date"], True)} – {short_day(days[-1]["date"], True)}, {t["days"]} days · {e(r["timezone"])}{skipped}. Time comes from when you sent messages, not from when a window was open.</p>
<!-- avoid-ai-design-ignore: L4 (measured numbers in one sentence, not a stat strip) -->
<p class="lead">You spent about <b>{fh(t["your_hours"])}</b> in T3 over {t["active_days"]} active days, {t["hours_per_active_day"]:.1f} h a day, while agents ran {fh(t["agent_hours"])}{ratio}. <b>{pct(t["switch_share"])}</b> of your next messages in a session went to a different thread than the one before.</p>
<ul class="facts">
<li><b>{t["messages"]:,}</b> of your messages in <b>{t["threads"]}</b> threads, {t["sessions"]} sessions</li>
<li><b>{t["long_threads"]}</b> threads with {LONG_THREAD}+ messages took <b>{fh(t["long_hours"])}</b>, {pct(t["long_share"])} of your time</li>
<li><b>{t["settled_threads"]}</b> of {t["threads"]} threads are settled; <b>{t["open_threads"]}</b> unsettled ones took over {OPEN_MIN} min each ({fh(t["open_hours"])}){": " + open_list if open_list else ""}</li>
<li>While an agent ran over a minute you wrote in another thread <b>{t["runs_wrote_elsewhere"]}×</b> and nowhere <b>{t["runs_wrote_nowhere"]}×</b></li>
</ul>

<div class="chart"><div class="t">Your time by project</div><div class="s">Hover a row for agent time, threads and messages per thread.</div>
{proj}</div>

<div class="two">
<div class="chart"><div class="t">Your time by day</div><div class="s">Busiest: {short_day(busiest["date"], True)}, {fh(busiest["your_min"] / 60)} in {busiest["threads"]} threads.</div><div class="cols">{day_cols}</div></div>
<div class="chart"><div class="t">When you write</div><div class="s">Your messages by hour of day; {night} between 23:00 and 5:00.</div><div class="cols">{hour_cols}</div></div>
</div>

<h2>Threads that took the most of your time</h2>
<table><tr><th>Thread</th><th class="n">your min</th><th class="n">messages</th><th class="n hide">agent min</th><th class="n">settled</th></tr>{rows}</table>

<h2>What this measures and what it cannot</h2>
<div class="box"><ul>
<li>Your messages from every database go into one stream. A gap of up to {r["idle_min"]} minutes between two of them counts as active time for the thread of the later one; the first message of a session gets one minute. WakaTime works the same way.</li>
<li>T3 does not record typing, focus or reading, so hours are an estimate, roughly ±30 %. Message counts and times, their order (switching), agent run times and settled state are read as they are.</li>
<li>Agent time adds up every agent run that finished, also in delegated or scheduled threads you never wrote in{not_yours}. They count in their project's agent time; projects and threads you never wrote in are not listed.</li>
<li>"Wrote nowhere" during a run can also mean you were away. "Settled" means a thread was ended, by you or by an agent, not that it worked.</li>
<li>Your messages are the ones T3 marks as yours, sent from the app or queued on the server. Agent and delegated messages, automatic /compact, "Resumed thread" notices and T3's own "Continue where you left off." after a usage-limit reset are left out.</li>
</ul></div>
<footer>Made with t3-time, which reads T3's database read-only. Generated {e(r["generated"])}.</footer>"""
    return page(body)


def short_day(iso, full=False):
    d = date.fromisoformat(iso)
    return d.strftime("%a %-d %b %Y" if full else "%-d %b") if os.name != "nt" else d.isoformat()


def page(body):
    return ('<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1"><title>T3 time</title>'
            '<style>%s</style></head><body><div class="wrap">%s\n</div></body></html>\n' % (CSS, body))


# ---------------------------------------------------------------- CLI

def main(argv=None):
    ap = argparse.ArgumentParser(prog="t3time.py", description="How much of your time each T3 Code thread and "
                                 "project takes. Reads T3's database read-only.")
    ap.add_argument("--ssh", action="append", default=[], metavar="HOST",
                    help="also read the T3 database on HOST over ssh (needs python3 3.9+ there); repeatable")
    ap.add_argument("--db", action="append", default=[], metavar="[LABEL=]PATH",
                    help="also read a copied statev2.sqlite; repeatable")
    ap.add_argument("--no-local", action="store_true", help="skip this machine's database")
    ap.add_argument("--since", type=date.fromisoformat, metavar="YYYY-MM-DD", help="only from this day on")
    ap.add_argument("--tz", metavar="ZONE", help="time zone for days and hours, e.g. Europe/Berlin (default: local)")
    ap.add_argument("--idle", type=int, default=15, metavar="MIN", help="longest gap that still counts (default 15)")
    ap.add_argument("--html", metavar="FILE", help="also write a standalone HTML page")
    ap.add_argument("--json", action="store_true", help="print the full results as JSON instead of the summary")
    ap.add_argument("--extract", action="store_true", help=argparse.SUPPRESS)  # what --ssh runs remotely
    args = ap.parse_args(argv)
    try:
        if args.extract:
            json.dump(extract(args.db[-1] if args.db else DEFAULT_DB), sys.stdout)
            return 0
        if args.idle < 1:
            raise Problem("--idle must be at least 1 minute")
        clock = Clock(args.tz)
        sources, skipped = load_sources(args)
        result = analyze(sources, args.idle, args.since, clock)
        result["skipped"] = skipped
        if args.html:
            try:
                with open(args.html, "w", encoding="utf-8") as f:
                    f.write(render_html(result))
            except OSError as e:
                raise Problem("cannot write %s: %s" % (args.html, e.strerror or e))
        if args.json:
            json.dump(result, sys.stdout, ensure_ascii=False, indent=1)
            print()
        else:
            print(render_text(result))
            if args.html:
                print("\nWrote " + args.html)
    except Problem as e:
        print("t3-time: %s" % e, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
