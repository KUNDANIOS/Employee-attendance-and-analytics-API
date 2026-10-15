"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (a local .env is read, but real environment variables win)

Layout of this file
    1. configuration + database handle
    2. validation helpers (dates, months, epoch milliseconds)
    3. time / business-rule helpers (R1 - R5)         <- pure functions, no database
    4. request models
    5. serialisation (BSON document -> API JSON)
    6. indexes
    7. endpoints: system, employees, attendance
    8. analytics pipelines + endpoints
    9. explain endpoint
"""
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("uvicorn.error")

# --------------------------------------------------------------------------- #
# 1. Configuration
# --------------------------------------------------------------------------- #
load_dotenv()  # override=False by default: real environment variables win over .env

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

# tz_aware=True: PyMongo hands back datetimes that know they are UTC. Without it they are
# naive and every .timestamp() / astimezone() call silently uses the *server's* local zone.
client = MongoClient(MONGO_URI, tz_aware=True, tzinfo=timezone.utc, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

MIN_MS = 100_000_000_000          # below this a number "looks like seconds"
MAX_MS = 4_102_444_800_000        # 2100-01-01
PRESENCE = ("PRESENT", "WFH", "ON_DUTY")       # R6
ABSENCE = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60                        # R2
MIN_OVERTIME_MINUTES = 30                      # R3
HALF_DAY_BELOW_HUNDREDTHS = 450                # R5: work_hours < 4.50


# --------------------------------------------------------------------------- #
# 2. Validation helpers
# --------------------------------------------------------------------------- #
_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def parse_date(s: str) -> Date:
    """Strict YYYY-MM-DD. (date.fromisoformat also accepts '20260706' - we do not.)"""
    if not _DATE_RE.fullmatch(s):
        raise ValueError("expected a date formatted YYYY-MM-DD")
    return Date(int(s[:4]), int(s[5:7]), int(s[8:10]))  # ValueError for 2026-02-31


def invalid(loc: tuple, msg: str) -> RequestValidationError:
    """A 422 whose body has the same shape as FastAPI's own validation errors."""
    return RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": None}])


def query_date(value: Optional[str], name: str) -> Optional[Date]:
    if value is None:
        return None
    try:
        return parse_date(value)
    except ValueError as exc:
        raise invalid(("query", name), str(exc))


def month_bounds(month: str) -> tuple[Date, Date]:
    """First and last calendar day of a YYYY-MM month."""
    try:
        y, m = int(month[:4]), int(month[5:7])
        first = Date(y, m, 1)
        last = (Date(y + 1, 1, 1) if m == 12 else Date(y, m + 1, 1)) - timedelta(days=1)
    except (ValueError, OverflowError):
        raise invalid(("query", "month"), "invalid month")
    return first, last


def require(value: Any, name: str) -> Any:
    if value is None:
        raise invalid(("query", name), "field required")
    return value


# --------------------------------------------------------------------------- #
# 3. Time + business rules (R1 - R5).  Pure functions: easy to unit-test.
# --------------------------------------------------------------------------- #
def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def trunc(dt: Optional[datetime]) -> Optional[datetime]:
    """Aware-UTC datetime truncated to whole seconds (R1). None stays None."""
    if dt is None:
        return None
    if dt.tzinfo is None:                      # defensive: treat naive values as UTC
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0)


def ms_to_dt(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to the second."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """aware/naive datetime -> epoch milliseconds (exact integer arithmetic, no floats)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def _hm(s: str) -> tuple[int, int]:
    h, m = s.split(":")
    return int(h), int(m)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return _hm(shift_end) <= _hm(shift_start)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: the IST calendar date a punch-in belongs to."""
    local = punch_in.astimezone(IST)
    d = local.date()
    if is_overnight(shift_start, shift_end):
        eh, em = _hm(shift_end)
        # a punch-in earlier than shift_end belongs to the shift that started the day before
        if (local.hour, local.minute, local.second) < (eh, em, 0):
            d -= timedelta(days=1)
    return d.isoformat()


def shift_start_dt(att_date: str, shift_start: str) -> datetime:
    d = parse_date(att_date)
    h, m = _hm(shift_start)
    return datetime(d.year, d.month, d.day, h, m, tzinfo=IST)


def shift_end_dt(att_date: str, shift_start: str, shift_end: str) -> datetime:
    d = parse_date(att_date)
    if is_overnight(shift_start, shift_end):
        d += timedelta(days=1)                 # an overnight shift ends on the next calendar day
    h, m = _hm(shift_end)
    return datetime(d.year, d.month, d.day, h, m, tzinfo=IST)


def compute_late_minutes(punch_in: datetime, att_date: str, shift_start: str) -> int:
    """R2: late only if STRICTLY more than 10:00 after shift start; then floor minutes since shift start."""
    secs = int((punch_in - shift_start_dt(att_date, shift_start)).total_seconds())
    return secs // 60 if secs > GRACE_SECONDS else 0


def compute_overtime_minutes(punch_out: datetime, att_date: str, shift_start: str, shift_end: str) -> int:
    """R3: floor minutes after shift end, counted only if >= 30."""
    secs = int((punch_out - shift_end_dt(att_date, shift_start, shift_end)).total_seconds())
    minutes = secs // 60                       # floor; negative (left early) stays negative
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def work_hours_hundredths(punch_in: datetime, punch_out: datetime) -> int:
    """R4 in integer arithmetic: round-half-up(seconds * 100 / 3600). No float, no banker's rounding."""
    secs = int((punch_out - punch_in).total_seconds())
    return (secs * 200 + 3600) // 7200         # floor(x + 1/2) with x = secs*100/3600


def derive(emp: dict, att_date: str, status: str, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """All four derived fields for one record (R2 - R5). Single source of truth for punch-in/out and PATCH."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, att_date, emp["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hundredths = work_hours_hundredths(punch_in, punch_out)
    return {
        "work_hours": hundredths / 100,
        "late_minutes": late,
        "overtime_minutes": compute_overtime_minutes(punch_out, att_date, emp["shift_start"], emp["shift_end"]),
        "half_day": hundredths < HALF_DAY_BELOW_HUNDREDTHS,
    }


# --------------------------------------------------------------------------- #
# 4. Request models
# --------------------------------------------------------------------------- #
EpochMs = Annotated[int, Field(strict=True, ge=MIN_MS, le=MAX_MS)]   # strict: no floats, no "123", no true
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
HHMM = Annotated[str, Field(pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")]


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_joined_on(cls, v: str) -> str:
        parse_date(v)
        return v

    @model_validator(mode="after")
    def _shift_times_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_null(self):
        # "omitted" means unchanged, but an explicit null is not allowed by the contract
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} must not be null")
        return self


# --------------------------------------------------------------------------- #
# 5. Serialisation: BSON document -> API JSON
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _history_value(field: str, value: Any) -> Any:
    return dt_to_ms(value) if field in ("punch_in", "punch_out") and value is not None else value


def history_out(entry: dict) -> dict:
    return {
        "at": dt_to_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": {
            k: {"from": _history_value(k, v.get("from")), "to": _history_value(k, v.get("to"))}
            for k, v in (entry.get("changes") or {}).items()
        },
    }


def record_out(doc: dict) -> dict:
    """Seeded documents may omit late/overtime/half_day/history: missing means 0 / 0 / false / []."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 6. Indexes (created at startup, idempotent)
# --------------------------------------------------------------------------- #
_indexes_ready = False


def ensure_indexes() -> None:
    global _indexes_ready
    e, a = db.employees, db.attendance_logs
    e.create_index([("emp_code", 1)], unique=True, name="uniq_emp_code")           # identity + unique guarantee
    e.create_index([("department", 1), ("emp_code", 1)], name="dept_emp_code")     # list filter+sort, trend dept match
    e.create_index([("department", 1), ("joined_on", 1)], name="dept_joined_on")   # headcount per day / month
    e.create_index([("joined_on", 1)], name="joined_on")                           # headcount without department
    a.create_index([("emp_code", 1), ("date", 1)], unique=True, name="uniq_emp_date")  # one record per day; per-employee ranges
    a.create_index([("date", -1), ("emp_code", 1)], name="date_desc_emp")          # listing order, month/day range scans
    a.create_index([("status", 1), ("date", -1), ("emp_code", 1)], name="status_date_emp")
    a.create_index([("emp_code", 1), ("punch_in", -1)], name="emp_punch_in")       # punch-out: latest punch-in <= t
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:               # keep the process alive; /health will retry and report 503
        log.error("could not create indexes at startup: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 7a. System
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:
            ensure_indexes()
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# 7b. Employees
# --------------------------------------------------------------------------- #
def get_employee_or_404(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        db.employees.insert_one(doc)           # the unique index decides the race, not a prior find()
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    flt = {"department": department} if department is not None else {}
    total = db.employees.count_documents(flt) if flt else db.employees.estimated_document_count()
    cursor = db.employees.find(flt).sort("emp_code", 1).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 7c. Attendance
# --------------------------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    punched = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    att_date = attendance_date(punched, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": att_date,
        "status": body.status,
        "punch_in": punched,
        "punch_out": None,
        **derive(emp, att_date, body.status, punched, None),
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)     # unique (emp_code, date) index: exactly one concurrent insert wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    out = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    # most recent record whose punch_in is at or before the punch-out (works across midnight)
    doc = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": out}}, sort=[("punch_in", -1)]
    )
    if doc is None:
        raise HTTPException(404, "no punch-in found")
    if doc.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    punched_in = trunc(doc["punch_in"])
    if out <= punched_in or out - punched_in > timedelta(hours=24):
        raise invalid(("body", "punched_at"), "punched_at must be after punch_in and within 24 hours of it")

    d = derive(emp, doc["date"], doc["status"], punched_in, out)
    # Atomic compare-and-set: only succeeds while the record is still open and unchanged.
    updated = db.attendance_logs.find_one_and_update(
        {"_id": doc["_id"], "punch_out": None, "punch_in": doc["punch_in"]},
        {"$set": {"punch_out": out, "work_hours": d["work_hours"],
                  "overtime_minutes": d["overtime_minutes"], "half_day": d["half_day"]}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:                        # lost the race to another punch-out (or a correction)
        raise HTTPException(409, "already punched out")
    return record_out(updated)


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    try:
        parse_date(date)
    except ValueError as exc:
        raise invalid(("path", "date"), str(exc))
    emp = get_employee_or_404(emp_code)
    doc = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if doc is None:
        raise HTTPException(404, "no attendance record for that date")

    given = body.model_fields_set
    old_status = doc["status"]
    old_in, old_out = trunc(doc.get("punch_in")), trunc(doc.get("punch_out"))
    new_status = body.status or old_status

    if new_status in ABSENCE:                  # ABSENT / LEAVE clear both punch times
        if "punch_in" in given or "punch_out" in given:
            raise invalid(("body",), "punch times cannot be supplied with status ABSENT or LEAVE")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if "punch_in" in given else old_in
        new_out = ms_to_dt(body.punch_out) if "punch_out" in given else old_out
        if new_in is None:
            raise invalid(("body", "punch_in"), "a presence status requires a punch_in")
        if "punch_in" in given and attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != date:
            raise invalid(("body", "punch_in"), "punch_in must stay on the record's attendance date")
        if ("punch_in" in given or "punch_out" in given) and new_out is not None:
            if new_out <= new_in or new_out - new_in > timedelta(hours=24):
                raise invalid(("body", "punch_out"), "punch_out must be after punch_in and within 24 hours")

    new_vals = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
                **derive(emp, date, new_status, new_in, new_out)}
    old_vals = {
        "status": old_status, "punch_in": old_in, "punch_out": old_out,
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
    }
    if all(new_vals[k] == old_vals[k] for k in ("status", "punch_in", "punch_out")):
        raise invalid(("body",), "the request changes nothing")
    changes = {k: {"from": old_vals[k], "to": new_vals[k]} for k in new_vals if new_vals[k] != old_vals[k]}
    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: the filter pins everything we read. A concurrent correction appends to
    # history (so "history.<n>" now exists) and a concurrent punch-out changes punch_out; either makes
    # this update match nothing, and the loser gets 409 instead of overwriting the winner's entry.
    n = len(doc.get("history") or [])
    updated = db.attendance_logs.find_one_and_update(
        {"emp_code": emp_code, "date": date, "status": doc["status"],
         "punch_in": doc.get("punch_in"), "punch_out": doc.get("punch_out"),
         f"history.{n}": {"$exists": False}},
        {"$set": new_vals, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "the record was modified concurrently; retry")
    return record_out(updated)


def build_attendance_query(emp_code, date_from, date_to, status, page, page_size) -> dict:
    """Shared by GET /attendance and the explain endpoint so both run the identical query."""
    d_from, d_to = query_date(date_from, "date_from"), query_date(date_to, "date_to")
    if d_from and d_to and d_from > d_to:
        raise invalid(("query", "date_from"), "date_from must not be after date_to")
    flt: dict = {}
    if emp_code:
        flt["emp_code"] = emp_code
    if d_from or d_to:
        flt["date"] = {}
        if d_from:
            flt["date"]["$gte"] = d_from.isoformat()
        if d_to:
            flt["date"]["$lte"] = d_to.isoformat()
    if status:
        flt["status"] = status
    return {"filter": flt, "sort": {"date": -1, "emp_code": 1}, "skip": (page - 1) * page_size, "limit": page_size}


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
    flt = q["filter"]
    total = db.attendance_logs.count_documents(flt) if flt else db.attendance_logs.estimated_document_count()
    cursor = db.attendance_logs.find(flt).sort(list(q["sort"].items())).skip(q["skip"]).limit(q["limit"])
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 8. Analytics.  Every number is computed by MongoDB from the STORED derived fields.
#
# Rounding: MongoDB's $round is banker's rounding and R8 wants half-up, so the pipelines return exact
# integers (half-days, hundredths of an hour, ten-thousandths of a rate) and the half-up step is
# integer arithmetic of the form floor((2*a + b) / (2*b)).
# --------------------------------------------------------------------------- #
_LATE = {"$ifNull": ["$late_minutes", 0]}
_OVERTIME = {"$ifNull": ["$overtime_minutes", 0]}
_IS_PRESENT = {"$in": ["$status", list(PRESENCE)]}
_DAY_OF_WEEK = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}  # 1=Sun .. 7=Sat
_IS_WEEKDAY = {"$and": [{"$gte": [_DAY_OF_WEEK, 2]}, {"$lte": [_DAY_OF_WEEK, 6]}]}
# present value in HALF-days so sums stay exact integers: full day = 2, half day = 1, not present = 0
_HALF_UNITS = {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 1, 2]}
_UNITS_ANY_DAY = {"$cond": [_IS_PRESENT, _HALF_UNITS, 0]}
_UNITS_WEEKDAY = {"$cond": [{"$and": [_IS_PRESENT, _IS_WEEKDAY]}, _HALF_UNITS, 0]}   # R7: Mon-Fri only
_LATE_FLAG = {"$cond": [{"$gt": [_LATE, 0]}, 1, 0]}
_LATE_MINUTES = {"$cond": [{"$gt": [_LATE, 0]}, _LATE, 0]}
_IS_LEAVE = {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
_IS_ON_DUTY = {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}
_HAS_HOURS = {"$and": [_IS_PRESENT, {"$isNumber": "$work_hours"}]}
_HOURS_HUNDREDTHS = {"$cond": [_HAS_HOURS, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}
_HAS_HOURS_FLAG = {"$cond": [_HAS_HOURS, 1, 0]}


def working_days(first: Date, last: Date, joined_on: str) -> int:
    """R7: Mon-Fri between max(first of month, joined_on) and the last day of the month."""
    start = max(first, parse_date(joined_on))
    n, d = 0, start
    while d <= last:
        n += d.weekday() < 5
        d += timedelta(days=1)
    return n


def employee_monthly_pipeline(emp_code: str, start: str, end: str) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},       # index emp_code+date
        {"$group": {
            "_id": None,
            "half_units": {"$sum": _UNITS_WEEKDAY},
            "leave_days": {"$sum": _IS_LEAVE},
            "late_count": {"$sum": _LATE_FLAG},
            "late_minutes": {"$sum": _LATE_MINUTES},
            "overtime_minutes": {"$sum": _OVERTIME},
        }},
    ]


def department_summary_pipeline(start: str, end: str, department: Optional[str]) -> list:
    emp_match: dict = {"joined_on": {"$lte": end}}                                       # R9
    if department is not None:
        emp_match["department"] = department
    return [
        {"$match": emp_match},
        # Start from EMPLOYEES so people with zero logs are still counted. For each employee, summarise
        # their logs of the month (correlated sub-pipeline using the emp_code+date index).
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": start, "$lte": end}}},
                {"$group": {
                    "_id": None,
                    "half_units": {"$sum": _UNITS_WEEKDAY},
                    "late_count": {"$sum": _LATE_FLAG},
                    "late_minutes": {"$sum": _LATE_MINUTES},
                    "leave_count": {"$sum": _IS_LEAVE},
                    "on_duty_count": {"$sum": _IS_ON_DUTY},
                    "hours_hundredths": {"$sum": _HOURS_HUNDREDTHS},
                    "hours_count": {"$sum": _HAS_HOURS_FLAG},
                }},
            ],
            "as": "m",
        }},
        {"$unwind": {"path": "$m", "preserveNullAndEmptyArrays": True}},                # no logs -> m missing, row kept
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "half_units": {"$sum": "$m.half_units"},
            "late_count": {"$sum": "$m.late_count"},
            "late_minutes": {"$sum": "$m.late_minutes"},
            "leave_count": {"$sum": "$m.leave_count"},
            "on_duty_count": {"$sum": "$m.on_duty_count"},
            "hours_hundredths": {"$sum": "$m.hours_hundredths"},                         # summed over RECORDS,
            "hours_count": {"$sum": "$m.hours_count"},                                   # not averaged per person
        }},
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(start: str, end: str, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": start, "$lte": end}}},                              # index on date
        {"$group": {"_id": "$emp_code", "total": {"$sum": _LATE_MINUTES}, "late_count": {"$sum": _LATE_FLAG}}},
        {"$match": {"total": {"$gt": 0}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},                                                               # drops logs of unknown employees
    ]
    if department is not None:
        pipeline.append({"$match": {"e.department": department}})                       # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking (1, 2, 2, 4). Array position is NOT a rank.
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},                                           # limit applied AFTER ranking
        {"$sort": {"total": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$e.name", "department": "$e.department",
                      "total_late_minutes": "$total", "late_count": "$late_count"}},
    ]
    return pipeline


def department_trend_pipeline(department: str, d_from: Date, d_to: Date) -> list:
    n_days = (d_to - d_from).days + 1
    from_dt = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    frm, to = d_from.isoformat(), d_to.isoformat()
    rate_ok = {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]}
    return [
        {"$match": {"department": department}},                                         # index department
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": frm, "$lte": to}}},
                {"$project": {"_id": 0, "date": 1, "h": _UNITS_ANY_DAY, "late": _LATE_FLAG}},
            ],
            "as": "m",
        }},
        {"$unwind": {"path": "$m", "preserveNullAndEmptyArrays": True}},                 # keep the pipeline non-empty
        {"$group": {"_id": "$m.date", "h": {"$sum": "$m.h"}, "late": {"$sum": "$m.late"}}},   # one row per day WITH data
        {"$group": {"_id": None, "rows": {"$push": {"date": "$_id", "h": "$h", "late": "$late"}}}},
        # Gap filling: generate every calendar day of the range inside the database, then join the data in.
        {"$project": {"_id": 0, "rows": 1, "d": {"$range": [0, n_days]}}},
        {"$unwind": "$d"},
        {"$set": {"dt": {"$add": [from_dt, {"$multiply": ["$d", 86_400_000]}]}}},
        {"$set": {"date": {"$dateToString": {"date": "$dt", "format": "%Y-%m-%d"}}}},
        {"$set": {"row": {"$arrayElemAt": [
            {"$filter": {"input": "$rows", "as": "r", "cond": {"$eq": ["$$r.date", "$date"]}}}, 0]}}},
        {"$set": {"h": {"$ifNull": ["$row.h", 0]}, "late_count": {"$ifNull": ["$row.late", 0]},
                  "dow": {"$dayOfWeek": "$dt"}}},
        {"$set": {"is_working_day": {"$and": [{"$gte": ["$dow", 2]}, {"$lte": ["$dow", 6]}]}}},
        # R9: headcount on that day = department employees with joined_on <= day
        {"$lookup": {
            "from": "employees", "let": {"day": "$date"},
            "pipeline": [{"$match": {"department": department, "$expr": {"$lte": ["$joined_on", "$$day"]}}},
                         {"$count": "n"}],
            "as": "hc",
        }},
        {"$set": {"headcount": {"$ifNull": [{"$arrayElemAt": ["$hc.n", 0]}, 0]}}},
        # rate in ten-thousandths, half-up: floor((10000*h + H) / (2H))   with h in half-days, H = headcount
        {"$set": {"rate_t": {"$cond": [rate_ok, {"$floor": {"$divide": [
            {"$add": [{"$multiply": ["$h", 10000]}, "$headcount"]}, {"$multiply": [2, "$headcount"]}]}}, None]}}},
        {"$set": {"has_rate": {"$cond": [{"$isNumber": "$rate_t"}, 1, 0]}}},
        # 7-row moving window INSIDE the range (first rows have fewer rows). null rates are ignored by $sum.
        {"$setWindowFields": {"sortBy": {"date": 1}, "output": {
            "win_sum": {"$sum": "$rate_t", "window": {"documents": [-6, 0]}},
            "win_n": {"$sum": "$has_rate", "window": {"documents": [-6, 0]}}}}},
        {"$set": {"mavg_t": {"$cond": [{"$gt": ["$win_n", 0]}, {"$floor": {"$divide": [
            {"$add": [{"$multiply": [2, "$win_sum"]}, "$win_n"]}, {"$multiply": [2, "$win_n"]}]}}, None]}}},
        {"$sort": {"date": 1}},
        {"$project": {"_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "h": 1, "late_count": 1,
                      "rate_t": 1, "mavg_t": 1}},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=_MONTH_PATTERN)):
    first, last = month_bounds(month)
    emp = get_employee_or_404(emp_code)
    rows = list(db.attendance_logs.aggregate(employee_monthly_pipeline(emp_code, first.isoformat(), last.isoformat())))
    r = rows[0] if rows else {}
    h = int(r.get("half_units", 0))
    wd = working_days(first, last, emp["joined_on"])
    # attendance_pct = (h/2)/wd*100 rounded half-up to 2 dp  ==  floor((10000*h + wd) / (2*wd)) / 100
    pct = ((10000 * h + wd) // (2 * wd)) / 100 if wd > 0 else None
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": wd,
        "present_days": h / 2,
        "leave_days": int(r.get("leave_days", 0)),
        "late_count": int(r.get("late_count", 0)),
        "total_late_minutes": int(r.get("late_minutes", 0)),
        "total_overtime_minutes": int(r.get("overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=_MONTH_PATTERN), department: Optional[str] = None):
    first, last = month_bounds(month)
    rows = db.employees.aggregate(department_summary_pipeline(first.isoformat(), last.isoformat(), department))
    items = []
    for r in rows:
        n, total = int(r["hours_count"]), int(r["hours_hundredths"])
        items.append({
            "department": r["_id"],
            "headcount": int(r["headcount"]),
            "present_days": int(r["half_units"]) / 2,
            # mean over records, half-up to 2 dp: floor((2*sum + n) / (2n)) hundredths
            "avg_work_hours": ((2 * total + n) // (2 * n)) / 100 if n > 0 else None,
            "late_count": int(r["late_count"]),
            "total_late_minutes": int(r["late_minutes"]),
            "leave_count": int(r["leave_count"]),
            "on_duty_count": int(r["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=_MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    first, last = month_bounds(month)
    rows = db.attendance_logs.aggregate(late_leaderboard_pipeline(first.isoformat(), last.isoformat(), limit, department))
    items = [{"rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"], "department": r["department"],
              "total_late_minutes": int(r["total_late_minutes"]), "late_count": int(r["late_count"])} for r in rows]
    return {"month": month, "items": items}


def trend_range(from_: str, to: str) -> tuple[Date, Date]:
    d_from, d_to = query_date(from_, "from"), query_date(to, "to")
    if d_to < d_from:
        raise invalid(("query", "to"), "'to' must not be before 'from'")
    if (d_to - d_from).days + 1 > 92:
        raise invalid(("query", "to"), "the range may not exceed 92 days")
    return d_from, d_to


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(..., alias="from"), to: str = Query(...)):
    d_from, d_to = trend_range(from_, to)
    if db.employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, "department not found")
    items = []
    for r in db.employees.aggregate(department_trend_pipeline(department, d_from, d_to)):
        items.append({
            "date": r["date"],
            "is_working_day": bool(r["is_working_day"]),
            "headcount": int(r["headcount"]),
            "present_count": int(r["h"]) / 2,
            "late_count": int(r["late_count"]),
            "attendance_rate": None if r.get("rate_t") is None else round(int(r["rate_t"]) / 10000, 4),
            "moving_avg_7d": None if r.get("mavg_t") is None else round(int(r["mavg_t"]) / 10000, 4),
        })
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 9. Explain: prove the plans use indexes.  Uses the SAME builders as the endpoints above.
# --------------------------------------------------------------------------- #
def _run_explain(command: dict) -> dict:
    result = db.command({"explain": command, "verbosity": "executionStats"})
    return json.loads(json_util.dumps(result))      # ObjectId / datetime / Int64 -> plain JSON


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=_MONTH_PATTERN),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    if endpoint == "attendance_list":
        q = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
        coll = "attendance_logs"
        cmd = {"find": coll, "filter": q["filter"], "sort": q["sort"], "skip": q["skip"], "limit": q["limit"]}
    else:
        if endpoint == "department_trend":
            d_from, d_to = trend_range(require(from_, "from"), require(to, "to"))
            coll, pipeline = "employees", department_trend_pipeline(require(department, "department"), d_from, d_to)
        else:
            first, last = month_bounds(require(month, "month"))
            s, e = first.isoformat(), last.isoformat()
            if endpoint == "employee_monthly":
                coll, pipeline = "attendance_logs", employee_monthly_pipeline(require(emp_code, "emp_code"), s, e)
            elif endpoint == "department_summary":
                coll, pipeline = "employees", department_summary_pipeline(s, e, department)
            else:
                coll, pipeline = "attendance_logs", late_leaderboard_pipeline(s, e, limit, department)
        cmd = {"aggregate": coll, "pipeline": pipeline, "cursor": {}}
    return {"endpoint": endpoint, "collection": coll, "explain": _run_explain(cmd)}