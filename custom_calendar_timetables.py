from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Set, Sequence, Dict, Any, ClassVar

import pendulum
from pendulum import DateTime as Pdt

from airflow.timetables.base import (
    DagRunInfo,
    DataInterval,
    TimeRestriction,
    Timetable,
)


# ---------------------------
# Helpers
# ---------------------------

def _normalize_tz(tz: Optional[str]) -> pendulum.tz.timezone.Timezone:
    if tz is None:
        return pendulum.local_timezone()
    return pendulum.timezone(tz)


def _to_date_set(holidays: Optional[Iterable]) -> Set[pendulum.Date]:
    out: Set[pendulum.Date] = set()
    if not holidays:
        return out
    for h in holidays:
        if isinstance(h, str):
            d = pendulum.parse(h).date()
        elif isinstance(h, pendulum.DateTime):
            d = h.date()
        elif hasattr(h, "year") and hasattr(h, "month") and hasattr(h, "day"):
            d = pendulum.date(h.year, h.month, h.day)
        else:
            raise ValueError(f"Unsupported holiday type: {type(h)}")
        out.add(d)
    return out


def _is_weekend(dt: Pdt, tz: Optional[pendulum.tz.timezone.Timezone] = None) -> bool:
    check_dt = dt if tz is None else dt.in_timezone(tz)
    # isoweekday: Mon=1 .. Sun=7 -> weekend = (6,7)
    return check_dt.isoweekday() in (6, 7)


def _is_holiday(dt: Pdt, holidays: Set[pendulum.Date]) -> bool:
    return dt.date() in holidays


def _at_time(dt: Pdt, hour: int, minute: int, second: int) -> Pdt:
    return dt.replace(hour=hour, minute=minute, second=second, microsecond=0)


def _next_business_day_at(
    start: Pdt,
    *,
    hour: int,
    minute: int,
    second: int,
    tz: pendulum.tz.timezone.Timezone,
    holidays: Set[pendulum.Date],
) -> Pdt:
    """
    Find next business-slot >= start (accept exact match). Returned datetime is in tz.
    """
    cur = start.in_timezone(tz)
    candidate = _at_time(cur, hour, minute, second)
    # If candidate is strictly before 'cur', move to next day
    if candidate < cur:
        candidate = candidate.add(days=1)

    # Move forward until candidate is a business day & not a holiday
    while _is_weekend(candidate, tz) or _is_holiday(candidate, holidays):
        candidate = candidate.add(days=1)
        candidate = _at_time(candidate, hour, minute, second)

    return candidate.in_timezone(tz)


def _prev_business_day_at(
    end: Pdt,
    *,
    hour: int,
    minute: int,
    second: int,
    tz: pendulum.tz.timezone.Timezone,
    holidays: Set[pendulum.Date],
) -> Pdt:
    """
    Find most recent business-slot strictly before `end` (i.e., previous scheduled slot).
    If `end` is exactly at a slot time, we return the prior slot.
    Returned datetime is in tz.
    """
    cur = end.in_timezone(tz)
    candidate = _at_time(cur, hour, minute, second)

    # If candidate is >= cur (end is at/after the slot), step back one day to get previous slot.
    if candidate >= cur:
        candidate = candidate.subtract(days=1)

    while _is_weekend(candidate, tz) or _is_holiday(candidate, holidays):
        candidate = candidate.subtract(days=1)
        candidate = _at_time(candidate, hour, minute, second)

    return candidate.in_timezone(tz)


def _next_weekly_at(
    start: Pdt,
    *,
    hour: int,
    minute: int,
    second: int,
    tz: pendulum.tz.timezone.Timezone,
    weekdays: Sequence[int],
) -> Pdt:
    cur = start.in_timezone(tz)
    candidate = _at_time(cur, hour, minute, second)

    for _ in range(8):
        if candidate.weekday() in weekdays and candidate >= cur:
            return candidate.in_timezone(tz)
        candidate = candidate.add(days=1)
        candidate = _at_time(candidate, hour, minute, second)

    return candidate.in_timezone(tz)


def _prev_weekly_at(
    end: Pdt,
    *,
    hour: int,
    minute: int,
    second: int,
    tz: pendulum.tz.timezone.Timezone,
    weekdays: Sequence[int],
) -> Pdt:
    cur = end.in_timezone(tz)
    candidate = _at_time(cur, hour, minute, second)

    if candidate >= cur:
        candidate = candidate.subtract(days=1)

    for _ in range(8):
        if candidate.weekday() in weekdays:
            return candidate.in_timezone(tz)
        candidate = candidate.subtract(days=1)
        candidate = _at_time(candidate, hour, minute, second)

    return candidate.in_timezone(tz)


def _respect_restrictions(run_after: Pdt, restriction: TimeRestriction) -> Optional[Pdt]:
    if restriction.earliest and run_after < restriction.earliest:
        return restriction.earliest
    if restriction.latest and run_after > restriction.latest:
        return None
    return run_after


# ---------------------------
# 1) DemoCalendarTimetable (non-serializable)
# ---------------------------

class DemoCalendarTimetable(Timetable):
    NAME: ClassVar[str] = "demo_calendar_non_serializable"

    def __init__(self, hour: int = 12, minute: int = 0, second: int = 0, timezone: Optional[str] = None) -> None:
        self.hour = hour
        self.minute = minute
        self.second = second
        self.tz = _normalize_tz(timezone)

    def infer_manual_data_interval(self, run_after: Pdt) -> DataInterval:
        return DataInterval(start=run_after, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        now = pendulum.now(self.tz)

        if last_automated_data_interval is None:
            base = restriction.earliest or now
            if not restriction.catchup:
                base = max(base, now)
            # choose the next slot >= base
            scheduled = _next_business_day_at(
                base, hour=self.hour, minute=self.minute, second=self.second, tz=self.tz, holidays=set()
            )
        else:
            prev_end = last_automated_data_interval.end.in_timezone(self.tz)
            # schedule next slot strictly after prev_end
            scheduled = _next_business_day_at(
                prev_end.add(seconds=1), hour=self.hour, minute=self.minute, second=self.second, tz=self.tz, holidays=set()
            )

        scheduled = scheduled.in_timezone(self.tz)
        scheduled = _respect_restrictions(scheduled, restriction)
        if scheduled is None:
            return None

        # For demo: data interval is previous 24 hours (keeps behavior simple)
        start = scheduled.subtract(days=1).in_timezone(self.tz)
        return DagRunInfo(run_after=scheduled, data_interval=DataInterval(start=start, end=scheduled))

    def serialize(self) -> Dict[str, Any]:  # type: ignore[override]
        raise NotImplementedError("DemoCalendarTimetable is intentionally non-serializable.")


# ---------------------------
# 2) BusinessCalendarTimetable
# ---------------------------

@dataclass
class BusinessCalendarTimetable(Timetable):
    holidays: Iterable
    hour: int = 9
    minute: int = 0
    second: int = 0
    timezone: Optional[str] = None

    def __post_init__(self):
        self._tz = _normalize_tz(self.timezone)
        self._holidays = _to_date_set(self.holidays)

    NAME: ClassVar[str] = "business_calendar"

    def infer_manual_data_interval(self, run_after: Pdt) -> DataInterval:
        return DataInterval(start=run_after, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        now = pendulum.now(self._tz)

        if last_automated_data_interval is None:
            base = restriction.earliest or now
            if not restriction.catchup:
                base = max(base, now)
            # scheduled slot is the next business slot >= base
            scheduled = _next_business_day_at(
                base,
                hour=self.hour,
                minute=self.minute,
                second=self.second,
                tz=self._tz,
                holidays=self._holidays,
            )
        else:
            prev_end = last_automated_data_interval.end.in_timezone(self._tz)
            # we want the next scheduled slot strictly AFTER the previous slot
            scheduled = _next_business_day_at(
                prev_end.add(seconds=1),
                hour=self.hour,
                minute=self.minute,
                second=self.second,
                tz=self._tz,
                holidays=self._holidays,
            )

        # scheduled is the logical "slot" (data_interval.end). normalize and enforce restrictions
        scheduled = scheduled.in_timezone(self._tz)
        scheduled = _respect_restrictions(scheduled, restriction)
        if scheduled is None:
            return None

        # compute previous scheduled slot (strictly before scheduled) as data_interval.start
        prev_slot = _prev_business_day_at(
            scheduled,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            tz=self._tz,
            holidays=self._holidays,
        ).in_timezone(self._tz)

        # Ensure prev_slot < scheduled; if prev_slot >= scheduled (shouldn't happen) fallback to subtract 1 day
        if prev_slot >= scheduled:
            prev_slot = (scheduled.subtract(days=1)).in_timezone(self._tz)

        # run_after should be the scheduled time (logical_date == data_interval.end)
        run_after = scheduled

        return DagRunInfo(run_after=run_after, data_interval=DataInterval(start=prev_slot, end=scheduled))

    def serialize(self) -> Dict[str, Any]:  # type: ignore[override]
        return {
            "holidays": sorted([d.to_date_string() for d in self._holidays]),
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
            "timezone": str(self._tz.name),
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "BusinessCalendarTimetable":  # type: ignore[override]
        return cls(
            holidays=data.get("holidays", []),
            hour=data.get("hour", 9),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
            timezone=data.get("timezone"),
        )


@dataclass
class BusinessCalendarTimetableNextDay(Timetable):
    """
    Emits runs on Tue..Sat at configured time (run_after).
    logical_date = run_after - 1 day (Mon..Fri).
    Skip only when the logical_date is a holiday (user requirement).
    Catchup behavior: when deployed (no prior runs) and catchup=True, finds the most recent
    eligible run_after <= now so last Saturday can be emitted as catchup.
    """
    holidays: Iterable
    hour: int = 9
    minute: int = 0
    second: int = 0
    timezone: Optional[str] = None

    def __post_init__(self):
        self._tz = _normalize_tz(self.timezone)
        self._holidays = _to_date_set(self.holidays)

    NAME: ClassVar[str] = "business_calendar_next_day"

    def infer_manual_data_interval(self, run_after: Pdt) -> DataInterval:
        return DataInterval(start=run_after, end=run_after)

    def _next_run_after_candidate(self, start: Pdt) -> Pdt:
        """
        Find next candidate run_after >= start that falls on Tue..Sat and where the
        logical_date (run_after - 1 day) is NOT a holiday.
        We intentionally DO NOT skip when run_after.date() itself is a holiday (per requirement).
        """
        cur = start.in_timezone(self._tz)
        candidate = _at_time(cur, self.hour, self.minute, self.second)
        if candidate < cur:
            candidate = candidate.add(days=1)

        # Accept weekdays Tue(1) .. Sat(5)
        for _ in range(21):
            run_wd = candidate.weekday()  # 0=Mon .. 6=Sun
            logical_date = candidate.subtract(days=1)
            if run_wd in (1, 2, 3, 4, 5):  # Tue..Sat
                if logical_date.date() not in self._holidays:
                    return candidate.in_timezone(self._tz)
            candidate = candidate.add(days=1)
            candidate = _at_time(candidate, self.hour, self.minute, self.second)

        return candidate.in_timezone(self._tz)

    def _prev_run_after_candidate(self, start: Pdt) -> Pdt:
        """
        Find latest candidate run_after <= start that falls on Tue..Sat and where the
        logical_date (run_after - 1 day) is NOT a holiday.
        """
        cur = start.in_timezone(self._tz)
        candidate = _at_time(cur, self.hour, self.minute, self.second)
        if candidate > cur:
            candidate = candidate.subtract(days=1)

        for _ in range(21):
            run_wd = candidate.weekday()
            logical_date = candidate.subtract(days=1)
            if run_wd in (1, 2, 3, 4, 5):
                if logical_date.date() not in self._holidays:
                    return candidate.in_timezone(self._tz)
            candidate = candidate.subtract(days=1)
            candidate = _at_time(candidate, self.hour, self.minute, self.second)

        return candidate.in_timezone(self._tz)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        now = pendulum.now(self._tz)

        # Determine search base:
        if last_automated_data_interval is None:
            if restriction.catchup:
                base = restriction.earliest or self._prev_run_after_candidate(now)
            else:
                base = max(restriction.earliest or now, now)
        else:
            base = last_automated_data_interval.end.in_timezone(self._tz).add(seconds=1)

        run_after = self._next_run_after_candidate(base)

        # If not catchup ensure returned run_after is >= now
        if not restriction.catchup and run_after < now:
            run_after = self._next_run_after_candidate(now)

        if restriction.latest and run_after > restriction.latest:
            return None

        logical_date = run_after.subtract(days=1).in_timezone(self._tz)

        interval_start = _at_time(logical_date, self.hour, self.minute, self.second)
        interval_end = interval_start.add(days=1)

        return DagRunInfo(run_after=run_after.in_timezone(self._tz), data_interval=DataInterval(start=interval_start, end=interval_end))

    def serialize(self) -> Dict[str, Any]:  # type: ignore[override]
        return {
            "holidays": sorted([d.to_date_string() for d in self._holidays]),
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
            "timezone": str(self._tz.name),
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "BusinessCalendarTimetableNextDay":  # type: ignore[override]
        return cls(
            holidays=data.get("holidays", []),
            hour=data.get("hour", 9),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
            timezone=data.get("timezone"),
        )


# ---------------------------
# 3) WeeklyCalendarTimetable
# ---------------------------

@dataclass
class WeeklyCalendarTimetable(Timetable):
    weekdays: Sequence[int]
    hour: int = 0
    minute: int = 0
    second: int = 0
    timezone: Optional[str] = None

    def __post_init__(self):
        if not self.weekdays:
            raise ValueError("Provide at least one weekday (0=Mon .. 6=Sun).")
        if not all(0 <= d <= 6 for d in self.weekdays):
            raise ValueError("Weekdays must be integers in [0..6].")
        self._tz = _normalize_tz(self.timezone)
        self._days = sorted(set(self.weekdays))

    NAME: ClassVar[str] = "weekly_calendar"

    def infer_manual_data_interval(self, run_after: Pdt) -> DataInterval:
        return DataInterval(start=run_after, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        now = pendulum.now(self._tz)

        if last_automated_data_interval is None:
            base = restriction.earliest or now
            if not restriction.catchup:
                base = max(base, now)
            scheduled = _next_weekly_at(
                base,
                hour=self.hour,
                minute=self.minute,
                second=self.second,
                tz=self._tz,
                weekdays=self._days,
            )
        else:
            prev_end = last_automated_data_interval.end.in_timezone(self._tz)
            scheduled = _next_weekly_at(
                prev_end.add(seconds=1),
                hour=self.hour,
                minute=self.minute,
                second=self.second,
                tz=self._tz,
                weekdays=self._days,
            )

        scheduled = scheduled.in_timezone(self._tz)
        scheduled = _respect_restrictions(scheduled, restriction)
        if scheduled is None:
            return None

        prev_slot = _prev_weekly_at(
            scheduled,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            tz=self._tz,
            weekdays=self._days,
        ).in_timezone(self._tz)

        if prev_slot >= scheduled:
            prev_slot = (scheduled.subtract(days=1)).in_timezone(self._tz)

        run_after = scheduled

        return DagRunInfo(run_after=run_after, data_interval=DataInterval(start=prev_slot, end=scheduled))

    def serialize(self) -> Dict[str, Any]:  # type: ignore[override]
        return {
            "weekdays": list(self._days),
            "hour": self.hour,
            "minute": self.minute,
            "second": self.second,
            "timezone": str(self._tz.name),
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "WeeklyCalendarTimetable":  # type: ignore[override]
        return cls(
            weekdays=data.get("weekdays", [0]),
            hour=data.get("hour", 0),
            minute=data.get("minute", 0),
            second=data.get("second", 0),
            timezone=data.get("timezone"),
        )


# ---------------------------
# (Optional) Legacy plugin wrapper
# ---------------------------

try:
    from airflow.plugins_manager import AirflowPlugin

    class CustomCalendarsPlugin(AirflowPlugin):
        name = "custom_calendars_plugin"
        operators: Sequence = []
        hooks: Sequence = []
        executors: Sequence = []
        macros: Sequence = []
        appbuilder_views: Sequence = []
        appbuilder_menu_items: Sequence = []
        flask_blueprints: Sequence = []
        timetables = [DemoCalendarTimetable, BusinessCalendarTimetable, BusinessCalendarTimetableNextDay,WeeklyCalendarTimetable]

except Exception:
    pass