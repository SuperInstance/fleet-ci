"""Fleet CI Pipeline — orchestrator, conformance runner, change detection, history tracking."""

from __future__ import annotations

import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set


# ---------------------------------------------------------------------------
# 1. Pipeline Config
# ---------------------------------------------------------------------------


class RunStatus(Enum):
    """Status of a single test or a whole suite."""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


class PipelineStatus(Enum):
    """Overall pipeline status."""

    PASS = "PASS"
    PARTIAL = "PARTIAL"
    FAIL = "FAIL"


class ChangeType(Enum):
    """Type of file change."""

    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"


@dataclass
class ThresholdConfig:
    """Quality thresholds that must be met."""

    min_pass_rate: float = 100.0
    max_duration_seconds: float = 600.0
    required_coverage: float = 80.0


@dataclass
class NotificationConfig:
    """Where to send CI results."""

    channel: str  # "github_issues", "commit_status", "slack", "webhook"
    target: str = ""
    on_failure_only: bool = False
    on_success_only: bool = False


@dataclass
class TestSuiteConfig:
    """Configuration for a single test suite."""

    name: str
    suite_type: str  # "conformance", "unit", "integration", "security"
    command: str = ""
    timeout: float = 120.0
    retry_count: int = 0
    tags: List[str] = field(default_factory=list)


@dataclass
class RepoConfig:
    """Configuration for a monitored repository."""

    name: str
    url: str
    branch: str = "main"
    test_suites: List[str] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration."""

    repos: List[RepoConfig] = field(default_factory=list)
    test_suites: List[TestSuiteConfig] = field(default_factory=list)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    notifications: List[NotificationConfig] = field(default_factory=list)
    parallel: bool = True
    max_workers: int = 4
    dry_run: bool = False


# ---------------------------------------------------------------------------
# 2. Test Runner — result types & execution
# ---------------------------------------------------------------------------


@dataclass
class TestResult:
    """Result of a single test."""

    suite_name: str
    test_name: str
    status: RunStatus
    duration: float = 0.0
    error_message: str = ""
    timestamp: str = field(default_factory=lambda: _now_iso())


@dataclass
class TestSuiteResult:
    """Aggregated result for a test suite."""

    suite_name: str
    results: List[TestResult] = field(default_factory=list)
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    duration: float = 0.0
    timestamp: str = field(default_factory=lambda: _now_iso())

    @property
    def pass_rate(self) -> float:
        if self.total == 0:
            return 100.0
        return (self.passed / self.total) * 100.0

    def compute_summary(self) -> None:
        """Recompute totals from individual results."""
        self.total = len(self.results)
        self.passed = sum(1 for r in self.results if r.status == RunStatus.PASSED)
        self.failed = sum(1 for r in self.results if r.status == RunStatus.FAILED)
        self.skipped = sum(1 for r in self.results if r.status == RunStatus.SKIPPED)
        self.duration = sum(r.duration for r in self.results)


class TestRunner:
    """Execute a test suite and collect results."""

    def __init__(self, suite_config: TestSuiteConfig, dry_run: bool = False):
        self.suite_config = suite_config
        self.dry_run = dry_run

    def run(self) -> TestSuiteResult:
        """Run the suite and return aggregated results."""
        result = TestSuiteResult(suite_name=self.suite_config.name)
        # In dry-run mode we fabricate a "listed" result
        if self.dry_run:
            r = TestResult(
                suite_name=self.suite_config.name,
                test_name=f"{self.suite_config.name}/* (dry-run)",
                status=RunStatus.SKIPPED,
            )
            result.results.append(r)
            result.compute_summary()
            return result
        # Real execution would shell out to self.suite_config.command here.
        # For the module-level API we provide a programmatic hook.
        result.compute_summary()
        return result

    @staticmethod
    def run_from_results(
        suite_name: str, items: List[Dict[str, Any]]
    ) -> TestSuiteResult:
        """Build a TestSuiteResult from a list of dicts (for testing / manual use)."""
        tsr = TestSuiteResult(suite_name=suite_name)
        for item in items:
            tr = TestResult(
                suite_name=suite_name,
                test_name=item["test_name"],
                status=RunStatus(item["status"]),
                duration=item.get("duration", 0.0),
                error_message=item.get("error_message", ""),
            )
            tsr.results.append(tr)
        tsr.compute_summary()
        return tsr


# ---------------------------------------------------------------------------
# 3. Fleet Test Orchestrator
# ---------------------------------------------------------------------------


@dataclass
class RepoTestReport:
    """CI report for a single repository."""

    repo_name: str
    suites: List[TestSuiteResult] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: _now_iso())

    @property
    def total_passed(self) -> int:
        return sum(s.passed for s in self.suites)

    @property
    def total_failed(self) -> int:
        return sum(s.failed for s in self.suites)

    @property
    def total_tests(self) -> int:
        return sum(s.total for s in self.suites)

    @property
    def pass_rate(self) -> float:
        if self.total_tests == 0:
            return 100.0
        return (self.total_passed / self.total_tests) * 100.0


@dataclass
class ConformanceReport:
    """Cross-VM conformance check result."""

    overall_status: PipelineStatus
    repo_reports: List[RepoTestReport] = field(default_factory=list)
    conformance_matrix: Dict[str, Dict[str, RunStatus]] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: _now_iso())

    @property
    def total_repos(self) -> int:
        return len(self.repo_reports)

    @property
    def fully_conformant_count(self) -> int:
        return sum(1 for r in self.repo_reports if r.total_failed == 0)

    def _serialize_matrix(self) -> Any:
        """Recursively serialize conformance matrix values."""
        result: Dict[str, Any] = {}
        for repo, suites in self.conformance_matrix.items():
            if isinstance(suites, dict):
                result[repo] = {
                    k: v.value if isinstance(v, RunStatus) else v
                    for k, v in suites.items()
                }
            elif isinstance(suites, RunStatus):
                result[repo] = suites.value
            else:
                result[repo] = suites
        return result

    def to_json(self) -> str:
        return json.dumps(
            {
                "overall_status": self.overall_status.value,
                "total_repos": self.total_repos,
                "fully_conformant": self.fully_conformant_count,
                "conformance_matrix": self._serialize_matrix(),
                "timestamp": self.timestamp,
            },
            indent=2,
        )


@dataclass
class FleetTestReport:
    """Comprehensive fleet-wide CI report."""

    status: PipelineStatus
    repo_reports: List[RepoTestReport] = field(default_factory=list)
    conformance: Optional[ConformanceReport] = None
    duration: float = 0.0
    timestamp: str = field(default_factory=lambda: _now_iso())

    @property
    def total_passed(self) -> int:
        return sum(r.total_passed for r in self.repo_reports)

    @property
    def total_failed(self) -> int:
        return sum(r.total_failed for r in self.repo_reports)

    @property
    def total_tests(self) -> int:
        return sum(r.total_tests for r in self.repo_reports)

    def to_markdown(self) -> str:
        lines = [
            "# Fleet CI Report",
            "",
            f"**Status:** {self.status.value}",
            f"**Time:** {self.timestamp}",
            f"**Duration:** {self.duration:.2f}s",
            f"**Total tests:** {self.total_tests}",
            f"**Passed:** {self.total_passed}",
            f"**Failed:** {self.total_failed}",
            "",
            "## Repository Results",
            "",
        ]
        for report in self.repo_reports:
            badge = (
                "✅" if report.total_failed == 0 else ("⚠️" if report.pass_rate >= 50 else "❌")
            )
            lines.append(f"### {badge} {report.repo_name}")
            lines.append(f"- Tests: {report.total_tests} | Passed: {report.total_passed} | Failed: {report.total_failed}")
            lines.append(f"- Pass rate: {report.pass_rate:.1f}%")
            for suite in report.suites:
                s_badge = (
                    "✅"
                    if suite.failed == 0
                    else ("⚠️" if suite.pass_rate >= 50 else "❌")
                )
                lines.append(
                    f"  - {s_badge} {suite.suite_name}: "
                    f"{suite.passed}/{suite.total} ({suite.pass_rate:.1f}%)"
                )
            lines.append("")
        if self.conformance:
            lines.append("## Conformance Matrix")
            lines.append("")
            lines.append(f"Overall: **{self.conformance.overall_status.value}**")
            lines.append(
                f"Conformant repos: {self.conformance.fully_conformant_count}/{self.conformance.total_repos}"
            )
            lines.append("")
        return "\n".join(lines)

    def to_json(self) -> str:
        return json.dumps(
            {
                "status": self.status.value,
                "total_tests": self.total_tests,
                "passed": self.total_passed,
                "failed": self.total_failed,
                "duration": self.duration,
                "timestamp": self.timestamp,
                "repos": [
                    {
                        "name": r.repo_name,
                        "total": r.total_tests,
                        "passed": r.total_passed,
                        "failed": r.total_failed,
                        "pass_rate": r.pass_rate,
                    }
                    for r in self.repo_reports
                ],
            },
            indent=2,
        )


class FleetOrchestrator:
    """Run test suites across all repositories in the fleet."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._runners: Dict[str, TestRunner] = {}
        self._suite_map: Dict[str, TestSuiteConfig] = {
            s.name: s for s in config.test_suites
        }
        # Pre-build runners
        for suite in config.test_suites:
            self._runners[suite.name] = TestRunner(suite, dry_run=config.dry_run)

    def run_all(self) -> FleetTestReport:
        """Run all suites across all repos. Returns a fleet-wide report."""
        start = time.time()
        repo_reports: List[RepoTestReport] = []
        work = self._build_work_items()

        if self.config.parallel and len(work) > 1:
            repo_reports = self._run_parallel(work)
        else:
            for repo_cfg, suite_names in work:
                repo_reports.append(self._run_repo_suites(repo_cfg, suite_names))

        # Build conformance report
        conformance = self._build_conformance(repo_reports)

        elapsed = time.time() - start
        overall = self._compute_overall_status(repo_reports, conformance)

        return FleetTestReport(
            status=overall,
            repo_reports=repo_reports,
            conformance=conformance,
            duration=elapsed,
        )

    def run_repo(self, repo_name: str) -> RepoTestReport:
        """Run tests for a single repository."""
        repo_cfg = self._find_repo(repo_name)
        if repo_cfg is None:
            return RepoTestReport(repo_name=repo_name)
        return self._run_repo_suites(repo_cfg, repo_cfg.test_suites)

    def run_conformance(self) -> ConformanceReport:
        """Run conformance suites across all repos and cross-check."""
        fleet = self.run_all()
        return fleet.conformance or ConformanceReport(
            overall_status=PipelineStatus.FAIL
        )

    # -- internal helpers ---------------------------------------------------

    def _find_repo(self, name: str) -> Optional[RepoConfig]:
        for r in self.config.repos:
            if r.name == name:
                return r
        return None

    def _build_work_items(
        self,
    ) -> List[tuple[RepoConfig, List[str]]]:
        items: List[tuple[RepoConfig, List[str]]] = []
        for repo_cfg in self.config.repos:
            suite_names = repo_cfg.test_suites or [
                s.name for s in self.config.test_suites
            ]
            items.append((repo_cfg, suite_names))
        return items

    def _run_repo_suites(
        self, repo_cfg: RepoConfig, suite_names: List[str]
    ) -> RepoTestReport:
        suites: List[TestSuiteResult] = []
        for name in suite_names:
            runner = self._runners.get(name)
            if runner is None:
                cfg = TestSuiteConfig(name=name, suite_type="unknown")
                runner = TestRunner(cfg, dry_run=self.config.dry_run)
            suites.append(runner.run())
        return RepoTestReport(repo_name=repo_cfg.name, suites=suites)

    def _run_parallel(
        self, work: List[tuple[RepoConfig, List[str]]]
    ) -> List[RepoTestReport]:
        reports: List[RepoTestReport] = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as pool:
            futures = {
                pool.submit(self._run_repo_suites, rc, sn): rc.name
                for rc, sn in work
            }
            for future in as_completed(futures):
                reports.append(future.result())
        # Sort by name for deterministic output
        reports.sort(key=lambda r: r.repo_name)
        return reports

    def _build_conformance(
        self, repo_reports: List[RepoTestReport]
    ) -> ConformanceReport:
        matrix: Dict[str, Dict[str, RunStatus]] = {}
        for rr in repo_reports:
            for suite in rr.suites:
                suite_key = suite.suite_name
                if suite.failed == 0:
                    matrix.setdefault(rr.repo_name, {})[
                        suite_key
                    ] = RunStatus.PASSED
                else:
                    matrix.setdefault(rr.repo_name, {})[
                        suite_key
                    ] = RunStatus.FAILED

        all_passed = all(
            s == RunStatus.PASSED
            for row in matrix.values()
            for s in row.values()
        )
        any_failed = any(
            s == RunStatus.FAILED
            for row in matrix.values()
            for s in row.values()
        )

        if all_passed:
            overall = PipelineStatus.PASS
        elif any_failed:
            # Check thresholds
            total_failed = sum(r.total_failed for r in repo_reports)
            total_tests = sum(r.total_tests for r in repo_reports)
            if total_tests > 0 and total_failed / total_tests > 0.5:
                overall = PipelineStatus.FAIL
            else:
                overall = PipelineStatus.PARTIAL
        else:
            overall = PipelineStatus.PASS

        return ConformanceReport(
            overall_status=overall,
            repo_reports=repo_reports,
            conformance_matrix=matrix,
        )

    def _compute_overall_status(
        self,
        repo_reports: List[RepoTestReport],
        conformance: ConformanceReport,
    ) -> PipelineStatus:
        return conformance.overall_status


# ---------------------------------------------------------------------------
# 4. Change Detector
# ---------------------------------------------------------------------------


@dataclass
class ChangedFile:
    """A file changed between two commits."""

    path: str
    change_type: ChangeType
    additions: int = 0
    deletions: int = 0


# Patterns mapping file paths to test suites
_DEFAULT_SUITE_PATTERNS: Dict[str, List[str]] = {
    "conformance": [
        r"tests/conformance",
        r"src/vm/",
        r"src/interpreter",
        r"src/compiler",
    ],
    "unit": [
        r"src/",
        r"tests/unit",
    ],
    "integration": [
        r"tests/integration",
        r"src/api",
        r"src/services",
    ],
    "security": [
        r"src/auth",
        r"src/security",
        r"tests/security",
    ],
}


class ChangeDetector:
    """Detect what changed and map to affected test suites."""

    def __init__(
        self,
        repo_url: str = "",
        suite_patterns: Optional[Dict[str, List[str]]] = None,
    ):
        self.repo_url = repo_url
        self.suite_patterns = suite_patterns or _DEFAULT_SUITE_PATTERNS

    def detect_changes(
        self, repo: str, commit_range: str
    ) -> List[ChangedFile]:
        """Detect changed files between two commits.

        In production this would call ``git diff``.  Here we provide a
        stub that can be overridden in tests.
        """
        return []

    def detect_changes_from_list(
        self, files: List[Dict[str, Any]]
    ) -> List[ChangedFile]:
        """Build ChangedFile list from raw dicts (for testing)."""
        result: List[ChangedFile] = []
        for f in files:
            result.append(
                ChangedFile(
                    path=f["path"],
                    change_type=ChangeType(f["change_type"]),
                    additions=f.get("additions", 0),
                    deletions=f.get("deletions", 0),
                )
            )
        return result

    def map_to_suites(self, changed_files: List[ChangedFile]) -> Set[str]:
        """Map changed files to affected test suites."""
        affected: Set[str] = set()
        for cf in changed_files:
            for suite_name, patterns in self.suite_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, cf.path):
                        affected.add(suite_name)
                        break
        return affected

    def unchanged_suites(
        self,
        all_suites: List[str],
        changed_files: List[ChangedFile],
    ) -> List[str]:
        """Return suites that are NOT affected by the changes."""
        affected = self.map_to_suites(changed_files)
        return [s for s in all_suites if s not in affected]


# ---------------------------------------------------------------------------
# 5. History Tracker
# ---------------------------------------------------------------------------


@dataclass
class HistoryEntry:
    """A single CI run stored in history."""

    run_id: str
    timestamp: str
    repo_name: str
    status: PipelineStatus
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    duration: float = 0.0
    suite_results: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TrendReport:
    """Pass-rate trend over a time window."""

    period_days: int
    total_runs: int
    pass_rate_avg: float
    pass_rate_min: float
    pass_rate_max: float
    pass_rate_trend: str  # "improving", "stable", "declining"
    daily_rates: List[Dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(
            {
                "period_days": self.period_days,
                "total_runs": self.total_runs,
                "pass_rate_avg": round(self.pass_rate_avg, 2),
                "pass_rate_min": round(self.pass_rate_min, 2),
                "pass_rate_max": round(self.pass_rate_max, 2),
                "trend": self.pass_rate_trend,
                "daily_rates": self.daily_rates,
            },
            indent=2,
        )


@dataclass
class FlakyTestInfo:
    """Information about a flaky test."""

    test_name: str
    total_runs: int
    fail_count: int
    flake_rate: float


class CIHistory:
    """Track CI results over time with trend analysis and flaky detection."""

    def __init__(self):
        self._entries: List[HistoryEntry] = []
        self._lock = threading.Lock()

    def add_entry(self, entry: HistoryEntry) -> None:
        with self._lock:
            self._entries.append(entry)

    def add_from_report(
        self,
        report: FleetTestReport,
        run_id: str = "",
    ) -> None:
        """Convenience: add entries from a FleetTestReport."""
        rid = run_id or report.timestamp
        for rr in report.repo_reports:
            status = (
                PipelineStatus.PASS
                if rr.total_failed == 0
                else PipelineStatus.FAIL
            )
            self.add_entry(
                HistoryEntry(
                    run_id=rid,
                    timestamp=report.timestamp,
                    repo_name=rr.repo_name,
                    status=status,
                    total_tests=rr.total_tests,
                    passed=rr.total_passed,
                    failed=rr.total_failed,
                    duration=report.duration,
                )
            )

    @property
    def entries(self) -> List[HistoryEntry]:
        with self._lock:
            return list(self._entries)

    def entries_for_repo(self, repo_name: str) -> List[HistoryEntry]:
        return [e for e in self.entries if e.repo_name == repo_name]

    def trend_analysis(self, days: int = 7) -> TrendReport:
        """Compute pass-rate trends over the last *days* days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        relevant: List[HistoryEntry] = []
        for e in self.entries:
            try:
                ts = datetime.fromisoformat(e.timestamp)
                if ts >= cutoff:
                    relevant.append(e)
            except (ValueError, TypeError):
                relevant.append(e)

        if not relevant:
            return TrendReport(
                period_days=days,
                total_runs=0,
                pass_rate_avg=0.0,
                pass_rate_min=0.0,
                pass_rate_max=0.0,
                pass_rate_trend="stable",
            )

        rates = [
            (e.passed / e.total_tests * 100) if e.total_tests > 0 else 100.0
            for e in relevant
        ]
        avg = sum(rates) / len(rates)
        mn = min(rates)
        mx = max(rates)

        # Simple trend: compare first half vs second half
        mid = len(rates) // 2
        if mid == 0:
            trend = "stable"
        else:
            first_half = sum(rates[:mid]) / mid
            second_half = sum(rates[mid:]) / (len(rates) - mid)
            diff = second_half - first_half
            if diff > 2.0:
                trend = "improving"
            elif diff < -2.0:
                trend = "declining"
            else:
                trend = "stable"

        # Daily breakdown
        daily: Dict[str, List[float]] = {}
        for e in relevant:
            try:
                day = e.timestamp[:10]
            except (TypeError, IndexError):
                day = "unknown"
            rate = (e.passed / e.total_tests * 100) if e.total_tests > 0 else 100.0
            daily.setdefault(day, []).append(rate)

        daily_rates = [
            {"date": d, "avg_rate": round(sum(v) / len(v), 2), "runs": len(v)}
            for d, v in sorted(daily.items())
        ]

        return TrendReport(
            period_days=days,
            total_runs=len(relevant),
            pass_rate_avg=avg,
            pass_rate_min=mn,
            pass_rate_max=mx,
            pass_rate_trend=trend,
            daily_rates=daily_rates,
        )

    def flaky_test_detector(
        self, threshold: float = 0.3
    ) -> List[FlakyTestInfo]:
        """Detect tests that sometimes fail.

        Iterates over ``suite_results`` inside history entries.  Only
        tests whose failure rate is above *threshold* (but below 1.0)
        are considered flaky.
        """
        test_runs: Dict[str, Dict[str, int]] = {}
        with self._lock:
            for entry in self._entries:
                for sr in entry.suite_results:
                    suite = sr.get("suite_name", "")
                    for t in sr.get("tests", []):
                        key = f"{suite}::{t.get('name', '')}"
                        if key not in test_runs:
                            test_runs[key] = {"total": 0, "fail": 0}
                        test_runs[key]["total"] += 1
                        if t.get("status") in ("failed", "error"):
                            test_runs[key]["fail"] += 1

        flaky: List[FlakyTestInfo] = []
        for name, counts in test_runs.items():
            total = counts["total"]
            fails = counts["fail"]
            if total < 2:
                continue
            rate = fails / total
            if threshold <= rate < 1.0:
                flaky.append(
                    FlakyTestInfo(
                        test_name=name,
                        total_runs=total,
                        fail_count=fails,
                        flake_rate=rate,
                    )
                )
        flaky.sort(key=lambda f: f.flake_rate, reverse=True)
        return flaky

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
