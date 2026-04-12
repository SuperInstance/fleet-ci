"""Tests for fleet_ci.pipeline — 50+ tests across all 6 components."""

from __future__ import annotations

import json
import time

import pytest

from fleet_ci.pipeline import (
    ChangeDetector,
    ChangeType,
    ChangedFile,
    CIHistory,
    ConformanceReport,
    FleetOrchestrator,
    FleetTestReport,
    HistoryEntry,
    NotificationConfig,
    PipelineConfig,
    PipelineStatus,
    RepoConfig,
    RepoTestReport,
    RunStatus,
    TestResult,
    TestRunner,
    TestSuiteConfig,
    TestSuiteResult,
    ThresholdConfig,
    TrendReport,
)


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def thresholds() -> ThresholdConfig:
    return ThresholdConfig(min_pass_rate=95.0, max_duration_seconds=300.0, required_coverage=80.0)


@pytest.fixture
def notifications() -> list[NotificationConfig]:
    return [
        NotificationConfig(channel="github_issues", target="SuperInstance/fleet-ci"),
        NotificationConfig(channel="commit_status", on_failure_only=True),
    ]


@pytest.fixture
def suite_configs() -> list[TestSuiteConfig]:
    return [
        TestSuiteConfig(name="conformance", suite_type="conformance", timeout=120),
        TestSuiteConfig(name="unit", suite_type="unit", timeout=60),
        TestSuiteConfig(name="integration", suite_type="integration", timeout=180),
        TestSuiteConfig(name="security", suite_type="security", timeout=90),
    ]


@pytest.fixture
def repo_configs() -> list[RepoConfig]:
    return [
        RepoConfig(name="flux-vm-python", url="https://github.com/SuperInstance/flux-vm-python", test_suites=["conformance", "unit"]),
        RepoConfig(name="flux-vm-rust", url="https://github.com/SuperInstance/flux-vm-rust", test_suites=["conformance", "unit"]),
        RepoConfig(name="flux-vm-go", url="https://github.com/SuperInstance/flux-vm-go", test_suites=["conformance", "unit", "integration"]),
    ]


@pytest.fixture
def pipeline_config(thresholds, notifications, suite_configs, repo_configs) -> PipelineConfig:
    return PipelineConfig(
        repos=repo_configs,
        test_suites=suite_configs,
        thresholds=thresholds,
        notifications=notifications,
        parallel=True,
        max_workers=4,
        dry_run=True,
    )


@pytest.fixture
def orchestrator(pipeline_config) -> FleetOrchestrator:
    return FleetOrchestrator(pipeline_config)


def _make_suite_result(
    name: str,
    passed: int = 0,
    failed: int = 0,
    skipped: int = 0,
) -> TestSuiteResult:
    results = []
    for i in range(passed):
        results.append(TestResult(suite_name=name, test_name=f"{name}_pass_{i}", status=RunStatus.PASSED, duration=0.1))
    for i in range(failed):
        results.append(TestResult(suite_name=name, test_name=f"{name}_fail_{i}", status=RunStatus.FAILED, duration=0.2, error_message="assertion failed"))
    for i in range(skipped):
        results.append(TestResult(suite_name=name, test_name=f"{name}_skip_{i}", status=RunStatus.SKIPPED))
    tsr = TestSuiteResult(suite_name=name, results=results)
    tsr.compute_summary()
    return tsr


def _make_repo_report(name: str, suite_results: list[TestSuiteResult]) -> RepoTestReport:
    return RepoTestReport(repo_name=name, suites=suite_results)


# =========================================================================
# 1. ThresholdConfig tests
# =========================================================================


class TestThresholdConfig:
    def test_default_values(self):
        t = ThresholdConfig()
        assert t.min_pass_rate == 100.0
        assert t.max_duration_seconds == 600.0
        assert t.required_coverage == 80.0

    def test_custom_values(self, thresholds):
        assert thresholds.min_pass_rate == 95.0
        assert thresholds.max_duration_seconds == 300.0
        assert thresholds.required_coverage == 80.0


# =========================================================================
# 2. NotificationConfig tests
# =========================================================================


class TestNotificationConfig:
    def test_defaults(self):
        n = NotificationConfig(channel="slack")
        assert n.target == ""
        assert n.on_failure_only is False
        assert n.on_success_only is False

    def test_github_issues(self):
        n = NotificationConfig(channel="github_issues", target="org/repo")
        assert n.channel == "github_issues"

    def test_failure_only_flag(self):
        n = NotificationConfig(channel="commit_status", on_failure_only=True)
        assert n.on_failure_only is True


# =========================================================================
# 3. TestSuiteConfig tests
# =========================================================================


class TestTestSuiteConfig:
    def test_defaults(self):
        s = TestSuiteConfig(name="unit", suite_type="unit")
        assert s.command == ""
        assert s.timeout == 120.0
        assert s.retry_count == 0
        assert s.tags == []

    def test_custom_config(self):
        s = TestSuiteConfig(name="conformance", suite_type="conformance", command="pytest tests/conformance", timeout=300, retry_count=2, tags=["critical"])
        assert s.command == "pytest tests/conformance"
        assert s.timeout == 300
        assert s.retry_count == 2
        assert "critical" in s.tags


# =========================================================================
# 4. RepoConfig tests
# =========================================================================


class TestRepoConfig:
    def test_defaults(self):
        r = RepoConfig(name="test-repo", url="https://github.com/test/repo")
        assert r.branch == "main"
        assert r.test_suites == []

    def test_with_suites(self):
        r = RepoConfig(name="flux-vm", url="https://github.com/SuperInstance/flux-vm", branch="develop", test_suites=["conformance", "unit"])
        assert r.branch == "develop"
        assert len(r.test_suites) == 2


# =========================================================================
# 5. PipelineConfig tests
# =========================================================================


class TestPipelineConfig:
    def test_defaults(self):
        p = PipelineConfig()
        assert p.repos == []
        assert p.test_suites == []
        assert isinstance(p.thresholds, ThresholdConfig)
        assert p.notifications == []
        assert p.parallel is True
        assert p.max_workers == 4
        assert p.dry_run is False

    def test_full_config(self, pipeline_config):
        assert len(pipeline_config.repos) == 3
        assert len(pipeline_config.test_suites) == 4
        assert pipeline_config.thresholds.min_pass_rate == 95.0
        assert len(pipeline_config.notifications) == 2


# =========================================================================
# 6. TestResult tests
# =========================================================================


class TestTestResult:
    def test_passed_result(self):
        tr = TestResult(suite_name="unit", test_name="test_add", status=RunStatus.PASSED, duration=0.05)
        assert tr.status == RunStatus.PASSED
        assert tr.duration == 0.05
        assert tr.error_message == ""

    def test_failed_result(self):
        tr = TestResult(suite_name="unit", test_name="test_divide", status=RunStatus.FAILED, duration=0.1, error_message="ZeroDivisionError")
        assert tr.error_message == "ZeroDivisionError"

    def test_timestamp_auto_set(self):
        tr = TestResult(suite_name="unit", test_name="t", status=RunStatus.PASSED)
        assert tr.timestamp != ""

    def test_all_statuses(self):
        for status in RunStatus:
            tr = TestResult(suite_name="s", test_name="t", status=status)
            assert tr.status == status


# =========================================================================
# 7. TestSuiteResult tests
# =========================================================================


class TestTestSuiteResult:
    def test_empty_suite(self):
        tsr = TestSuiteResult(suite_name="empty")
        tsr.compute_summary()
        assert tsr.total == 0
        assert tsr.passed == 0
        assert tsr.pass_rate == 100.0

    def test_all_passed(self):
        tsr = _make_suite_result("unit", passed=10)
        assert tsr.total == 10
        assert tsr.passed == 10
        assert tsr.failed == 0
        assert tsr.pass_rate == 100.0

    def test_mixed_results(self):
        tsr = _make_suite_result("unit", passed=7, failed=2, skipped=1)
        assert tsr.total == 10
        assert tsr.passed == 7
        assert tsr.failed == 2
        assert tsr.skipped == 1
        assert tsr.pass_rate == 70.0

    def test_all_failed(self):
        tsr = _make_suite_result("unit", failed=5)
        assert tsr.pass_rate == 0.0
        assert tsr.total == 5

    def test_compute_summary_recomputes(self):
        tsr = _make_suite_result("unit", passed=3, failed=1)
        assert tsr.passed == 3
        # Append more and recompute
        tsr.results.append(TestResult(suite_name="unit", test_name="extra_pass", status=RunStatus.PASSED, duration=0.1))
        tsr.compute_summary()
        assert tsr.passed == 4
        assert tsr.total == 5

    def test_duration_summed(self):
        tsr = _make_suite_result("unit", passed=3, failed=1)
        expected_duration = 3 * 0.1 + 1 * 0.2
        assert abs(tsr.duration - expected_duration) < 1e-9


# =========================================================================
# 8. TestRunner tests
# =========================================================================


class TestTestRunner:
    def test_dry_run(self):
        runner = TestRunner(TestSuiteConfig(name="conformance", suite_type="conformance"), dry_run=True)
        result = runner.run()
        assert result.suite_name == "conformance"
        assert result.total == 1
        assert result.skipped == 1

    def test_real_run_empty(self):
        runner = TestRunner(TestSuiteConfig(name="unit", suite_type="unit"), dry_run=False)
        result = runner.run()
        assert result.suite_name == "unit"
        assert result.total == 0

    def test_run_from_results(self):
        items = [
            {"test_name": "test_a", "status": "passed", "duration": 0.1},
            {"test_name": "test_b", "status": "failed", "duration": 0.2, "error_message": "boom"},
            {"test_name": "test_c", "status": "skipped"},
        ]
        tsr = TestRunner.run_from_results("suite1", items)
        assert tsr.suite_name == "suite1"
        assert tsr.total == 3
        assert tsr.passed == 1
        assert tsr.failed == 1
        assert tsr.skipped == 1
        assert tsr.pass_rate == pytest.approx(100 / 3)

    def test_run_from_results_empty(self):
        tsr = TestRunner.run_from_results("empty", [])
        assert tsr.total == 0
        assert tsr.pass_rate == 100.0


# =========================================================================
# 9. RepoTestReport tests
# =========================================================================


class TestRepoTestReport:
    def test_empty_report(self):
        rr = RepoTestReport(repo_name="test-repo")
        assert rr.total_tests == 0
        assert rr.total_passed == 0
        assert rr.pass_rate == 100.0

    def test_report_with_suites(self):
        s1 = _make_suite_result("conformance", passed=5)
        s2 = _make_suite_result("unit", passed=8, failed=2)
        rr = _make_repo_report("flux-vm-python", [s1, s2])
        assert rr.total_tests == 15
        assert rr.total_passed == 13
        assert rr.total_failed == 2
        assert rr.pass_rate == pytest.approx(13 / 15 * 100)

    def test_all_pass(self):
        s = _make_suite_result("unit", passed=20)
        rr = _make_repo_report("flux-vm-rust", [s])
        assert rr.total_failed == 0
        assert rr.pass_rate == 100.0


# =========================================================================
# 10. ConformanceReport tests
# =========================================================================


class TestConformanceReport:
    def test_all_pass(self):
        s1 = _make_suite_result("conformance", passed=10)
        rr1 = _make_repo_report("vm-python", [s1])
        s2 = _make_suite_result("conformance", passed=10)
        rr2 = _make_repo_report("vm-rust", [s2])
        cr = ConformanceReport(
            overall_status=PipelineStatus.PASS,
            repo_reports=[rr1, rr2],
            conformance_matrix={
                "vm-python": {"conformance": RunStatus.PASSED},
                "vm-rust": {"conformance": RunStatus.PASSED},
            },
        )
        assert cr.total_repos == 2
        assert cr.fully_conformant_count == 2

    def test_partial_conformance(self):
        s1 = _make_suite_result("conformance", passed=10)
        s2 = _make_suite_result("conformance", passed=7, failed=3)
        rr1 = _make_repo_report("vm-python", [s1])
        rr2 = _make_repo_report("vm-rust", [s2])
        cr = ConformanceReport(
            overall_status=PipelineStatus.PARTIAL,
            repo_reports=[rr1, rr2],
            conformance_matrix={
                "vm-python": {"conformance": RunStatus.PASSED},
                "vm-rust": {"conformance": RunStatus.FAILED},
            },
        )
        assert cr.fully_conformant_count == 1
        assert cr.overall_status == PipelineStatus.PARTIAL

    def test_to_json(self):
        cr = ConformanceReport(
            overall_status=PipelineStatus.PASS,
            repo_reports=[],
            conformance_matrix={"vm-a": {"conformance": RunStatus.PASSED}},
        )
        data = json.loads(cr.to_json())
        assert data["overall_status"] == "PASS"
        assert data["fully_conformant"] == 0


# =========================================================================
# 11. FleetTestReport tests
# =========================================================================


class TestFleetTestReport:
    def test_empty_report(self):
        fr = FleetTestReport(status=PipelineStatus.PASS)
        assert fr.total_tests == 0
        assert fr.total_passed == 0

    def test_totals(self):
        rr1 = _make_repo_report("a", [_make_suite_result("unit", passed=5)])
        rr2 = _make_repo_report("b", [_make_suite_result("unit", passed=3, failed=2)])
        fr = FleetTestReport(
            status=PipelineStatus.PARTIAL,
            repo_reports=[rr1, rr2],
            duration=10.5,
        )
        assert fr.total_tests == 10
        assert fr.total_passed == 8
        assert fr.total_failed == 2
        assert fr.duration == 10.5

    def test_to_markdown(self):
        rr = _make_repo_report("flux-vm-python", [_make_suite_result("conformance", passed=10)])
        fr = FleetTestReport(status=PipelineStatus.PASS, repo_reports=[rr], duration=5.0)
        md = fr.to_markdown()
        assert "# Fleet CI Report" in md
        assert "flux-vm-python" in md
        assert "PASS" in md

    def test_to_json(self):
        rr = _make_repo_report("flux-vm-rust", [_make_suite_result("unit", passed=8, failed=2)])
        fr = FleetTestReport(status=PipelineStatus.PARTIAL, repo_reports=[rr])
        data = json.loads(fr.to_json())
        assert data["status"] == "PARTIAL"
        assert data["total_tests"] == 10
        assert data["passed"] == 8

    def test_to_markdown_with_conformance(self):
        rr = _make_repo_report("vm-a", [_make_suite_result("conformance", passed=5)])
        cr = ConformanceReport(
            overall_status=PipelineStatus.PASS,
            repo_reports=[rr],
            conformance_matrix={"vm-a": {"conformance": RunStatus.PASSED}},
        )
        fr = FleetTestReport(status=PipelineStatus.PASS, repo_reports=[rr], conformance=cr)
        md = fr.to_markdown()
        assert "Conformance Matrix" in md


# =========================================================================
# 12. FleetOrchestrator tests
# =========================================================================


class TestFleetOrchestrator:
    def test_run_all_dry_run(self, orchestrator):
        report = orchestrator.run_all()
        assert isinstance(report, FleetTestReport)
        assert len(report.repo_reports) == 3
        assert report.total_tests > 0
        # dry-run produces SKIPPED results, so status should be PASS
        assert report.status in (PipelineStatus.PASS, PipelineStatus.PARTIAL)

    def test_run_all_has_conformance(self, orchestrator):
        report = orchestrator.run_all()
        assert report.conformance is not None
        assert isinstance(report.conformance, ConformanceReport)

    def test_run_all_duration(self, orchestrator):
        report = orchestrator.run_all()
        assert report.duration >= 0

    def test_run_repo(self, orchestrator):
        rr = orchestrator.run_repo("flux-vm-python")
        assert isinstance(rr, RepoTestReport)
        assert rr.repo_name == "flux-vm-python"
        assert len(rr.suites) == 2  # conformance + unit

    def test_run_repo_not_found(self, orchestrator):
        rr = orchestrator.run_repo("nonexistent")
        assert rr.repo_name == "nonexistent"
        assert rr.suites == []

    def test_run_conformance(self, orchestrator):
        cr = orchestrator.run_conformance()
        assert isinstance(cr, ConformanceReport)
        assert cr.total_repos == 3

    def test_sequential_mode(self, pipeline_config):
        pipeline_config.parallel = False
        orch = FleetOrchestrator(pipeline_config)
        report = orch.run_all()
        assert len(report.repo_reports) == 3

    def test_single_repo_config(self):
        cfg = PipelineConfig(
            repos=[RepoConfig(name="solo", url="https://github.com/test/solo", test_suites=["unit"])],
            test_suites=[TestSuiteConfig(name="unit", suite_type="unit")],
            dry_run=True,
        )
        orch = FleetOrchestrator(cfg)
        report = orch.run_all()
        assert len(report.repo_reports) == 1
        assert report.repo_reports[0].repo_name == "solo"

    def test_empty_config(self):
        cfg = PipelineConfig(dry_run=True)
        orch = FleetOrchestrator(cfg)
        report = orch.run_all()
        assert len(report.repo_reports) == 0
        assert report.total_tests == 0


# =========================================================================
# 13. ChangeDetector tests
# =========================================================================


class TestChangeDetector:
    def test_default_patterns(self):
        cd = ChangeDetector()
        assert "conformance" in cd.suite_patterns
        assert "unit" in cd.suite_patterns
        assert "integration" in cd.suite_patterns
        assert "security" in cd.suite_patterns

    def test_detect_changes_empty(self):
        cd = ChangeDetector()
        changes = cd.detect_changes("test-repo", "HEAD~1..HEAD")
        assert changes == []

    def test_detect_changes_from_list(self):
        cd = ChangeDetector()
        files = [
            {"path": "src/vm/engine.rs", "change_type": "modified", "additions": 10, "deletions": 2},
            {"path": "tests/unit/test_math.py", "change_type": "added"},
            {"path": "README.md", "change_type": "modified"},
        ]
        changes = cd.detect_changes_from_list(files)
        assert len(changes) == 3
        assert changes[0].path == "src/vm/engine.rs"
        assert changes[0].change_type == ChangeType.MODIFIED
        assert changes[0].additions == 10
        assert changes[1].change_type == ChangeType.ADDED

    def test_map_to_suites_vm_change(self):
        cd = ChangeDetector()
        changes = [ChangedFile(path="src/vm/engine.rs", change_type=ChangeType.MODIFIED)]
        affected = cd.map_to_suites(changes)
        assert "conformance" in affected
        assert "unit" in affected

    def test_map_to_suites_auth_change(self):
        cd = ChangeDetector()
        changes = [ChangedFile(path="src/auth/handler.py", change_type=ChangeType.MODIFIED)]
        affected = cd.map_to_suites(changes)
        assert "security" in affected
        assert "unit" in affected

    def test_map_to_suites_readme(self):
        cd = ChangeDetector()
        changes = [ChangedFile(path="README.md", change_type=ChangeType.MODIFIED)]
        affected = cd.map_to_suites(changes)
        # README shouldn't map to any test suite
        assert len(affected) == 0

    def test_map_to_suites_conformance_test(self):
        cd = ChangeDetector()
        changes = [ChangedFile(path="tests/conformance/test_instructions.wat", change_type=ChangeType.ADDED)]
        affected = cd.map_to_suites(changes)
        assert "conformance" in affected

    def test_unchanged_suites(self):
        cd = ChangeDetector()
        all_suites = ["conformance", "unit", "integration", "security"]
        changes = [ChangedFile(path="README.md", change_type=ChangeType.MODIFIED)]
        unchanged = cd.unchanged_suites(all_suites, changes)
        assert set(unchanged) == set(all_suites)

    def test_unchanged_suites_some_affected(self):
        cd = ChangeDetector()
        all_suites = ["conformance", "unit", "integration", "security"]
        changes = [ChangedFile(path="src/auth/token.py", change_type=ChangeType.MODIFIED)]
        unchanged = cd.unchanged_suites(all_suites, changes)
        assert "conformance" in unchanged
        assert "integration" in unchanged
        assert "security" not in unchanged

    def test_custom_patterns(self):
        patterns = {
            "custom": [r"custom/"],
        }
        cd = ChangeDetector(suite_patterns=patterns)
        changes = [ChangedFile(path="custom/module.py", change_type=ChangeType.ADDED)]
        affected = cd.map_to_suites(changes)
        assert "custom" in affected

    def test_multiple_changes_map(self):
        cd = ChangeDetector()
        changes = [
            ChangedFile(path="src/vm/engine.rs", change_type=ChangeType.MODIFIED),
            ChangedFile(path="tests/security/test_auth.py", change_type=ChangeType.ADDED),
        ]
        affected = cd.map_to_suites(changes)
        assert "conformance" in affected
        assert "security" in affected
        assert "unit" in affected


# =========================================================================
# 14. CIHistory tests
# =========================================================================


class TestCIHistory:
    def test_add_and_retrieve(self):
        h = CIHistory()
        entry = HistoryEntry(
            run_id="r1",
            timestamp="2025-01-15T10:00:00+00:00",
            repo_name="flux-vm-python",
            status=PipelineStatus.PASS,
            total_tests=50,
            passed=50,
            failed=0,
            duration=30.0,
        )
        h.add_entry(entry)
        assert len(h.entries) == 1
        assert h.entries[0].run_id == "r1"

    def test_clear(self):
        h = CIHistory()
        h.add_entry(HistoryEntry(run_id="r1", timestamp="2025-01-15T10:00:00+00:00", repo_name="a", status=PipelineStatus.PASS))
        h.clear()
        assert len(h.entries) == 0

    def test_entries_for_repo(self):
        h = CIHistory()
        h.add_entry(HistoryEntry(run_id="r1", timestamp="2025-01-15T10:00:00+00:00", repo_name="vm-python", status=PipelineStatus.PASS, total_tests=10, passed=10))
        h.add_entry(HistoryEntry(run_id="r2", timestamp="2025-01-15T11:00:00+00:00", repo_name="vm-rust", status=PipelineStatus.PASS, total_tests=20, passed=18, failed=2))
        h.add_entry(HistoryEntry(run_id="r3", timestamp="2025-01-15T12:00:00+00:00", repo_name="vm-python", status=PipelineStatus.PASS, total_tests=10, passed=9, failed=1))
        assert len(h.entries_for_repo("vm-python")) == 2
        assert len(h.entries_for_repo("vm-rust")) == 1
        assert len(h.entries_for_repo("vm-go")) == 0

    def test_add_from_report(self):
        h = CIHistory()
        rr = _make_repo_report("vm-a", [_make_suite_result("unit", passed=8, failed=2)])
        fr = FleetTestReport(status=PipelineStatus.PARTIAL, repo_reports=[rr], duration=10.0)
        h.add_from_report(fr, run_id="run-42")
        assert len(h.entries) == 1
        assert h.entries[0].run_id == "run-42"
        assert h.entries[0].total_tests == 10
        assert h.entries[0].failed == 2


# =========================================================================
# 15. TrendReport tests
# =========================================================================


def _recent_ts(hours_ago: int = 0) -> str:
    """Return a timezone-aware ISO timestamp relative to now."""
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()


def _recent_date_str() -> str:
    """Return today's date as YYYY-MM-DD."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class TestTrendReport:
    def test_empty_history(self):
        h = CIHistory()
        tr = h.trend_analysis(days=7)
        assert tr.total_runs == 0
        assert tr.pass_rate_avg == 0.0
        assert tr.pass_rate_trend == "stable"

    def test_single_entry(self):
        h = CIHistory()
        h.add_entry(HistoryEntry(run_id="r1", timestamp=_recent_ts(1), repo_name="a", status=PipelineStatus.PASS, total_tests=10, passed=10))
        tr = h.trend_analysis(days=7)
        assert tr.total_runs == 1
        assert tr.pass_rate_avg == 100.0
        assert tr.pass_rate_min == 100.0
        assert tr.pass_rate_max == 100.0

    def test_improving_trend(self):
        h = CIHistory()
        h.add_entry(HistoryEntry(run_id="r1", timestamp=_recent_ts(72), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=80))
        h.add_entry(HistoryEntry(run_id="r2", timestamp=_recent_ts(48), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=85))
        h.add_entry(HistoryEntry(run_id="r3", timestamp=_recent_ts(1), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=95))
        tr = h.trend_analysis(days=7)
        assert tr.pass_rate_trend == "improving"
        assert tr.pass_rate_avg == pytest.approx(86.67, abs=0.1)

    def test_declining_trend(self):
        h = CIHistory()
        h.add_entry(HistoryEntry(run_id="r1", timestamp=_recent_ts(72), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=95))
        h.add_entry(HistoryEntry(run_id="r2", timestamp=_recent_ts(48), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=85))
        h.add_entry(HistoryEntry(run_id="r3", timestamp=_recent_ts(1), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=70))
        tr = h.trend_analysis(days=7)
        assert tr.pass_rate_trend == "declining"

    def test_stable_trend(self):
        h = CIHistory()
        h.add_entry(HistoryEntry(run_id="r1", timestamp=_recent_ts(12), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=90))
        h.add_entry(HistoryEntry(run_id="r2", timestamp=_recent_ts(1), repo_name="a", status=PipelineStatus.PASS, total_tests=100, passed=91))
        tr = h.trend_analysis(days=7)
        assert tr.pass_rate_trend == "stable"

    def test_daily_rates(self):
        h = CIHistory()
        h.add_entry(HistoryEntry(run_id="r1", timestamp=_recent_ts(6), repo_name="a", status=PipelineStatus.PASS, total_tests=10, passed=8))
        h.add_entry(HistoryEntry(run_id="r2", timestamp=_recent_ts(3), repo_name="a", status=PipelineStatus.PASS, total_tests=10, passed=9))
        tr = h.trend_analysis(days=7)
        assert len(tr.daily_rates) == 1
        assert tr.daily_rates[0]["date"] == _recent_date_str()
        assert tr.daily_rates[0]["runs"] == 2

    def test_to_json(self):
        tr = TrendReport(
            period_days=7,
            total_runs=5,
            pass_rate_avg=92.5,
            pass_rate_min=85.0,
            pass_rate_max=100.0,
            pass_rate_trend="improving",
        )
        data = json.loads(tr.to_json())
        assert data["period_days"] == 7
        assert data["trend"] == "improving"
        assert data["pass_rate_avg"] == 92.5


# =========================================================================
# 16. Flaky Test Detector tests
# =========================================================================


class TestFlakyTestDetector:
    def test_no_flaky(self):
        h = CIHistory()
        entry = HistoryEntry(
            run_id="r1",
            timestamp="2025-01-15T10:00:00+00:00",
            repo_name="a",
            status=PipelineStatus.PASS,
            suite_results=[
                {
                    "suite_name": "unit",
                    "tests": [
                        {"name": "test_a", "status": "passed"},
                        {"name": "test_b", "status": "passed"},
                    ],
                }
            ],
        )
        h.add_entry(entry)
        flaky = h.flaky_test_detector()
        assert len(flaky) == 0

    def test_detect_flaky(self):
        h = CIHistory()
        for i in range(5):
            entry = HistoryEntry(
                run_id=f"r{i}",
                timestamp=f"2025-01-{15 + i}T10:00:00+00:00",
                repo_name="a",
                status=PipelineStatus.PARTIAL,
                suite_results=[
                    {
                        "suite_name": "unit",
                        "tests": [
                            {"name": "test_stable", "status": "passed"},
                            {"name": "test_flaky", "status": "failed" if i % 2 == 0 else "passed"},
                        ],
                    }
                ],
            )
            h.add_entry(entry)
        flaky = h.flaky_test_detector(threshold=0.2)
        assert len(flaky) == 1
        assert "test_flaky" in flaky[0].test_name
        assert flaky[0].flake_rate == 0.6

    def test_always_failing_not_flaky(self):
        h = CIHistory()
        for i in range(5):
            entry = HistoryEntry(
                run_id=f"r{i}",
                timestamp=f"2025-01-{15 + i}T10:00:00+00:00",
                repo_name="a",
                status=PipelineStatus.FAIL,
                suite_results=[
                    {
                        "suite_name": "unit",
                        "tests": [
                            {"name": "test_broken", "status": "failed"},
                        ],
                    }
                ],
            )
            h.add_entry(entry)
        flaky = h.flaky_test_detector(threshold=0.2)
        # rate is 1.0 → not flaky
        assert len(flaky) == 0

    def test_flaky_sorted_by_rate(self):
        h = CIHistory()
        for i in range(6):
            entry = HistoryEntry(
                run_id=f"r{i}",
                timestamp=f"2025-01-{15 + i}T10:00:00+00:00",
                repo_name="a",
                status=PipelineStatus.PARTIAL,
                suite_results=[
                    {
                        "suite_name": "s",
                        "tests": [
                            {"name": "a", "status": "failed" if i < 4 else "passed"},
                            {"name": "b", "status": "failed" if i == 0 else "passed"},
                        ],
                    }
                ],
            )
            h.add_entry(entry)
        flaky = h.flaky_test_detector(threshold=0.1)
        assert len(flaky) == 2
        # 'a' has higher flake rate (4/6 ≈ 0.67) vs 'b' (1/6 ≈ 0.17)
        assert flaky[0].test_name.endswith("::a")
        assert flaky[1].test_name.endswith("::b")

    def test_single_run_not_flaky(self):
        h = CIHistory()
        entry = HistoryEntry(
            run_id="r1",
            timestamp="2025-01-15T10:00:00+00:00",
            repo_name="a",
            status=PipelineStatus.PARTIAL,
            suite_results=[
                {
                    "suite_name": "unit",
                    "tests": [{"name": "test_x", "status": "failed"}],
                }
            ],
        )
        h.add_entry(entry)
        flaky = h.flaky_test_detector(threshold=0.1)
        # Only 1 run — not enough data
        assert len(flaky) == 0


# =========================================================================
# 17. Enum tests
# =========================================================================


class TestEnums:
    def test_run_status_values(self):
        assert RunStatus.PASSED.value == "passed"
        assert RunStatus.FAILED.value == "failed"
        assert RunStatus.SKIPPED.value == "skipped"
        assert RunStatus.ERROR.value == "error"

    def test_pipeline_status_values(self):
        assert PipelineStatus.PASS.value == "PASS"
        assert PipelineStatus.PARTIAL.value == "PARTIAL"
        assert PipelineStatus.FAIL.value == "FAIL"

    def test_change_type_values(self):
        assert ChangeType.ADDED.value == "added"
        assert ChangeType.MODIFIED.value == "modified"
        assert ChangeType.DELETED.value == "deleted"
        assert ChangeType.RENAMED.value == "renamed"


# =========================================================================
# 18. Integration tests
# =========================================================================


class TestIntegration:
    def test_full_pipeline_dry_run(self, pipeline_config):
        orch = FleetOrchestrator(pipeline_config)
        report = orch.run_all()

        # Verify report structure
        assert report.status in (PipelineStatus.PASS, PipelineStatus.PARTIAL)
        assert len(report.repo_reports) == 3
        assert report.conformance is not None

        # Verify markdown output
        md = report.to_markdown()
        assert "# Fleet CI Report" in md
        for repo in pipeline_config.repos:
            assert repo.name in md

        # Verify JSON output
        data = json.loads(report.to_json())
        assert "status" in data
        assert "repos" in data

    def test_history_with_orchestrator(self, pipeline_config):
        orch = FleetOrchestrator(pipeline_config)
        report = orch.run_all()

        h = CIHistory()
        h.add_from_report(report, run_id="integration-1")
        assert len(h.entries) == 3  # 3 repos

        tr = h.trend_analysis(days=7)
        assert tr.total_runs == 3

    def test_change_detector_with_orchestrator(self):
        cd = ChangeDetector()
        changes = cd.detect_changes_from_list([
            {"path": "src/vm/engine.rs", "change_type": "modified"},
            {"path": "src/auth/token.py", "change_type": "added"},
        ])
        affected = cd.map_to_suites(changes)
        assert "conformance" in affected
        assert "security" in affected
        assert "unit" in affected

    def test_conformance_report_json_roundtrip(self):
        s1 = _make_suite_result("conformance", passed=10)
        rr1 = _make_repo_report("vm-a", [s1])
        cr = ConformanceReport(
            overall_status=PipelineStatus.PASS,
            repo_reports=[rr1],
            conformance_matrix={"vm-a": {"conformance": RunStatus.PASSED}},
        )
        json_str = cr.to_json()
        data = json.loads(json_str)
        assert data["overall_status"] == "PASS"
        assert data["conformance_matrix"]["vm-a"]["conformance"] == "passed"

    def test_parallel_execution_consistent(self, pipeline_config):
        pipeline_config.parallel = True
        orch1 = FleetOrchestrator(pipeline_config)
        r1 = orch1.run_all()

        pipeline_config.parallel = False
        orch2 = FleetOrchestrator(pipeline_config)
        r2 = orch2.run_all()

        # Both should have the same number of repo reports
        assert len(r1.repo_reports) == len(r2.repo_reports)
        assert set(rr.repo_name for rr in r1.repo_reports) == set(rr.repo_name for rr in r2.repo_reports)
