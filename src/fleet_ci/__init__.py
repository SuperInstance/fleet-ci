"""Fleet CI — automated conformance testing, webhook notifications, and fleet coordination."""

__version__ = "0.1.0"

from fleet_ci.pipeline import (
    ChangedFile,
    ChangeDetector,
    ChangeType,
    CIHistory,
    ConformanceReport,
    FleetOrchestrator,
    FleetTestReport,
    NotificationConfig,
    PipelineConfig,
    RepoTestReport,
    RunStatus,
    TestResult,
    TestSuiteConfig,
    TestSuiteResult,
    ThresholdConfig,
    TrendReport,
)

from fleet_ci.webhook import (
    BottleMessage,
    MessageBottle,
    MessagePriority,
    RouteRule,
    WebhookEvent,
    WebhookEventType,
    WebhookRouter,
    WebhookServer,
    process_webhook_event,
    verify_github_signature,
)

__all__ = [
    "ChangedFile",
    "ChangeDetector",
    "ChangeType",
    "CIHistory",
    "ConformanceReport",
    "FleetOrchestrator",
    "FleetTestReport",
    "NotificationConfig",
    "PipelineConfig",
    "RepoTestReport",
    "RunStatus",
    "TestResult",
    "TestSuiteConfig",
    "TestSuiteResult",
    "ThresholdConfig",
    "TrendReport",
    "BottleMessage",
    "MessageBottle",
    "MessagePriority",
    "RouteRule",
    "WebhookEvent",
    "WebhookEventType",
    "WebhookRouter",
    "WebhookServer",
    "process_webhook_event",
    "verify_github_signature",
]
