"""Tests for fleet_ci.webhook — webhook notification system for fleet coordination."""

from __future__ import annotations

import json
import time

import pytest

from fleet_ci.webhook import (
    BottleMessage,
    MessageBottle,
    MessagePriority,
    RouteRule,
    WebhookEvent,
    WebhookEventType,
    WebhookHandler,
    WebhookRouter,
    WebhookServer,
    verify_github_signature,
    process_webhook_event,
)


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def sample_config() -> dict:
    return {
        "routes": [
            {
                "name": "push-to-all",
                "event": "push",
                "branches": ["main", "develop"],
                "target_repos": ["*"],
                "notify": True,
            },
            {
                "name": "pr-review",
                "event": "pull_request",
                "actions": ["opened", "synchronize"],
                "target_repos": ["SuperInstance/fleet-ci"],
                "notify": True,
            },
            {
                "name": "bug-issues",
                "event": "issues",
                "actions": ["opened"],
                "labels": ["bug"],
                "target_repos": ["SuperInstance/fleet-ci"],
                "notify": True,
            },
            {
                "name": "workflow-fail",
                "event": "workflow_run",
                "actions": ["completed"],
                "conclusion": ["failure"],
                "target_repos": ["*"],
                "notify": True,
                "priority": "high",
            },
            {
                "name": "ignore-success",
                "event": "workflow_run",
                "actions": ["completed"],
                "conclusion": ["success"],
                "target_repos": [],
                "notify": False,
            },
        ],
        "fleet_repos": [
            "SuperInstance/fleet-ci",
            "SuperInstance/oracle1-index",
            "SuperInstance/flux-vm-python",
        ],
    }


@pytest.fixture
def router(sample_config) -> WebhookRouter:
    return WebhookRouter.from_dict(sample_config)


@pytest.fixture
def bottle() -> MessageBottle:
    return MessageBottle(max_messages=100, default_ttl=3600.0)


# =========================================================================
# 1. WebhookEventType tests
# =========================================================================


class TestWebhookEventType:
    def test_all_event_types(self):
        assert WebhookEventType.PUSH.value == "push"
        assert WebhookEventType.PULL_REQUEST.value == "pull_request"
        assert WebhookEventType.ISSUES.value == "issues"
        assert WebhookEventType.WORKFLOW_RUN.value == "workflow_run"
        assert WebhookEventType.PING.value == "ping"

    def test_from_string(self):
        assert WebhookEventType("push") == WebhookEventType.PUSH
        assert WebhookEventType("ping") == WebhookEventType.PING

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            WebhookEventType("nonexistent")


# =========================================================================
# 2. MessagePriority tests
# =========================================================================


class TestMessagePriority:
    def test_priority_levels(self):
        assert MessagePriority.LOW.value == "low"
        assert MessagePriority.NORMAL.value == "normal"
        assert MessagePriority.HIGH.value == "high"
        assert MessagePriority.CRITICAL.value == "critical"

    def test_from_string(self):
        assert MessagePriority("high") == MessagePriority.HIGH
        assert MessagePriority("normal") == MessagePriority.NORMAL


# =========================================================================
# 3. WebhookEvent tests
# =========================================================================


class TestWebhookEvent:
    def test_parse_push_event(self):
        body = json.dumps({
            "ref": "refs/heads/main",
            "repository": {"full_name": "SuperInstance/flux-vm-python"},
            "sender": {"login": "agent-1"},
            "commits": [{"message": "feat: add opcode"}],
        })
        event = WebhookEvent.from_github_payload("push", {}, body)
        assert event.event_type == WebhookEventType.PUSH
        assert event.source_repo == "SuperInstance/flux-vm-python"
        assert event.source_branch == "main"
        assert event.sender == "agent-1"

    def test_parse_pull_request_event(self):
        body = json.dumps({
            "action": "opened",
            "pull_request": {
                "head": {"ref": "feature-branch"},
                "labels": [{"name": "enhancement"}],
            },
            "repository": {"full_name": "SuperInstance/flux-vm-rust"},
            "sender": {"login": "agent-2"},
        })
        event = WebhookEvent.from_github_payload("pull_request", {}, body)
        assert event.event_type == WebhookEventType.PULL_REQUEST
        assert event.action == "opened"
        assert event.source_branch == "feature-branch"
        assert event.labels == ["enhancement"]

    def test_parse_issues_event(self):
        body = json.dumps({
            "action": "opened",
            "issue": {
                "labels": [{"name": "bug"}, {"name": "fleet-critical"}],
                "title": "Test failure in conformance",
            },
            "repository": {"full_name": "SuperInstance/fleet-ci"},
            "sender": {"login": "bot-1"},
        })
        event = WebhookEvent.from_github_payload("issues", {}, body)
        assert event.event_type == WebhookEventType.ISSUES
        assert event.labels == ["bug", "fleet-critical"]

    def test_parse_workflow_run_event(self):
        body = json.dumps({
            "action": "completed",
            "workflow_run": {
                "head_branch": "main",
                "conclusion": "failure",
                "name": "CI Pipeline",
            },
            "repository": {"full_name": "SuperInstance/flux-vm-go"},
            "sender": {"login": "github-actions"},
        })
        event = WebhookEvent.from_github_payload("workflow_run", {}, body)
        assert event.event_type == WebhookEventType.WORKFLOW_RUN
        assert event.action == "completed"
        assert event.conclusion == "failure"
        assert event.source_branch == "main"

    def test_parse_ping_event(self):
        event = WebhookEvent.from_github_payload("ping", {}, json.dumps({"zen": "Keep it simple"}))
        assert event.event_type == WebhookEventType.PING

    def test_parse_invalid_json(self):
        event = WebhookEvent.from_github_payload("push", {}, "not json")
        assert event.payload == {}
        assert event.event_type == WebhookEventType.PUSH

    def test_default_values(self):
        event = WebhookEvent.from_github_payload("ping", {}, "{}")
        assert event.action == ""
        assert event.source_repo == ""
        assert event.sender == ""
        assert event.labels == []
        assert event.conclusion == ""
        assert event.signature_valid is False


# =========================================================================
# 4. BottleMessage tests
# =========================================================================


class TestBottleMessage:
    def test_create_message(self):
        msg = BottleMessage(source="repo-a", event_type="push", target="*")
        assert msg.source == "repo-a"
        assert msg.target == "*"
        assert msg.picked_up is False
        assert msg.id != ""

    def test_is_expired_false(self):
        msg = BottleMessage(ttl_seconds=86400)
        assert msg.is_expired is False

    def test_is_expired_true(self):
        msg = BottleMessage(ttl_seconds=0)
        # Give a moment for time to pass
        time.sleep(0.01)
        assert msg.is_expired is True

    def test_to_dict(self):
        msg = BottleMessage(
            source="repo-a",
            event_type="pull_request",
            target="repo-b",
            priority=MessagePriority.HIGH,
            payload={"key": "value"},
        )
        d = msg.to_dict()
        assert d["source"] == "repo-a"
        assert d["event_type"] == "pull_request"
        assert d["target"] == "repo-b"
        assert d["priority"] == "high"
        assert d["payload"] == {"key": "value"}
        assert d["picked_up"] is False
        assert "id" in d
        assert "created_at" in d


# =========================================================================
# 5. RouteRule tests
# =========================================================================


class TestRouteRule:
    def test_push_matches_main(self):
        rule = RouteRule(name="test", event="push", branches=["main"], target_repos=["*"])
        event = WebhookEvent(event_type=WebhookEventType.PUSH, source_branch="main")
        assert rule.matches(event) is True

    def test_push_no_match_branch(self):
        rule = RouteRule(name="test", event="push", branches=["main"], target_repos=["*"])
        event = WebhookEvent(event_type=WebhookEventType.PUSH, source_branch="feature")
        assert rule.matches(event) is False

    def test_event_type_mismatch(self):
        rule = RouteRule(name="test", event="push", target_repos=["*"])
        event = WebhookEvent(event_type=WebhookEventType.ISSUES)
        assert rule.matches(event) is False

    def test_action_filter(self):
        rule = RouteRule(name="test", event="pull_request", actions=["opened"], target_repos=["*"])
        event_opened = WebhookEvent(event_type=WebhookEventType.PULL_REQUEST, action="opened")
        event_closed = WebhookEvent(event_type=WebhookEventType.PULL_REQUEST, action="closed")
        assert rule.matches(event_opened) is True
        assert rule.matches(event_closed) is False

    def test_label_filter(self):
        rule = RouteRule(name="test", event="issues", labels=["bug"], target_repos=["*"])
        event_bug = WebhookEvent(event_type=WebhookEventType.ISSUES, labels=["bug", "urgent"])
        event_enhance = WebhookEvent(event_type=WebhookEventType.ISSUES, labels=["enhancement"])
        assert rule.matches(event_bug) is True
        assert rule.matches(event_enhance) is False

    def test_conclusion_filter(self):
        rule = RouteRule(name="test", event="workflow_run", actions=["completed"], conclusion=["failure"], target_repos=["*"])
        event_fail = WebhookEvent(event_type=WebhookEventType.WORKFLOW_RUN, action="completed", conclusion="failure")
        event_ok = WebhookEvent(event_type=WebhookEventType.WORKFLOW_RUN, action="completed", conclusion="success")
        assert rule.matches(event_fail) is True
        assert rule.matches(event_ok) is False

    def test_empty_filters_match_anything(self):
        rule = RouteRule(name="test", event="push", target_repos=["*"])
        event = WebhookEvent(event_type=WebhookEventType.PUSH, action="anything", source_branch="any")
        assert rule.matches(event) is True


# =========================================================================
# 6. WebhookRouter tests
# =========================================================================


class TestWebhookRouter:
    def test_route_push_to_all(self, router):
        event = WebhookEvent(event_type=WebhookEventType.PUSH, source_branch="main", source_repo="SuperInstance/flux-vm-python")
        result = router.route(event)
        assert "SuperInstance/fleet-ci" in result["targets"]
        assert "SuperInstance/oracle1-index" in result["targets"]
        assert "SuperInstance/flux-vm-python" in result["targets"]
        assert "push-to-all" in result["rules_matched"]
        assert result["should_notify"] is True

    def test_route_push_non_main_not_matched(self, router):
        event = WebhookEvent(event_type=WebhookEventType.PUSH, source_branch="feature-xyz")
        result = router.route(event)
        assert len(result["targets"]) == 0
        assert result["should_notify"] is False

    def test_route_pr_opened(self, router):
        event = WebhookEvent(
            event_type=WebhookEventType.PULL_REQUEST,
            action="opened",
            source_repo="SuperInstance/flux-vm-rust",
        )
        result = router.route(event)
        assert "pr-review" in result["rules_matched"]
        assert "SuperInstance/fleet-ci" in result["targets"]

    def test_route_pr_closed_not_matched(self, router):
        event = WebhookEvent(
            event_type=WebhookEventType.PULL_REQUEST,
            action="closed",
            source_repo="SuperInstance/flux-vm-rust",
        )
        result = router.route(event)
        assert len(result["targets"]) == 0

    def test_route_issue_with_bug_label(self, router):
        event = WebhookEvent(
            event_type=WebhookEventType.ISSUES,
            action="opened",
            source_repo="SuperInstance/fleet-ci",
            labels=["bug", "urgent"],
        )
        result = router.route(event)
        assert "bug-issues" in result["rules_matched"]
        assert "SuperInstance/fleet-ci" in result["targets"]

    def test_route_issue_without_bug_label(self, router):
        event = WebhookEvent(
            event_type=WebhookEventType.ISSUES,
            action="opened",
            labels=["enhancement"],
        )
        result = router.route(event)
        assert "bug-issues" not in result["rules_matched"]

    def test_route_workflow_failure(self, router):
        event = WebhookEvent(
            event_type=WebhookEventType.WORKFLOW_RUN,
            action="completed",
            conclusion="failure",
            source_repo="SuperInstance/flux-vm-python",
        )
        result = router.route(event)
        assert "workflow-fail" in result["rules_matched"]
        assert result["should_notify"] is True
        assert "high" in result["priorities"]
        # Should route to all fleet repos
        assert len(result["targets"]) == 3

    def test_route_workflow_success_no_notify(self, router):
        event = WebhookEvent(
            event_type=WebhookEventType.WORKFLOW_RUN,
            action="completed",
            conclusion="success",
        )
        result = router.route(event)
        assert result["should_notify"] is False

    def test_event_log(self, router):
        event = WebhookEvent(event_type=WebhookEventType.PUSH, source_branch="main")
        router.route(event)
        router.route(event)
        assert len(router.event_log) == 2

    def test_clear_log(self, router):
        event = WebhookEvent(event_type=WebhookEventType.PUSH, source_branch="main")
        router.route(event)
        router.clear_log()
        assert len(router.event_log) == 0

    def test_add_rule(self):
        r = WebhookRouter(fleet_repos=["a", "b"])
        rule = RouteRule(name="custom", event="push", target_repos=["a"])
        r.add_rule(rule)
        assert len(r.rules) == 1
        event = WebhookEvent(event_type=WebhookEventType.PUSH)
        result = r.route(event)
        assert "custom" in result["rules_matched"]

    def test_remove_rule(self):
        r = WebhookRouter()
        rule = RouteRule(name="removable", event="push", target_repos=["*"])
        r.add_rule(rule)
        assert r.remove_rule("removable") is True
        assert len(r.rules) == 0
        assert r.remove_rule("nonexistent") is False

    def test_from_dict(self, sample_config):
        r = WebhookRouter.from_dict(sample_config)
        assert len(r.rules) == 5
        assert len(r.fleet_repos) == 3


# =========================================================================
# 7. MessageBottle tests
# =========================================================================


class TestMessageBottle:
    def test_cast_and_retrieve(self, bottle):
        bottle.cast(source="repo-a", event_type="push", payload={"ref": "main"})
        assert bottle.message_count == 1

    def test_retrieve_by_target(self, bottle):
        bottle.cast(source="repo-a", event_type="push", target="repo-b")
        bottle.cast(source="repo-c", event_type="issues", target="repo-d")

        msgs = bottle.retrieve(target="repo-b")
        assert len(msgs) == 1
        assert msgs[0].event_type == "push"

    def test_retrieve_broadcast(self, bottle):
        bottle.cast(source="repo-a", event_type="push", target="*")
        bottle.cast(source="repo-b", event_type="issues", target="repo-c")

        msgs_for_x = bottle.retrieve(target="repo-x")
        assert len(msgs_for_x) == 1  # Only the broadcast
        assert msgs_for_x[0].event_type == "push"

    def test_retrieve_unpicked_only(self, bottle):
        bottle.cast(source="a", event_type="push", target="*")
        # First retrieve marks as picked
        msgs1 = bottle.retrieve(target="*", unpicked_only=True)
        assert len(msgs1) == 1
        # Second retrieve should be empty
        msgs2 = bottle.retrieve(target="*", unpicked_only=True)
        assert len(msgs2) == 0

    def test_retrieve_all_including_picked(self, bottle):
        bottle.cast(source="a", event_type="push", target="*")
        bottle.retrieve(target="*")
        msgs = bottle.retrieve(target="*", unpicked_only=False)
        assert len(msgs) == 1

    def test_retrieve_with_event_type_filter(self, bottle):
        bottle.cast(source="a", event_type="push", target="*")
        bottle.cast(source="b", event_type="issues", target="*")
        msgs = bottle.retrieve(target="*", event_type="push")
        assert len(msgs) == 1
        assert msgs[0].event_type == "push"

    def test_retrieve_all(self, bottle):
        bottle.cast(source="a", event_type="push")
        bottle.cast(source="b", event_type="issues")
        all_msgs = bottle.retrieve_all(unpicked_only=True)
        assert len(all_msgs) == 2

    def test_mark_picked_by(self, bottle):
        bottle.cast(source="a", event_type="push", target="*")
        msgs = bottle.retrieve(target="*", mark_picked=True, picked_by="agent-1")
        assert msgs[0].picked_up is True
        assert msgs[0].picked_up_by == "agent-1"

    def test_max_messages_limit(self):
        b = MessageBottle(max_messages=3)
        for i in range(5):
            b.cast(source="a", event_type="push", payload={"i": i})
        assert b.message_count == 3
        # Oldest should be removed
        msgs = b.retrieve_all(unpicked_only=False)
        assert msgs[0].payload["i"] == 2  # First 2 were evicted

    def test_expired_messages_purged(self):
        b = MessageBottle(default_ttl=0)
        b.cast(source="a", event_type="push")
        time.sleep(0.01)
        msgs = b.retrieve(target="*")
        assert len(msgs) == 0

    def test_status(self, bottle):
        bottle.cast(source="a", event_type="push", target="*")
        bottle.cast(source="b", event_type="issues", target="c")
        bottle.retrieve(target="*")
        status = bottle.status()
        assert status["total_messages"] == 2
        assert status["picked_up"] == 1
        assert status["unpicked"] == 1

    def test_clear(self, bottle):
        bottle.cast(source="a", event_type="push")
        bottle.cast(source="b", event_type="issues")
        assert bottle.message_count == 2
        bottle.clear()
        assert bottle.message_count == 0

    def test_broadcast_from_event(self, bottle):
        routing = {
            "source": "flux-vm-python",
            "event_type": "push",
            "priorities": ["normal"],
            "targets": ["repo-a", "repo-b"],
            "should_notify": True,
        }
        event = WebhookEvent(
            event_type=WebhookEventType.PUSH,
            source_repo="flux-vm-python",
            payload={"ref": "refs/heads/main"},
        )
        bottles = bottle.broadcast_from_event(routing, event)
        assert len(bottles) == 2
        assert bottle.message_count == 2
        msgs_a = bottle.retrieve(target="repo-a")
        assert len(msgs_a) == 1


# =========================================================================
# 8. Signature Verification tests
# =========================================================================


class TestSignatureVerification:
    def test_valid_signature(self):
        import hashlib
        import hmac
        secret = "my-webhook-secret"
        body = '{"test": true}'
        sig = "sha256=" + hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
        assert verify_github_signature(body, sig, secret) is True

    def test_invalid_signature(self):
        assert verify_github_signature("{}", "sha256=badsig", "secret") is False

    def test_missing_signature(self):
        assert verify_github_signature("{}", "", "secret") is False

    def test_no_secret_accepts_anything(self):
        assert verify_github_signature("{}", "", "") is True
        assert verify_github_signature("{}", "sha256=anything", "") is True

    def test_malformed_signature(self):
        assert verify_github_signature("{}", "nosha", "secret") is False


# =========================================================================
# 9. process_webhook_event convenience tests
# =========================================================================


class TestProcessWebhookEvent:
    def test_push_creates_bottles(self, router, bottle):
        body = json.dumps({
            "ref": "refs/heads/main",
            "repository": {"full_name": "SuperInstance/flux-vm-python"},
            "sender": {"login": "agent-1"},
        })
        result = process_webhook_event(router, bottle, "push", body)
        assert result["should_notify"] is True
        assert result["bottles_created"] == 3  # All fleet repos
        assert bottle.message_count == 3

    def test_no_notify_no_bottles(self, router, bottle):
        body = json.dumps({
            "action": "completed",
            "workflow_run": {"conclusion": "success"},
            "repository": {"full_name": "SuperInstance/test"},
        })
        result = process_webhook_event(router, bottle, "workflow_run", body)
        assert result["should_notify"] is False
        assert result["bottles_created"] == 0
        assert bottle.message_count == 0


# =========================================================================
# 10. Integration tests
# =========================================================================


class TestIntegration:
    def test_full_push_flow(self, router, bottle):
        """Simulate a complete push event flow."""
        body = json.dumps({
            "ref": "refs/heads/main",
            "repository": {"full_name": "SuperInstance/flux-vm-python"},
            "sender": {"login": "greenhorn"},
            "commits": [
                {"message": "feat: add WASM opcode handler", "added": 3, "removed": 1}
            ],
        })

        # Process event
        result = process_webhook_event(router, bottle, "push", body)

        # Verify routing
        assert result["source"] == "SuperInstance/flux-vm-python"
        assert result["event_type"] == "push"
        assert len(result["targets"]) == 3
        assert "push-to-all" in result["rules_matched"]

        # Verify bottles were created
        assert result["bottles_created"] == 3

        # Verify each fleet repo can pick up its bottle
        for repo in ["SuperInstance/fleet-ci", "SuperInstance/oracle1-index", "SuperInstance/flux-vm-python"]:
            msgs = bottle.retrieve(target=repo)
            assert len(msgs) == 1
            assert msgs[0].source == "SuperInstance/flux-vm-python"
            assert msgs[0].event_type == "push"

    def test_workflow_failure_high_priority(self, router, bottle):
        """Verify workflow failures get high priority bottles."""
        body = json.dumps({
            "action": "completed",
            "workflow_run": {
                "head_branch": "main",
                "conclusion": "failure",
                "name": "CI Pipeline",
            },
            "repository": {"full_name": "SuperInstance/flux-vm-rust"},
            "sender": {"login": "github-actions"},
        })

        result = process_webhook_event(router, bottle, "workflow_run", body)
        assert "high" in result["priorities"]
        assert result["bottles_created"] == 3

        msgs = bottle.retrieve(target="SuperInstance/fleet-ci")
        assert len(msgs) == 1
        assert msgs[0].priority == MessagePriority.HIGH

    def test_multiple_events_accumulate(self, router, bottle):
        """Simulate multiple events and verify accumulation."""
        events_data = [
            ("push", {"ref": "refs/heads/main", "repository": {"full_name": "SuperInstance/flux-vm-python"}, "sender": {"login": "a"}}),
            ("push", {"ref": "refs/heads/develop", "repository": {"full_name": "SuperInstance/flux-vm-rust"}, "sender": {"login": "b"}}),
            ("pull_request", {"action": "opened", "pull_request": {"head": {"ref": "feat"}}, "repository": {"full_name": "SuperInstance/flux-vm-go"}, "sender": {"login": "c"}}),
        ]

        for et, payload in events_data:
            process_webhook_event(router, bottle, et, json.dumps(payload))

        # push to main: 3 bottles, push to develop: 3 bottles, PR opened: 1 bottle = 7
        assert bottle.message_count == 7
        assert len(router.event_log) == 3

    def test_event_log_tracks_all(self, router, bottle):
        """Verify the event log captures all routing decisions."""
        for i in range(5):
            event = WebhookEvent(
                event_type=WebhookEventType.PUSH,
                source_branch="main",
                source_repo=f"repo-{i}",
            )
            router.route(event)

        log = router.event_log
        assert len(log) == 5
        for entry in log:
            assert "targets" in entry
            assert "rules_matched" in entry
            assert "routed_at" in entry

    def test_bottle_ttl_expiry(self):
        """Verify bottles expire after TTL."""
        b = MessageBottle(default_ttl=0)
        b.cast(source="a", event_type="push", target="*")
        time.sleep(0.05)
        assert b.message_count == 1  # Still in store
        msgs = b.retrieve(target="*")  # Should purge expired
        assert len(msgs) == 0

    def test_wildcard_route_expansion(self, router):
        """Verify wildcard routes expand to all fleet repos."""
        event = WebhookEvent(
            event_type=WebhookEventType.PUSH,
            source_branch="main",
        )
        result = router.route(event)
        assert len(result["targets"]) == 3
        assert set(result["targets"]) == set(router.fleet_repos)
