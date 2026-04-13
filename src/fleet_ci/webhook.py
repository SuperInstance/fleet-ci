"""Webhook notification system for cross-repo fleet coordination.

Receives GitHub webhook events (push, pull_request, issues, workflow_run),
routes notifications to fleet repos based on configurable rules, and provides
a message-in-a-bottle system for fleet-wide communication.

Usage as a library::

    from fleet_ci.webhook import WebhookRouter, MessageBottle, WebhookServer

    router = WebhookRouter.from_config("webhook_config.json")
    bottle = MessageBottle(max_messages=500)

    # Route an event
    targets = router.route("push", payload)
    bottle.cast(source="flux-vm-python", event_type="push", payload=payload)

    # Pick up messages
    messages = bottle.retrieve(target="fleet-ci")

Usage as a server::

    python -m fleet_ci.webhook --config webhook_config.json --port 8080
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Event Types & Models
# ---------------------------------------------------------------------------


class WebhookEventType(str, Enum):
    """Supported GitHub webhook event types."""

    PUSH = "push"
    PULL_REQUEST = "pull_request"
    ISSUES = "issues"
    WORKFLOW_RUN = "workflow_run"
    PING = "ping"


class MessagePriority(str, Enum):
    """Priority levels for bottle messages."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class WebhookEvent:
    """Parsed representation of an incoming GitHub webhook event."""

    event_type: WebhookEventType
    action: str = ""
    source_repo: str = ""
    source_branch: str = ""
    sender: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    raw_body: str = ""
    received_at: str = field(default_factory=lambda: _now_iso())
    signature_valid: bool = False

    # Optional fields populated by certain event types
    labels: List[str] = field(default_factory=list)
    conclusion: str = ""  # for workflow_run events

    @classmethod
    def from_github_payload(
        cls,
        event_type: str,
        headers: Dict[str, str],
        body: str,
    ) -> WebhookEvent:
        """Parse a GitHub webhook delivery into a structured event."""
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}

        et = WebhookEventType(event_type) if event_type in WebhookEventType._value2member_map_ else WebhookEventType.PING
        action = payload.get("action", "")

        # Extract common fields
        repo_info = payload.get("repository", {})
        source_repo = repo_info.get("full_name", "")

        # Branch detection differs by event type
        source_branch = ""
        if et == WebhookEventType.PUSH:
            ref = payload.get("ref", "")
            source_branch = ref.replace("refs/heads/", "") if ref.startswith("refs/heads/") else ref
        elif et == WebhookEventType.PULL_REQUEST:
            pr = payload.get("pull_request", {})
            source_branch = pr.get("head", {}).get("ref", "")
        elif et == WebhookEventType.WORKFLOW_RUN:
            wr = payload.get("workflow_run", {})
            source_branch = wr.get("head_branch", "")

        sender_info = payload.get("sender", {})
        sender = sender_info.get("login", "")

        # Labels (issues / PR events)
        labels = []
        if et in (WebhookEventType.ISSUES, WebhookEventType.PULL_REQUEST):
            issue_obj = payload.get("issue") or payload.get("pull_request", {})
            for lbl in issue_obj.get("labels", []):
                labels.append(lbl.get("name", ""))

        # Conclusion (workflow_run)
        conclusion = ""
        if et == WebhookEventType.WORKFLOW_RUN:
            conclusion = payload.get("workflow_run", {}).get("conclusion", "")

        return cls(
            event_type=et,
            action=action,
            source_repo=source_repo,
            source_branch=source_branch,
            sender=sender,
            payload=payload,
            raw_body=body,
            labels=labels,
            conclusion=conclusion,
        )


@dataclass
class BottleMessage:
    """A single message in the bottle system."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    source: str = ""
    event_type: str = ""
    target: str = "*"  # "*" = broadcast to all
    priority: MessagePriority = MessagePriority.NORMAL
    payload: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: _now_iso())
    ttl_seconds: float = 86400.0
    picked_up: bool = False
    picked_up_by: str = ""

    @property
    def is_expired(self) -> bool:
        try:
            created = datetime.fromisoformat(self.created_at)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) > created + timedelta(seconds=self.ttl_seconds)
        except (ValueError, TypeError):
            return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "event_type": self.event_type,
            "target": self.target,
            "priority": self.priority.value,
            "payload": self.payload,
            "created_at": self.created_at,
            "ttl_seconds": self.ttl_seconds,
            "picked_up": self.picked_up,
            "picked_up_by": self.picked_up_by,
        }


# ---------------------------------------------------------------------------
# 2. Routing Configuration
# ---------------------------------------------------------------------------


@dataclass
class RouteRule:
    """A single routing rule that maps events to target repos."""

    name: str
    event: str
    actions: List[str] = field(default_factory=list)
    branches: List[str] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    conclusion: List[str] = field(default_factory=list)
    target_repos: List[str] = field(default_factory=list)
    notify: bool = True
    priority: str = "normal"

    def matches(self, wh_event: WebhookEvent) -> bool:
        """Check if this rule matches the given webhook event."""
        # Event type must match
        if self.event != wh_event.event_type.value:
            return False

        # Action filter
        if self.actions and wh_event.action not in self.actions:
            return False

        # Branch filter
        if self.branches and wh_event.source_branch not in self.branches:
            return False

        # Label filter (any of the rule labels must be present in the event)
        if self.labels:
            if not any(lbl in wh_event.labels for lbl in self.labels):
                return False

        # Conclusion filter (workflow_run)
        if self.conclusion and wh_event.conclusion not in self.conclusion:
            return False

        return True


# ---------------------------------------------------------------------------
# 3. Webhook Router
# ---------------------------------------------------------------------------


class WebhookRouter:
    """Routes incoming webhook events to target fleet repos based on rules."""

    def __init__(self, rules: Optional[List[RouteRule]] = None, fleet_repos: Optional[List[str]] = None):
        self.rules: List[RouteRule] = rules or []
        self.fleet_repos: List[str] = fleet_repos or []
        self._event_log: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, config_path: str) -> WebhookRouter:
        """Load router configuration from a JSON file."""
        with open(config_path, "r") as f:
            config = json.load(f)

        rules = []
        for r in config.get("routes", []):
            rules.append(RouteRule(
                name=r.get("name", ""),
                event=r.get("event", ""),
                actions=r.get("actions", []),
                branches=r.get("branches", []),
                labels=r.get("labels", []),
                conclusion=r.get("conclusion", []),
                target_repos=r.get("target_repos", []),
                notify=r.get("notify", True),
                priority=r.get("priority", "normal"),
            ))

        fleet_repos = config.get("fleet_repos", [])
        return cls(rules=rules, fleet_repos=fleet_repos)

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> WebhookRouter:
        """Load router configuration from a dictionary."""
        rules = []
        for r in config.get("routes", []):
            rules.append(RouteRule(
                name=r.get("name", ""),
                event=r.get("event", ""),
                actions=r.get("actions", []),
                branches=r.get("branches", []),
                labels=r.get("labels", []),
                conclusion=r.get("conclusion", []),
                target_repos=r.get("target_repos", []),
                notify=r.get("notify", True),
                priority=r.get("priority", "normal"),
            ))
        fleet_repos = config.get("fleet_repos", [])
        return cls(rules=rules, fleet_repos=fleet_repos)

    def route(self, event: WebhookEvent) -> Dict[str, Any]:
        """Determine which repos should be notified for a given event.

        Returns a dict with:
            - targets: list of resolved target repo names
            - rules_matched: list of rule names that matched
            - priorities: list of priority levels
            - should_notify: bool
        """
        targets: Set[str] = set()
        rules_matched: List[str] = []
        priorities: List[str] = []
        any_notify = False

        for rule in self.rules:
            if rule.matches(event):
                rules_matched.append(rule.name)
                priorities.append(rule.priority)
                if rule.notify:
                    any_notify = True
                for target in rule.target_repos:
                    if target == "*":
                        targets.update(self.fleet_repos)
                    else:
                        targets.add(target)

        result = {
            "source": event.source_repo,
            "event_type": event.event_type.value,
            "action": event.action,
            "targets": sorted(targets),
            "rules_matched": rules_matched,
            "priorities": priorities,
            "should_notify": any_notify,
            "routed_at": _now_iso(),
        }

        with self._lock:
            self._event_log.append(result)

        return result

    def add_rule(self, rule: RouteRule) -> None:
        """Add a new routing rule."""
        self.rules.append(rule)

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name. Returns True if found and removed."""
        before = len(self.rules)
        self.rules = [r for r in self.rules if r.name != name]
        return len(self.rules) < before

    @property
    def event_log(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._event_log)

    def clear_log(self) -> None:
        with self._lock:
            self._event_log.clear()


# ---------------------------------------------------------------------------
# 4. Message-in-a-Bottle System
# ---------------------------------------------------------------------------


class MessageBottle:
    """Fleet communication via message-in-a-bottle pattern.

    Agents cast messages into the 'ocean' (broadcast channel) and other
    agents can pick up bottles addressed to them or addressed to '*' (all).
    """

    def __init__(self, max_messages: int = 1000, default_ttl: float = 86400.0):
        self._messages: List[BottleMessage] = []
        self._max_messages = max_messages
        self._default_ttl = default_ttl
        self._lock = threading.Lock()

    def cast(
        self,
        source: str,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
        target: str = "*",
        priority: str = "normal",
        ttl_seconds: Optional[float] = None,
    ) -> BottleMessage:
        """Cast a new message bottle into the ocean."""
        msg = BottleMessage(
            source=source,
            event_type=event_type,
            target=target,
            priority=MessagePriority(priority),
            payload=payload or {},
            ttl_seconds=ttl_seconds or self._default_ttl,
        )
        with self._lock:
            self._messages.append(msg)
            self._enforce_limit()
        logger.info("Bottle cast: %s -> %s [%s] id=%s", source, target, event_type, msg.id)
        return msg

    def retrieve(
        self,
        target: str,
        event_type: Optional[str] = None,
        unpicked_only: bool = True,
        mark_picked: bool = True,
        picked_by: str = "",
    ) -> List[BottleMessage]:
        """Pick up bottles addressed to a target.

        Args:
            target: Repo name to retrieve bottles for (or specific filter).
            event_type: Optionally filter by event type.
            unpicked_only: Only return messages that haven't been picked up.
            mark_picked: Mark retrieved messages as picked up.
            picked_by: Who is picking up the message.

        Returns:
            List of matching BottleMessage objects.
        """
        with self._lock:
            self._purge_expired()
            results: List[BottleMessage] = []
            for msg in self._messages:
                if msg.is_expired:
                    continue
                if unpicked_only and msg.picked_up:
                    continue
                # Match target: exact match or broadcast (*)
                if msg.target != "*" and msg.target != target:
                    continue
                if event_type and msg.event_type != event_type:
                    continue
                if mark_picked:
                    msg.picked_up = True
                    msg.picked_up_by = picked_by
                results.append(msg)
            return results

    def retrieve_all(self, unpicked_only: bool = True) -> List[BottleMessage]:
        """Retrieve all bottles (broadcast listener)."""
        with self._lock:
            self._purge_expired()
            results = []
            for msg in self._messages:
                if msg.is_expired:
                    continue
                if unpicked_only and msg.picked_up:
                    continue
                results.append(msg)
            return results

    def broadcast_from_event(
        self,
        routing_result: Dict[str, Any],
        event: WebhookEvent,
        ttl_seconds: Optional[float] = None,
    ) -> List[BottleMessage]:
        """Broadcast a webhook event to all routing targets as bottles."""
        messages = []
        priority = "normal"
        for p in routing_result.get("priorities", []):
            if p in ("high", "critical"):
                priority = p
                break

        for target_repo in routing_result.get("targets", []):
            msg = self.cast(
                source=routing_result.get("source", ""),
                event_type=routing_result.get("event_type", ""),
                payload=event.payload,
                target=target_repo,
                priority=priority,
                ttl_seconds=ttl_seconds,
            )
            messages.append(msg)
        return messages

    @property
    def message_count(self) -> int:
        with self._lock:
            return len(self._messages)

    def _enforce_limit(self) -> None:
        """Remove oldest messages when over the limit."""
        while len(self._messages) > self._max_messages:
            self._messages.pop(0)

    def _purge_expired(self) -> None:
        """Remove expired messages."""
        self._messages = [m for m in self._messages if not m.is_expired]

    def clear(self) -> None:
        with self._lock:
            self._messages.clear()

    def status(self) -> Dict[str, Any]:
        """Return status information about the bottle system."""
        with self._lock:
            total = len(self._messages)
            picked = sum(1 for m in self._messages if m.picked_up)
            expired = sum(1 for m in self._messages if m.is_expired)
            return {
                "total_messages": total,
                "picked_up": picked,
                "unpicked": total - picked,
                "expired": expired,
                "max_messages": self._max_messages,
            }


# ---------------------------------------------------------------------------
# 5. Signature Verification
# ---------------------------------------------------------------------------


def verify_github_signature(body: str, signature: str, secret: str) -> bool:
    """Verify the HMAC-SHA256 signature of a GitHub webhook payload.

    GitHub sends the signature as ``sha256=<hex_digest>`` in the
    ``X-Hub-Signature-256`` header.
    """
    if not secret:
        return True  # No secret configured — skip verification
    if not signature:
        return False
    if "=" in signature:
        algo, sig_hex = signature.split("=", 1)
        expected_hash = hmac.new(
            secret.encode("utf-8"),
            body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(sig_hex, expected_hash)
    return False


# ---------------------------------------------------------------------------
# 6. Webhook Handler (HTTP Server)
# ---------------------------------------------------------------------------


class WebhookHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the webhook server."""

    router: WebhookRouter = None  # type: ignore[assignment]
    bottle: MessageBottle = None  # type: ignore[assignment]
    secret: str = ""

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            self._send_json(200, {"status": "ok", "service": "fleet-ci-webhook"})
        elif parsed.path == "/bottles":
            params = {}
            for part in (parsed.query or "").split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k] = v
            target = params.get("target", "*")
            event_type = params.get("event_type")
            unpicked = params.get("unpicked", "true").lower() == "true"
            messages = self.bottle.retrieve(
                target=target,
                event_type=event_type,
                unpicked_only=unpicked,
            )
            self._send_json(200, {
                "messages": [m.to_dict() for m in messages],
                "count": len(messages),
            })
        elif parsed.path == "/bottles/status":
            self._send_json(200, self.bottle.status())
        elif parsed.path == "/routes":
            self._send_json(200, {
                "rules": [
                    {
                        "name": r.name,
                        "event": r.event,
                        "actions": r.actions,
                        "branches": r.branches,
                        "target_repos": r.target_repos,
                        "notify": r.notify,
                    }
                    for r in self.router.rules
                ],
                "fleet_repos": self.router.fleet_repos,
            })
        elif parsed.path == "/log":
            self._send_json(200, {"events": self.router.event_log})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)

        if parsed.path != "/webhook":
            self._send_json(404, {"error": "not found. POST to /webhook"})
            return

        # Read body
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else ""

        # Extract headers
        event_type = self.headers.get("X-GitHub-Event", "ping")
        delivery_id = self.headers.get("X-GitHub-Delivery", "unknown")
        signature = self.headers.get("X-Hub-Signature-256", "")

        # Verify signature if secret is configured
        if self.secret and not verify_github_signature(body, signature, self.secret):
            self._send_json(401, {"error": "invalid signature"})
            return

        # Parse event
        headers_dict = dict(self.headers)
        try:
            event = WebhookEvent.from_github_payload(event_type, headers_dict, body)
            event.signature_valid = True
        except Exception as e:
            self._send_json(400, {"error": f"failed to parse event: {e}"})
            return

        # Route event
        routing = self.router.route(event)

        # Cast bottles to targets
        if routing["should_notify"]:
            bottles = self.bottle.broadcast_from_event(routing, event)
            routing["bottles_created"] = len(bottles)
        else:
            routing["bottles_created"] = 0

        routing["delivery_id"] = delivery_id
        logger.info(
            "Webhook received: %s from %s -> %d targets",
            event_type, event.source_repo, len(routing["targets"]),
        )

        self._send_json(200, routing)

    def _send_json(self, status_code: int, data: Any) -> None:
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Override to use our logger."""
        logger.debug("HTTP: %s", format % args)


class WebhookServer:
    """Manages the webhook HTTP server lifecycle."""

    def __init__(
        self,
        router: WebhookRouter,
        bottle: MessageBottle,
        host: str = "0.0.0.0",
        port: int = 8080,
        secret: str = "",
    ):
        self.host = host
        self.port = port
        self.secret = secret
        self.router = router
        self.bottle = bottle
        self._server: Optional[HTTPServer] = None

    @classmethod
    def from_config(cls, config_path: str) -> WebhookServer:
        """Create a server from a JSON configuration file."""
        with open(config_path, "r") as f:
            config = json.load(f)

        server_cfg = config.get("server", {})
        router = WebhookRouter.from_config(config_path)

        bottle_cfg = config.get("bottle", {})
        bottle = MessageBottle(
            max_messages=bottle_cfg.get("max_messages", 1000),
            default_ttl=bottle_cfg.get("default_ttl_seconds", 86400),
        )

        return cls(
            router=router,
            bottle=bottle,
            host=server_cfg.get("host", "0.0.0.0"),
            port=server_cfg.get("port", 8080),
            secret=server_cfg.get("secret", ""),
        )

    def serve_forever(self) -> None:
        """Start the server (blocks)."""
        WebhookHandler.router = self.router
        WebhookHandler.bottle = self.bottle
        WebhookHandler.secret = self.secret

        self._server = HTTPServer((self.host, self.port), WebhookHandler)
        logger.info("Webhook server listening on %s:%d", self.host, self.port)
        try:
            self._server.serve_forever()
        except KeyboardInterrupt:
            logger.info("Server shutting down")
        finally:
            self._server.server_close()

    def shutdown(self) -> None:
        """Shutdown the server."""
        if self._server:
            self._server.shutdown()


# ---------------------------------------------------------------------------
# 7. Convenience: Process a webhook event programmatically
# ---------------------------------------------------------------------------


def process_webhook_event(
    router: WebhookRouter,
    bottle: MessageBottle,
    event_type: str,
    body: str,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Process a single webhook event through routing and bottle casting.

    Convenience function for non-server usage.
    """
    headers = headers or {}
    event = WebhookEvent.from_github_payload(event_type, headers, body)
    routing = router.route(event)

    if routing["should_notify"]:
        bottles = bottle.broadcast_from_event(routing, event)
        routing["bottles_created"] = len(bottles)
    else:
        routing["bottles_created"] = 0

    return routing


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the webhook server from the command line."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Fleet-CI Webhook Server")
    parser.add_argument("--config", default="webhook_config.json", help="Path to config file")
    parser.add_argument("--port", type=int, default=None, help="Override server port")
    args = parser.parse_args()

    server = WebhookServer.from_config(args.config)
    if args.port:
        server.port = args.port
    server.serve_forever()


if __name__ == "__main__":
    main()
