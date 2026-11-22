"""Consumer policy model and deterministic conflict resolution.

Resolution semantics (no file-order overrides, ever):
  * rules match by consumer/purpose (exact or '*'), a path-prefix or tag
    selector, and optionally a join-path ('via') context;
  * among matching rules the key (priority, specificity) decides; specificity
    is the selector path length (tag selectors have specificity 0), so a more
    specific path beats a parent path at equal priority;
  * a tie between different actions at equal (priority, specificity) is a
    PolicyConflictError -- it is never resolved by file order;
  * a deny is lifted ONLY by an explicit path rule with strictly higher
    priority. Tag/wildcard rules and equal-priority rules cannot lift a deny.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .errors import PolicyConflictError

ACTIONS = ("allow", "mask", "aggregate", "deny")
STRICTNESS = {"allow": 0, "mask": 1, "aggregate": 2, "deny": 3}
MASK_STYLES = ("hash", "email_domain", "null")


@dataclass(frozen=True)
class Rule:
    rid: str
    kind: str                 # "path" | "tag"
    value: str                # path prefix or tag name
    action: str
    priority: int
    consumer: str = "*"
    purpose: str = "*"
    mask_style: str | None = None
    group_keys: tuple = ()
    stats: tuple = ()
    via: tuple = ()           # relation names; rule only applies in that join context
    reason: str = ""


def load_rules(policy_doc) -> list[Rule]:
    rules = []
    for raw in policy_doc.get("rules", []):
        if "path" in raw:
            kind, value = "path", raw["path"]
        elif "tag" in raw:
            kind, value = "tag", raw["tag"]
        else:
            raise PolicyConflictError(f"rule {raw.get('id')!r} has no selector ('path' or 'tag')")
        action = raw.get("action")
        if action not in ACTIONS:
            raise PolicyConflictError(f"rule {raw.get('id')!r}: unknown action {action!r}")
        mask_style = raw.get("mask_style")
        if mask_style is not None and mask_style not in MASK_STYLES:
            raise PolicyConflictError(f"rule {raw.get('id')!r}: unknown mask_style {mask_style!r}")
        if action == "aggregate" and (not raw.get("stats") or not raw.get("group_keys")):
            raise PolicyConflictError(
                f"aggregate rule {raw.get('id')!r} must declare 'stats' and 'group_keys'")
        rules.append(Rule(
            rid=raw["id"], kind=kind, value=value, action=action,
            priority=int(raw.get("priority", 0)),
            consumer=raw.get("consumer", "*"), purpose=raw.get("purpose", "*"),
            mask_style=mask_style,
            group_keys=tuple(raw.get("group_keys", ())),
            stats=tuple(raw.get("stats", ())),
            via=tuple(raw.get("via", ())),
            reason=raw.get("reason", "")))
    ids = [r.rid for r in rules]
    if len(set(ids)) != len(ids):
        raise PolicyConflictError("duplicate rule id in policy")
    return rules


def specificity(rule: Rule) -> int:
    return len(rule.value.split(".")) if rule.kind == "path" else 0


def _describe(rule: Rule) -> str:
    return f"{rule.kind} '{rule.value}'"


def matches(rule: Rule, consumer, purpose, path, tags, via_ctx) -> bool:
    if rule.consumer not in ("*", consumer):
        return False
    if rule.purpose not in ("*", purpose):
        return False
    if rule.via and not (set(rule.via) & set(via_ctx)):
        return False
    if rule.kind == "path":
        return path == rule.value or path.startswith(rule.value + ".")
    return rule.value in tags


@dataclass
class Decision:
    action: str
    covered: bool             # at least one rule matched
    rule: Rule | None         # winning rule
    lifted: Rule | None       # deny rule that was explicitly lifted
    blocked: Rule | None      # best non-deny rule that failed to lift a deny
    matched: list = field(default_factory=list)
    reason: str = ""


def resolve(rules, consumer, purpose, path, tags=(), via_ctx=()) -> Decision:
    cand = [r for r in rules if matches(r, consumer, purpose, path, tags, via_ctx)]
    if not cand:
        return Decision("allow", False, None, None, None, [],
                        "no matching policy rule; default allow "
                        "(permitted only because the field carries no sensitive tag)")

    denies = [r for r in cand if r.action == "deny"]
    others = [r for r in cand if r.action != "deny"]

    def key(r):
        return (r.priority, specificity(r))

    def top(rs):
        bk = max(key(r) for r in rs)
        tops = sorted((r for r in rs if key(r) == bk), key=lambda r: r.rid)
        acts = {r.action for r in tops}
        if len(acts) > 1:
            raise PolicyConflictError(
                f"policy conflict for '{path}': rules {[r.rid for r in tops]} tie at "
                f"(priority={bk[0]}, specificity={bk[1]}) with different actions "
                f"{sorted(acts)}; conflicts are never resolved by file order")
        return tops[0]

    best_deny = top(denies) if denies else None
    best_other = top(others) if others else None

    if best_deny is None:
        return Decision(best_other.action, True, best_other, None, None, cand,
                        f"matched rule {best_other.rid} ({best_other.action}, "
                        f"priority {best_other.priority}, {_describe(best_other)})")
    if best_other is None:
        return Decision("deny", True, best_deny, None, None, cand,
                        f"deny rule {best_deny.rid} (priority {best_deny.priority}, "
                        f"{_describe(best_deny)}) is the only matching rule")

    # both a deny and a non-deny match
    lifters = [r for r in others if r.kind == "path" and r.priority > best_deny.priority]
    if lifters:
        w = top(lifters)
        return Decision(w.action, True, w, best_deny, None, cand,
                        f"explicit path rule {w.rid} ({w.action}, priority {w.priority}) "
                        f"lifts deny rule {best_deny.rid} (priority {best_deny.priority}, "
                        f"{_describe(best_deny)})")
    if key(best_other) == key(best_deny) and best_other.kind == "path" and best_deny.kind == "path":
        raise PolicyConflictError(
            f"policy conflict for '{path}': rule {best_other.rid} ({best_other.action}) vs "
            f"deny rule {best_deny.rid} at equal priority {best_deny.priority} and "
            f"specificity; cannot be resolved by file order")
    return Decision("deny", True, best_deny, None, best_other, cand,
                    f"deny rule {best_deny.rid} (priority {best_deny.priority}, "
                    f"{_describe(best_deny)}) stands; best non-deny rule {best_other.rid} "
                    f"({best_other.action}, priority {best_other.priority}, "
                    f"{_describe(best_other)}) cannot lift it -- a deny is only lifted by "
                    f"a higher-priority explicit path rule")
