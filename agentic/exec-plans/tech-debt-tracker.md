# Tech Debt Tracker

Central registry of known technical debt in the art-tools repository. Items are organized by priority and tracked through resolution.

## Item Template

When adding a new item, copy this template:

```
### [DEBT-NNNN] Short description

- **Status:** open | in-progress | resolved
- **Owner:** @username (or unassigned)
- **Created:** YYYY-MM-DD
- **Impact:** Description of how this affects development, reliability, or performance
- **Workaround:** Current mitigation, if any
- **Fix:** Description of the intended fix
- **Effort:** S | M | L
- **Related Issues:** #issue-number, #issue-number
```

---

## High Priority

_Items that actively impede development, cause production issues, or block planned work._

(No items currently tracked.)

---

## Medium Priority

_Items that increase maintenance burden or slow development but have acceptable workarounds._

(No items currently tracked.)

---

## Low Priority

_Items that would improve code quality or developer experience but are not urgent._

(No items currently tracked.)

---

## Resolved (Recent)

_Recently resolved items, kept here for reference. Move to git history after 3 months._

(No items currently tracked.)

---

## How to Use

1. **Adding debt:** Copy the item template above into the appropriate priority section. Assign the next sequential `DEBT-NNNN` ID.
2. **Claiming work:** Set the owner to your handle and change status to `in-progress`.
3. **Resolving debt:** Move the item to the "Resolved (Recent)" section, update status to `resolved`, and add the date and PR link.
4. **Priority changes:** Re-evaluate priority during planning. Move items between sections as impact becomes clearer.
5. **Linking from exec plans:** When an exec plan defers cleanup, add a debt item here and reference it from the exec plan's completion checklist.
6. **Cleanup:** Items in "Resolved (Recent)" older than 3 months can be removed; git history preserves them.
