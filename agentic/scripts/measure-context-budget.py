#!/usr/bin/env python3
"""
Measure documentation context budget for typical navigation paths.

Simulates agent workflows and measures how much documentation gets loaded.

Metrics:
- Total lines loaded per workflow
- Files accessed per workflow
- Context budget compliance

Usage:
    python3 agentic/scripts/measure-context-budget.py
    python3 agentic/scripts/measure-context-budget.py --max-budget 700
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict


def count_lines(file_path: Path) -> int:
    """Count non-empty lines in a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
            if lines and lines[0] == '---':
                try:
                    end_idx = lines[1:].index('---') + 2
                    lines = lines[end_idx:]
                except ValueError:
                    pass
            return len(lines)
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}", file=sys.stderr)
        return 0


class Workflow:
    """Represents a typical agent workflow."""

    def __init__(self, name: str, description: str, files: List[str]):
        self.name = name
        self.description = description
        self.files = files

    def measure(self, base_dir: Path) -> Dict:
        """Measure context budget for this workflow."""
        total_lines = 0
        file_details = []
        missing_files = []

        for file_pattern in self.files:
            file_path = base_dir / file_pattern

            if not file_path.exists():
                missing_files.append(file_pattern)
                continue

            lines = count_lines(file_path)
            total_lines += lines
            file_details.append({
                'path': file_pattern,
                'lines': lines
            })

        return {
            'name': self.name,
            'description': self.description,
            'total_lines': total_lines,
            'file_count': len(file_details),
            'files': file_details,
            'missing_files': missing_files
        }


# art-tools-specific workflows
WORKFLOWS = [
    Workflow(
        name="Bug Fix (Simple)",
        description="Find and fix a bug in existing code",
        files=[
            'AGENTS.md',
            'ARCHITECTURE.md',
            'agentic/DEVELOPMENT.md'
        ]
    ),
    Workflow(
        name="Bug Fix (Complex)",
        description="Debug an issue requiring domain knowledge",
        files=[
            'AGENTS.md',
            'ARCHITECTURE.md',
            'agentic/domain/glossary.md',
            'agentic/DEVELOPMENT.md',
            'agentic/TESTING.md'
        ]
    ),
    Workflow(
        name="Feature Implementation",
        description="Implement a new feature with exec-plan",
        files=[
            'AGENTS.md',
            'ARCHITECTURE.md',
            'agentic/design-docs/core-beliefs.md',
            'agentic/domain/glossary.md',
            'agentic/DESIGN.md',
            'agentic/DEVELOPMENT.md',
            'agentic/TESTING.md'
        ]
    ),
    Workflow(
        name="Understanding System",
        description="Learn how art-tools works",
        files=[
            'AGENTS.md',
            'ARCHITECTURE.md',
            'agentic/design-docs/core-beliefs.md',
            'agentic/domain/glossary.md'
        ]
    ),
    Workflow(
        name="Security Review",
        description="Review security implications of a change",
        files=[
            'AGENTS.md',
            'agentic/SECURITY.md',
            'agentic/design-docs/core-beliefs.md'
        ]
    ),
    Workflow(
        name="Advisory Management",
        description="Work on elliott advisory commands",
        files=[
            'AGENTS.md',
            'agentic/design-docs/components/elliott.md',
            'agentic/domain/concepts/errata-advisories.md',
            'agentic/domain/workflows/advisory-management.md'
        ]
    ),
    Workflow(
        name="Image Build Work",
        description="Work on doozer image build commands",
        files=[
            'AGENTS.md',
            'agentic/design-docs/components/doozer.md',
            'agentic/domain/concepts/distgit.md',
            'agentic/domain/workflows/image-build-lifecycle.md'
        ]
    ),
]


def print_workflow_report(result: Dict, max_budget: int):
    """Print report for a single workflow."""
    total = result['total_lines']
    over_budget = total > max_budget
    status = "OVER" if over_budget else "OK"

    print(f"\n{result['name']}")
    print(f"  {result['description']}")
    print(f"  Status: {status} ({total}/{max_budget} lines, {result['file_count']} files)")

    if result['missing_files']:
        print(f"  Missing files: {', '.join(result['missing_files'])}")

    if over_budget:
        print("  Files loaded:")
        for file in result['files']:
            print(f"    - {file['lines']:4d} lines: {file['path']}")


def print_summary(results: List[Dict], max_budget: int):
    """Print summary report."""
    print("\n" + "=" * 70)
    print("CONTEXT BUDGET ANALYSIS")
    print("=" * 70)
    print(f"Budget Limit: {max_budget} lines per workflow\n")

    passing = 0
    failing = 0

    for result in results:
        print_workflow_report(result, max_budget)
        if result['total_lines'] <= max_budget:
            passing += 1
        else:
            failing += 1

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Workflows tested:      {len(results)}")
    print(f"  Passing (<={max_budget} lines): {passing}")
    print(f"  Failing (>{max_budget} lines): {failing}")

    if results:
        max_observed = max(r['total_lines'] for r in results)
        avg_observed = sum(r['total_lines'] for r in results) / len(results)
        print(f"\n  Max observed:          {max_observed} lines")
        print(f"  Average observed:      {avg_observed:.0f} lines")

    print("\n" + "=" * 70)

    if failing == 0:
        print("PASSED: All workflows within budget")
        return True
    else:
        print(f"FAILED: {failing} workflows exceed budget")
        print("\nRecommendations:")
        print("  1. Split large files into smaller, focused documents")
        print("  2. Increase budget limit if justified by benchmarking")
        print("  3. Review if all linked docs are necessary for each workflow")
        return False


def main():
    parser = argparse.ArgumentParser(description='Measure context budget for workflows')
    parser.add_argument('--max-budget', type=int, default=700,
                        help='Maximum context budget in lines (default: 700)')
    parser.add_argument('--fail-on-violation', action='store_true',
                        help='Exit with error code if budget exceeded')

    args = parser.parse_args()

    base_dir = Path.cwd()

    results = []
    for workflow in WORKFLOWS:
        results.append(workflow.measure(base_dir))

    passed = print_summary(results, args.max_budget)

    if args.fail_on_violation and not passed:
        sys.exit(1)


if __name__ == '__main__':
    main()
