# The Annotation Pipeline Is Production Infrastructure

**URL:** https://tianpan.co/blog/2026-04-15-annotation-pipeline-production-infrastructure
**Author:** Tian Pan, April 2026

## Key Arguments
- Annotation is a software engineering problem, not a data problem
- Spreadsheet-based annotation has 5 structural failure modes: static allocation, no versioning, invisible quality degradation, no guideline enforcement, no feedback path

## Production Architecture Components
- Ingestion & task creation with metadata routing
- Sharded workload queues by type/skill/priority
- Model-assisted pre-labeling (surfacing model failures)
- Human annotation with embedded guidelines
- Multi-stage review (annotate → peer review → senior adjudication)
- Consensus engine for multi-annotator reconciliation
- Versioned dataset store (immutable, reproducible)
- Monitoring dashboards (throughput, accuracy, agreement rates)

## Key Insight
Inter-annotator agreement (IAA) is a specification health signal, not an annotator quality signal. Low agreement usually means the annotation spec is poorly defined, not that annotators are bad.
