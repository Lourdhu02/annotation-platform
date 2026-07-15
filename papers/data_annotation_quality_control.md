# Data Annotation Quality Control: A 2026 Field Guide

**URL:** https://dataxpower.com/blog/data-annotation-quality-control
**Author:** Chris Pham, DataX Power, Sept 2025

## Seven Operational Artefacts
1. **Living guidelines** — versioned in source control, 3+ examples per class, hard cases appendix
2. **Annotator training & certification** — 90%+ accuracy gate, rolling re-certification every 4-6 weeks
3. **Inter-annotator agreement (IAA)** — Cohen's κ > 0.80 target, κ > 0.75 per class
4. **Gold panel validation** — 200-1,000 adjudicated examples, stratified by class difficulty
5. **Multi-pass review** — annotate → peer review → senior adjudication
6. **Statistical audit** — 5-10% stratified random sampling per batch
7. **Model-assisted QA** — model flags suspicious annotations for triage

## KPIs
- Headline gold panel accuracy (with CI)
- IAA per class
- Throughput-vs-accuracy correlation
- Error type distribution
- Disagreement-cluster report
- Audit pass rate
- Guideline-revision count
