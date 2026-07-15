# Implementation Guide — Multi-Modal Annotation Quality & Pipeline Platform

## 1. Complete Project Structure

```
annotation-platform/
├── platform/                          # Core platform package
│   ├── __init__.py
│   │
│   ├── ingestion/                     # Data ingress
│   │   ├── __init__.py
│   │   ├── gateway.py                 # Ingest API server (FastAPI)
│   │   ├── validator.py              # Schema + integrity validator
│   │   ├── manifest.py               # Manifest creation & hashing
│   │   ├── de_identification.py      # PII scrubbing, face blurring
│   │   └── routes.py                 # /ingest endpoints
│   │
│   ├── tasks/                         # Task creation & routing
│   │   ├── __init__.py
│   │   ├── creator.py                # Task generating service
│   │   ├── router.py                 # Skill/priority/load routing
│   │   ├── schemas.py                # Pydantic task schemas (versioned)
│   │   └── queue.py                  # Redis queue adapter
│   │
│   ├── prelabel/                      # Model-assisted annotation
│   │   ├── __init__.py
│   │   ├── active_learner.py         # BALD uncertainty sampling
│   │   ├── detector.py               # YOLO/SAM model adapter
│   │   ├── reliability.py            # Confidence + agreement checker
│   │   └── ensemble.py              # Multi-model ensemble predictor
│   │
│   ├── annotation/                    # Human annotation service
│   │   ├── __init__.py
│   │   ├── interface.py             # Embedded guidelines engine
│   │   ├── validator.py             # Real-time submission validation
│   │   ├── shortcuts.py             # Keyboard binding config
│   │   └── honeypot.py              # Gold panel injection
│   │
│   ├── review/                        # Multi-stage review
│   │   ├── __init__.py
│   │   ├── assigner.py              # Peer/senior/reviewer assigner
│   │   ├── workflow.py              # Review state machine
│   │   ├── escalation.py            # Escalation rule engine
│   │   └── adjudication.py          # Final decision service
│   │
│   ├── consensus/                     # IAA & consensus engine
│   │   ├── __init__.py
│   │   ├── iaa.py                    # Cohen's κ, Fleiss' κ, α
│   │   ├── bootstrap.py             # Bootstrap CI computation
│   │   ├── gwet.py                   # Gwet's AC1 implementation
│   │   ├── clustering.py            # Disagreement cluster analysis
│   │   └── router.py                # Disagreement routing logic
│   │
│   ├── quality/                       # Gold panel & audit
│   │   ├── __init__.py
│   │   ├── gold_panel.py            # Panel creation & rotation
│   │   ├── calibration.py           # Annotator calibration pipeline
│   │   ├── audit.py                  # Statistical audit service
│   │   ├── sampling.py              # Stratified acceptance sampling
│   │   └── metrics.py               # Quality metric aggregator
│   │
│   ├── dataset/                       # Versioned dataset store
│   │   ├── __init__.py
│   │   ├── versioning.py            # Snapshot creation & semver
│   │   ├── content_addr.py          # SHA-256 content addressing
│   │   ├── provenance.py            # Lineage DAG tracking
│   │   ├── export.py                # Format export engine
│   │   └── store.py                 # Object store adapter
│   │
│   ├── api/                           # REST API layer
│   │   ├── __init__.py
│   │   ├── app.py                    # FastAPI application
│   │   ├── routes_tasks.py           # /tasks endpoints
│   │   ├── routes_annotations.py     # /annotations endpoints
│   │   ├── routes_review.py          # /review endpoints
│   │   ├── routes_audit.py           # /audit endpoints
│   │   ├── routes_datasets.py        # /datasets endpoints
│   │   ├── routes_metrics.py         # /metrics endpoints
│   │   └── middleware.py             # Auth, logging, rate limiting
│   │
│   ├── adapters/                      # Third-party integrations
│   │   ├── __init__.py
│   │   ├── label_studio.py           # Label Studio adapter
│   │   ├── cvat.py                   # CVAT adapter
│   │   ├── labelbox.py               # Labelbox adapter
│   │   └── encord.py                 # Encord adapter
│   │
│   ├── storage/                       # Storage abstraction
│   │   ├── __init__.py
│   │   ├── object_store.py          # S3/GCS/Azure abstraction
│   │   ├── database.py              # PostgreSQL connection pool
│   │   ├── cache.py                  # Redis connection + pool
│   │   └── migrations/              # Alembic DB migrations
│   │
│   ├── monitoring/                    # Observability
│   │   ├── __init__.py
│   │   ├── metrics.py               # Prometheus metric definitions
│   │   ├── dashboards/              # Grafana dashboard JSON
│   │   ├── alerts.py                # Alert rule definitions
│   │   └── tracing.py               # OpenTelemetry tracing
│   │
│   └── models/                        # Data models
│       ├── __init__.py
│       ├── task.py                    # Task model
│       ├── annotation.py              # Annotation model
│       ├── reviewer.py                # Reviewer/annotator profile
│       ├── gold_panel.py             # Gold panel model
│       ├── audit.py                  # Audit model
│       ├── dataset.py                # Dataset version model
│       └── iaa_report.py            # IAA report model
│
├── tests/                             # Test suite
│   ├── unit/
│   │   ├── test_iaa.py
│   │   ├── test_router.py
│   │   ├── test_gold_panel.py
│   │   ├── test_sampling.py
│   │   ├── test_versioning.py
│   │   └── test_export.py
│   ├── integration/
│   │   ├── test_ingestion_pipeline.py
│   │   ├── test_review_workflow.py
│   │   └── test_consensus_pipeline.py
│   └── benchmarks/
│       ├── benchmark_iaa.py
│       └── benchmark_queue.py
│
├── config/                            # Configuration
│   ├── default.yaml                   # Default config
│   ├── production.yaml                # Production overrides
│   └── schemas/                       # Versioned task schemas
│       ├── detection_v1.json
│       ├── detection_v2.json
│       └── segmentation_v1.json
│
├── docker/                            # Docker deployment
│   ├── Dockerfile.api
│   ├── Dockerfile.worker
│   ├── docker-compose.yaml
│   └── docker-compose.prod.yaml
│
├── docs/                              # Documentation
│   ├── api.md
│   ├── deployment.md
│   └── integrations.md
│
├── pyproject.toml                     # Python project config
├── poetry.lock                        # Dependency lockfile
└── README.md
```

## 2. Data Models & Schema

### Task Model (PostgreSQL)

```sql
CREATE TABLE tasks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     VARCHAR(255),                         -- source system ID
    task_type       VARCHAR(64)  NOT NULL,                 -- detection|seg|tracking|etc
    modality        VARCHAR(64)  NOT NULL,                 -- image|video|lidar|text|audio
    status          VARCHAR(32)  NOT NULL DEFAULT 'pending',  -- pending|prelabel|annotating|review|adjudicate|completed|rejected
    priority        SMALLINT     NOT NULL DEFAULT 2,       -- 0=critical … 3=low
    skill_level     SMALLINT     NOT NULL DEFAULT 1,       -- 1=novice … 5=expert
    schema_version  VARCHAR(16)  NOT NULL,                 -- e.g. "2.1.0"
    data_manifest   JSONB        NOT NULL,                 -- file paths + hashes
    prelabel_data   JSONB,                                 -- model pre-label output
    metrics         JSONB,                                 -- confidence, entropy, etc
    assigned_to     UUID REFERENCES annotators(id),
    batch_id        UUID REFERENCES batches(id),
    source          VARCHAR(255),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    deadline        TIMESTAMPTZ,
    sla_breached    BOOLEAN      DEFAULT FALSE
);

CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_tasks_type_priority ON tasks(task_type, priority);
CREATE INDEX idx_tasks_assigned ON tasks(assigned_to) WHERE assigned_to IS NOT NULL;
```

### Annotation Model (PostgreSQL)

```sql
CREATE TABLE annotations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id         UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    annotator_id    UUID NOT NULL REFERENCES annotators(id),
    annotation_data JSONB NOT NULL,                         -- full annotation payload
    format          VARCHAR(32) NOT NULL DEFAULT 'json',     -- coco|yolo|voc|jsonl|custom
    labels          JSONB,                                  -- normalized labels array
    bounding_boxes  JSONB,                                  -- normalized bbox array
    segments        JSONB,                                  -- polygon/RLE segments
    confidence      REAL DEFAULT 1.0,
    time_spent_sec  INTEGER,
    flags           JSONB,                                  -- {flagged: bool, reason: str}
    is_honeypot     BOOLEAN DEFAULT FALSE,
    gold_correct    BOOLEAN,                                -- NULL if not gold; T/F
    review_status   VARCHAR(32) DEFAULT 'unreviewed',       -- unreviewed|approved|rework|escalated
    reviewer_id     UUID REFERENCES annotators(id),
    review_decision JSONB,                                  -- {approved, changes_requested, flags}
    adjudicator_id  UUID REFERENCES annotators(id),
    final_decision  VARCHAR(32),                            -- accepted|rejected|modified
    content_hash    VARCHAR(64) NOT NULL,                   -- SHA-256 of annotation_data
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_annotations_task ON annotations(task_id);
CREATE INDEX idx_annotations_annotator ON annotations(annotator_id);
CREATE INDEX idx_annotations_review ON annotations(review_status) WHERE review_status != 'unreviewed';
CREATE INDEX idx_annotations_hash ON annotations(content_hash);
```

### Annotator Model (PostgreSQL)

```sql
CREATE TABLE annotators (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     VARCHAR(255) UNIQUE,
    name            VARCHAR(255) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    role            VARCHAR(32) NOT NULL DEFAULT 'annotator',  -- annotator|reviewer|senior|admin
    skill_tags      TEXT[] NOT NULL DEFAULT '{}',               -- {"detection","medical","lidar"}
    modality_tags   TEXT[] NOT NULL DEFAULT '{}',               -- {"image","video","lidar"}
    certifications  JSONB DEFAULT '{}',                         -- {task_type: cert_date}
    calibration     JSONB,                                      -- latest calibration scorecard
    metrics         JSONB DEFAULT '{}',                         -- {throughput, accuracy, iaa_avg}
    max_reviews     SMALLINT DEFAULT 50,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_calibrated TIMESTAMPTZ
);

CREATE INDEX idx_annotators_skills ON annotators USING GIN(skill_tags);
CREATE INDEX idx_annotators_role ON annotators(role);
```

### Gold Panel Model (PostgreSQL)

```sql
CREATE TABLE gold_panel (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    annotation_id   UUID NOT NULL UNIQUE REFERENCES annotations(id),
    task_id         UUID NOT NULL REFERENCES tasks(id),
    stratum         VARCHAR(32) NOT NULL,                     -- easy|medium|hard|expert
    difficulty_score REAL NOT NULL,
    class_label     VARCHAR(128),
    modality        VARCHAR(64),
    is_active       BOOLEAN DEFAULT TRUE,
    rotation_round  INTEGER DEFAULT 0,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at      TIMESTAMPTZ,
    created_by      UUID REFERENCES annotators(id),

    -- Adjudicated ground truth
    gold_labels     JSONB NOT NULL,
    gold_bbox       JSONB,
    gold_segments   JSONB,
    adjudication_count INTEGER NOT NULL DEFAULT 3,
    adjudicator_ids UUID[] NOT NULL
);

CREATE INDEX idx_gold_stratum ON gold_panel(stratum) WHERE is_active;
CREATE INDEX idx_gold_class ON gold_panel(class_label) WHERE is_active;
```

### Audit Model (PostgreSQL)

```sql
CREATE TABLE audits (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID NOT NULL REFERENCES batches(id),
    batch_size      INTEGER NOT NULL,
    sample_size     INTEGER NOT NULL,
    sample_strategy VARCHAR(64) NOT NULL,                    -- stratified|random|systematic
    strata          JSONB,                                   -- stratum-level breakdown
    overall_error_rate REAL,
    ci_95_lower     REAL,
    ci_95_upper     REAL,
    acceptance_decision VARCHAR(32) NOT NULL,                -- pass|conditional_pass|fail
    errors_found    INTEGER NOT NULL DEFAULT 0,
    escalated_items UUID[],
    auditor_id      UUID REFERENCES annotators(id),
    audited_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes           TEXT
);
```

### Dataset Version Model (PostgreSQL metadata + Object store data)

```sql
CREATE TABLE dataset_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_name    VARCHAR(128) NOT NULL,
    semver          VARCHAR(16) NOT NULL,                     -- "4.2.1"
    parent_version  VARCHAR(16),
    schema_version  VARCHAR(16) NOT NULL,
    content_hash    VARCHAR(64) NOT NULL UNIQUE,              -- SHA-256 of snapshot
    snapshot_path   VARCHAR(1024) NOT NULL,                   -- object store key
    stats           JSONB NOT NULL,
    qa_summary      JSONB,
    lineage         JSONB,
    tags            TEXT[] DEFAULT '{}',
    created_by      VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(dataset_name, semver)
);

CREATE INDEX idx_versions_name ON dataset_versions(dataset_name);
CREATE INDEX idx_versions_hash ON dataset_versions(content_hash);
```

## 3. Task Routing Algorithm

### Core Algorithm

```python
def route_task(task: Task, available_annotators: list[Annotator]) -> Annotator:
    scores = {}
    for ann in available_annotators:
        if not meets_baseline_requirements(task, ann):
            continue

        skill_score = compute_skill_match(task, ann)
        load_score = compute_load_factor(ann)
        quality_score = compute_quality_score(ann, task.task_type)
        diversity_score = compute_diversity_bonus(ann, task)
        iaa_history_score = compute_iaa_history(ann, task.task_type)

        scores[ann.id] = (
            ALPHA * skill_score +
            BETA * (1 - load_score) +
            GAMMA * quality_score +
            DELTA * diversity_score +
            EPSILON * iaa_history_score
        )

    if not scores:
        return dispatch_to_pool_backup(task)

    return max(scores, key=scores.get)


def compute_skill_match(task, annotator):
    required = set(task.required_skills)
    possessed = set(annotator.skill_tags)
    if not required:
        return 1.0
    overlap = len(required & possessed)
    return overlap / len(required)


def compute_load_factor(annotator):
    pending = count_pending_tasks(annotator.id)
    capacity = annotator.max_reviews
    return min(pending / max(capacity, 1), 1.0)


def compute_quality_score(annotator, task_type):
    cal = annotator.calibration or {}
    return cal.get(f"{task_type}_accuracy", cal.get("global_accuracy", 0.5))


def compute_diversity_bonus(annotator, task):
    recent = get_recent_tasks(annotator.id, limit=20)
    same_batch_count = sum(1 for t in recent if t.batch_id == task.batch_id)
    return max(0.0, 1.0 - same_batch_count / 20)


def compute_iaa_history(annotator, task_type):
    rolling = get_rolling_iaa(annotator.id, task_type, window_days=7)
    return rolling if rolling is not None else 0.5
```

### Weight Configuration

| Parameter | Symbol | Default | Description |
|-----------|--------|---------|-------------|
| Skill match | α | 0.40 | Task-type / modality alignment |
| Load balance | β | 0.25 | Available capacity weighting |
| Quality score | γ | 0.20 | Calibrated accuracy |
| Diversity bonus | δ | 0.10 | Avoid batch-concentration |
| IAA history | ε | 0.05 | Rolling agreement consistency |

### Special Cases

- **Honeypot tasks**: routed to lowest-recently-calibrated annotator
- **Edge-case tasks** (entropy > threshold): routed to most-experienced + highest-IAA
- **Review tasks**: never assigned to original annotator; prefer different skill profile
- **Senior adjudication**: requires level-5 (expert) role

## 4. Annotation Format Support

### Format Adapter Interface

```python
class FormatAdapter(ABC):
    @abstractmethod
    def load(self, path: str) -> AnnotationPayload:
        ...

    @abstractmethod
    def dump(self, annotations: AnnotationPayload, path: str):
        ...

    @abstractmethod
    def validate(self, annotations: AnnotationPayload) -> list[ValidationError]:
        ...

    @abstractmethod
    def normalize(self, annotations: AnnotationPayload) -> NormalizedAnnotation:
        ...

    @abstractmethod
    def denormalize(self, normalized: NormalizedAnnotation) -> AnnotationPayload:
        ...
```

### Supported Formats

```python
class FormatRegistry:
    FORMATS = {}

    def register(self, name: str, adapter: type[FormatAdapter]):
        self.FORMATS[name] = adapter
        return adapter

    def get(self, name: str) -> FormatAdapter:
        adapter = self.FORMATS.get(name.lower())
        if not adapter:
            raise FormatNotSupportedError(f"Unknown format: {name}")
        return adapter()


# Registration
registry = FormatRegistry()

@registry.register("coco")
class COCOAdapter(FormatAdapter): ...

@registry.register("yolo")
class YOLOAdapter(FormatAdapter): ...

@registry.register("pascal_voc")
class PascalVOCAdapter(FormatAdapter): ...

@registry.register("jsonl")
class JSONLAdapter(FormatAdapter): ...

@registry.register("parquet")
class ParquetAdapter(FormatAdapter): ...

@registry.register("cvat_xml")
class CVATXMLAdapter(FormatAdapter): ...

@registry.register("label_studio")
class LabelStudioAdapter(FormatAdapter): ...
```

### Normalized Intermediate Representation

```python
@dataclass
class NormalizedAnnotation:
    task_id: str
    annotator_id: str
    labels: list[Label]
    metadata: dict

@dataclass
class Label:
    class_id: int
    class_name: str
    confidence: float
    bbox: BBox | None = None
    segmentation: Polygon | RLE | None = None
    keypoints: list[Keypoint] | None = None
    attributes: dict | None = None
    track_id: int | None = None

@dataclass
class BBox:
    x: float      # normalized [0, 1] or absolute
    y: float
    width: float
    height: float
    absolute: bool = False

@dataclass
class Polygon:
    points: list[tuple[float, float]]

@dataclass
class RLE:
    counts: list[int]
    size: tuple[int, int]
```

## 5. IAA Computation Module

### Core Implementation

```python
import numpy as np
from scipy import stats as scipy_stats
from typing import Optional


class IAAEngine:
    def compute_all(
        self,
        annotations: np.ndarray,     # shape (n_items, n_annotators) with NaN for missing
        scale: str = "nominal",
        n_bootstrap: int = 5000,
        seed: int = 7777,
    ) -> dict:
        return {
            "cohen_kappa": self.cohen_kappa(annotations),
            "fleiss_kappa": self.fleiss_kappa(annotations),
            "krippendorff_alpha": self.krippendorff_alpha(annotations, scale),
            "gwet_ac1": self.gwet_ac1(annotations),
            "observed_agreement": self.percent_agreement(annotations),
            "bootstrap_ci": self.bootstrap_ci(
                annotations, scale=scale, n_iterations=n_bootstrap, seed=seed
            ),
            "per_class": self.per_class_iaa(annotations),
        }

    def cohen_kappa(self, annotations: np.ndarray) -> float:
        if annotations.shape[1] < 2:
            return np.nan
        a, b = annotations[:, 0], annotations[:, 1]
        valid = ~(np.isnan(a) | np.isnan(b))
        a, b = a[valid], b[valid]
        if len(a) < 2:
            return np.nan
        n = len(a)
        categories = sorted(set(a[a != -1]) | set(b[b != -1]))
        k = len(categories)
        cm = np.zeros((k, k), dtype=float)
        for i in range(n):
            row_idx = categories.index(a[i]) if a[i] in categories else -1
            col_idx = categories.index(b[i]) if b[i] in categories else -1
            if row_idx >= 0 and col_idx >= 0:
                cm[row_idx, col_idx] += 1
        cm /= n
        po = np.trace(cm)
        pa = cm.sum(axis=1) @ cm.sum(axis=0)
        if pa == 1.0:
            return 1.0
        return (po - pa) / (1 - pa)

    def fleiss_kappa(self, annotations: np.ndarray) -> float:
        n_items, n_annotators = annotations.shape
        categories = np.unique(annotations[~np.isnan(annotations)].astype(int))
        if len(categories) <= 1:
            return np.nan
        k = len(categories)
        cat_map = {c: i for i, c in enumerate(sorted(categories))}
        table = np.zeros((n_items, k), dtype=float)
        for i in range(n_items):
            row = annotations[i, :]
            valid = ~np.isnan(row)
            for label in row[valid]:
                table[i, cat_map[int(label)]] += 1
        n_valid = table.sum(axis=1)
        mask = n_valid >= 2
        table = table[mask]
        n_valid = n_valid[mask]
        n_items = table.shape[0]
        if n_items == 0:
            return np.nan
        pi_i = ((table ** 2).sum(axis=1) - n_valid) / (n_valid * (n_valid - 1))
        p_bar = pi_i.mean()
        p_j = table.sum(axis=0) / (n_valid.sum())
        p_e = (p_j ** 2).sum()
        if p_e >= 1.0:
            return 1.0
        return (p_bar - p_e) / (1 - p_e)

    def krippendorff_alpha(
        self, annotations: np.ndarray, scale: str = "nominal"
    ) -> float:
        n_items, n_annotators = annotations.shape
        values = np.unique(annotations[~np.isnan(annotations)])
        if len(values) <= 1:
            return np.nan
        value_map = {v: i for i, v in enumerate(sorted(values))}
        m = len(values)
        coinc = np.zeros((m, m), dtype=float)
        for i in range(n_items):
            row = annotations[i, :]
            valid = ~np.isnan(row)
            obs = row[valid]
            n_obs = len(obs)
            if n_obs < 2:
                continue
            for j in range(n_obs):
                for k_val in range(j + 1, n_obs):
                    c = value_map[int(obs[j])]
                    k = value_map[int(obs[k_val])]
                    coinc[c, k] += 1.0
                    coinc[k, c] += 1.0

        n_pairs = coinc.sum() / 2
        if n_pairs == 0:
            return np.nan
        do = 0.0
        de = 0.0
        delta_fn = self._get_delta(scale, values)
        for c in range(m):
            for k in range(m):
                d = delta_fn(values[c], values[k]) ** 2
                do += coinc[c, k] * d
        col_sums = coinc.sum(axis=0)
        total = col_sums.sum()
        for c in range(m):
            for k in range(m):
                d = delta_fn(values[c], values[k]) ** 2
                de += col_sums[c] * col_sums[k] * d
        if total > 0:
            de /= total
        if de == 0:
            return 1.0
        return 1 - do / de

    def _get_delta(self, scale: str, values: np.ndarray):
        if scale == "nominal":
            return lambda c, k: 0.0 if c == k else 1.0
        elif scale == "ordinal":
            sorted_vals = sorted(values)
            ranks = {v: i for i, v in enumerate(sorted_vals)}
            n = len(sorted_vals)
            return lambda c, k: (
                sum(1 for v in sorted_vals[min(ranks[c], ranks[k]):max(ranks[c], ranks[k])+1])
                - (n + 1) / 2
            ) if c != k else 0.0
        elif scale == "interval":
            return lambda c, k: (float(c) - float(k)) ** 2
        elif scale == "ratio":
            return lambda c, k: ((float(c) - float(k)) / (float(c) + float(k))) ** 2
        raise ValueError(f"Unknown scale: {scale}")

    def gwet_ac1(self, annotations: np.ndarray) -> float:
        n_items, n_annotators = annotations.shape
        categories = np.unique(annotations[~np.isnan(annotations)].astype(int))
        if len(categories) <= 1:
            return np.nan
        k = len(categories)
        cat_map = {c: i for i, c in enumerate(sorted(categories))}
        table = np.zeros((n_items, k), dtype=float)
        for i in range(n_items):
            row = annotations[i, :]
            valid = ~np.isnan(row)
            for label in row[valid]:
                table[i, cat_map[int(label)]] += 1
        n_valid = table.sum(axis=1)
        mask = n_valid >= 2
        table = table[mask]
        n_valid = n_valid[mask]
        n_items = table.shape[0]
        if n_items == 0:
            return np.nan
        pa = (table * (table - 1)).sum(axis=1).sum() / (n_valid * (n_valid - 1)).sum()
        p_k = table.sum(axis=0) / n_valid.sum()
        pe = sum(p * (1 - p) for p in p_k) / (k - 1)
        if pe >= 1.0:
            return 1.0
        return (pa - pe) / (1 - pe)

    def percent_agreement(self, annotations: np.ndarray) -> float:
        n_items, n_annotators = annotations.shape
        total_pairs = 0
        agree_pairs = 0
        for i in range(n_items):
            row = annotations[i, :]
            valid = ~np.isnan(row)
            obs = row[valid]
            for j in range(len(obs)):
                for k in range(j + 1, len(obs)):
                    total_pairs += 1
                    if obs[j] == obs[k]:
                        agree_pairs += 1
        if total_pairs == 0:
            return np.nan
        return agree_pairs / total_pairs

    def bootstrap_ci(
        self,
        annotations: np.ndarray,
        scale: str = "nominal",
        n_iterations: int = 5000,
        seed: int = 7777,
    ) -> dict:
        rng = np.random.default_rng(seed)
        results = {"cohen_kappa": [], "fleiss_kappa": [], "krippendorff_alpha": []}
        n = annotations.shape[0]
        for _ in range(n_iterations):
            idx = rng.integers(0, n, size=n)
            sample = annotations[idx]
            results["cohen_kappa"].append(self.cohen_kappa(sample))
            results["fleiss_kappa"].append(self.fleiss_kappa(sample))
            results["krippendorff_alpha"].append(
                self.krippendorff_alpha(sample, scale)
            )
        ci = {}
        for metric, vals in results.items():
            arr = np.array(vals)
            arr = arr[~np.isnan(arr)]
            ci[metric] = {
                "ci_95_lower": float(np.percentile(arr, 2.5)),
                "ci_95_upper": float(np.percentile(arr, 97.5)),
                "ci_90_lower": float(np.percentile(arr, 5.0)),
                "ci_90_upper": float(np.percentile(arr, 95.0)),
                "std_err": float(np.std(arr, ddof=1)),
                "mean": float(np.mean(arr)),
            }
        return ci

    def per_class_iaa(self, annotations: np.ndarray) -> dict:
        categories = sorted(set(
            int(v) for v in np.unique(annotations[~np.isnan(annotations)])
        ))
        per_class = {}
        for cat in categories:
            binary = np.where(annotations == cat, 1, 0).astype(float)
            binary[np.isnan(annotations)] = np.nan
            per_class[f"class_{cat}"] = {
                "cohen_kappa": self.cohen_kappa(binary),
                "fleiss_kappa": self.fleiss_kappa(binary),
                "krippendorff_alpha": self.krippendorff_alpha(binary),
            }
        return per_class
```

## 6. Quality Metrics Pipeline

### Metric Aggregator

```python
class QualityMetricsPipeline:
    def __init__(self, db: Database, iaa_engine: IAAEngine):
        self.db = db
        self.iaa = iaa_engine

    def compute_batch_metrics(self, batch_id: str) -> BatchQualityReport:
        batch = self.db.get_batch(batch_id)
        annotations = self.db.get_annotations(batch_id)
        gold_panel = self.db.get_gold_for_batch(batch_id)
        audit = self.db.get_audit(batch_id)

        iaa_matrix = self._build_iaa_matrix(annotations)
        iaa_report = self.iaa.compute_all(iaa_matrix)

        gold_accuracy = self._compute_gold_accuracy(annotations, gold_panel)
        audit_pass_rate = self._compute_audit_pass_rate(audit)
        throughput = self._compute_throughput(batch, annotations)
        annotator_metrics = self._per_annotator_metrics(annotations, gold_panel)
        error_distribution = self._error_type_distribution(annotations, audit)
        throughput_accuracy_corr = self._compute_throughput_accuracy_correlation(
            batch, annotator_metrics
        )

        return BatchQualityReport(
            batch_id=batch_id,
            iaa=iaa_report,
            gold_accuracy=gold_accuracy,
            gold_accuracy_ci=self._bootstrap_gold_ci(gold_panel),
            audit_pass_rate=audit_pass_rate,
            throughput=throughput,
            annotator_metrics=annotator_metrics,
            error_distribution=error_distribution,
            throughput_accuracy_correlation=throughput_accuracy_corr,
            disagreement_clusters=self._cluster_disagreements(iaa_matrix),
        )

    def _compute_gold_accuracy(self, annotations, gold_panel) -> dict:
        results = {"overall": 0.0, "by_stratum": {}, "by_class": {}, "by_annotator": {}}
        correct_total = 0
        total = 0
        for ann in annotations:
            if not ann.is_honeypot:
                continue
            gp = gold_panel.get(ann.task_id)
            if not gp:
                continue
            is_correct = self._compare_labels(ann.annotation_data, gp.gold_labels)
            if is_correct:
                correct_total += 1
            total += 1
            stratum = gp.stratum
            results["by_stratum"].setdefault(stratum, {"correct": 0, "total": 0})
            results["by_stratum"][stratum]["correct"] += int(is_correct)
            results["by_stratum"][stratum]["total"] += 1
            cls = gp.class_label
            results["by_class"].setdefault(cls, {"correct": 0, "total": 0})
            results["by_class"][cls]["correct"] += int(is_correct)
            results["by_class"][cls]["total"] += 1
            aid = ann.annotator_id
            results["by_annotator"].setdefault(aid, {"correct": 0, "total": 0})
            results["by_annotator"][aid]["correct"] += int(is_correct)
            results["by_annotator"][aid]["total"] += 1

        results["overall"] = correct_total / max(total, 1)
        for stratum, data in results["by_stratum"].items():
            data["accuracy"] = data["correct"] / max(data["total"], 1)
        for cls, data in results["by_class"].items():
            data["accuracy"] = data["correct"] / max(data["total"], 1)
        for aid, data in results["by_annotator"].items():
            data["accuracy"] = data["correct"] / max(data["total"], 1)
        return results

    def _compute_throughput_accuracy_correlation(self, batch, annotator_metrics) -> float:
        speeds = []
        accuracies = []
        for aid, metrics in annotator_metrics.items():
            speed = self.db.get_annotator_speed(aid, batch.time_range)
            if speed and metrics.get("accuracy") is not None:
                speeds.append(speed)
                accuracies.append(metrics["accuracy"])
        if len(speeds) < 5:
            return 0.0
        corr = np.corrcoef(speeds, accuracies)[0, 1]
        return float(corr) if not np.isnan(corr) else 0.0

    def _error_type_distribution(self, annotations, audit) -> dict:
        errors = audit.errors_found if audit else []
        distribution = {
            "label_wrong": 0,
            "bbox_tightness": 0,
            "missed_object": 0,
            "false_positive": 0,
            "boundary_error": 0,
            "attribute_error": 0,
        }
        for e in errors:
            t = e.get("error_type", "unknown")
            if t in distribution:
                distribution[t] += 1
        return distribution
```

## 7. Review Queue and Escalation Logic

### State Machine (annotate → peer → senior → final)

```python
class ReviewWorkflow:
    TRANSITIONS = {
        "annotated":        ["peer_review"],
        "peer_review":      ["approved", "rework", "escalated"],
        "rework":           ["peer_review"],
        "escalated":        ["senior_adjudication"],
        "senior_adjudication": ["final_accepted", "final_rejected", "final_modified"],
        "final_accepted":   ["dataset_ready"],
        "final_rejected":   ["archived"],
        "final_modified":   ["dataset_ready"],
    }

    def process(self, annotation, decision, reviewer_id):
        current = annotation.review_status
        if decision not in self._valid_transitions(current):
            raise InvalidTransition(current, decision)

        if current == "annotated" and decision == "peer_review":
            self._assign_peer_reviewer(annotation)

        elif current == "peer_review":
            if decision == "approved":
                annotation.reviewer_id = reviewer_id
                annotation.review_decision = decision
                self._check_iaa_gate(annotation)
            elif decision == "rework":
                annotation.review_status = "rework"
                annotation.reviewer_id = reviewer_id
                self._notify_annotator(annotation, "rework_requested")
            elif decision == "escalated":
                annotation.review_status = "escalated"
                self._assign_senior_adjudicator(annotation)

        elif current == "rework":
            if decision == "peer_review":
                annotation.review_status = "peer_review"

        elif current == "escalated" and decision == "senior_adjudication":
            annotation.adjudicator_id = reviewer_id

        elif current == "senior_adjudication":
            if decision == "final_accepted":
                annotation.final_decision = "accepted"
                annotation.review_status = "final_accepted"
                self._push_to_dataset_store(annotation)
            elif decision == "final_rejected":
                annotation.final_decision = "rejected"
                annotation.review_status = "final_rejected"
            elif decision == "final_modified":
                annotation.final_decision = "modified"
                annotation.review_status = "final_modified"
                self._push_to_dataset_store(annotation)

        self.db.save(annotation)
        return annotation
```

### Escalation Rule Engine

```python
class EscalationEngine:
    def __init__(self, db):
        self.db = db

    def should_escalate(self, annotation, reviewer_decision) -> EscalationReason:
        reasons = []

        # Rule 1: Reviewer flags as uncertain
        if reviewer_decision in ("uncertain", "needs_expert"):
            reasons.append(EscalationReason("reviewer_flagged"))

        # Rule 2: IAA below threshold
        rolling_iaa = self.db.get_rolling_iaa(
            annotation.task_id, window=7
        )
        if rolling_iaa and rolling_iaa < 0.667:
            reasons.append(EscalationReason(f"iaa_below_threshold", rolling_iaa))

        # Rule 3: Task type requires mandatory senior sign-off
        task = self.db.get_task(annotation.task_id)
        if task.task_type in MANDATORY_SENIOR_TYPES:
            reasons.append(EscalationReason("mandatory_senior_type"))

        # Rule 4: Annotator accuracy trend decline
        ann_accuracy = self.db.get_annotator_rolling_accuracy(
            annotation.annotator_id, annotation.task_type, window=30
        )
        if ann_accuracy and ann_accuracy["trend"] < -0.02:
            reasons.append(EscalationReason("accuracy_decline", ann_accuracy["trend"]))

        # Rule 5: Content-based edge case detection
        if self._is_edge_case(annotation):
            reasons.append(EscalationReason("edge_case_detected"))

        # Rule 6: Same peer reviewer rejects same annotator 3+ times
        if self._has_repeated_rejection_cycle(annotation):
            reasons.append(EscalationReason("repeated_rejection_cycle"))

        return reasons
```

## 8. API Endpoint Design

### Base URL: `/api/v1`

### Tasks

| Method | Endpoint | Request | Response | Description |
|--------|----------|---------|----------|-------------|
| POST | `/tasks` | `{source, task_type, modality, priority, schema_version, files}` | `{task_id, status, queue_position}` | Create new task |
| GET | `/tasks` | `?status=pending&task_type=detection&priority=P0&limit=50` | `[{task_id, ...}]` | Query available tasks |
| GET | `/tasks/{id}` | — | `{task_id, status, assignment, timeline}` | Task detail |
| PATCH | `/tasks/{id}/assign` | `{annotator_id}` | `{task_id, status}` | Assign task |
| PATCH | `/tasks/{id}/status` | `{status}` | `{task_id, updated_status}` | Update task status |

### Annotations

| Method | Endpoint | Request | Response | Description |
|--------|----------|---------|----------|-------------|
| POST | `/annotations` | `{task_id, annotation_data, format, time_spent_sec}` | `{annotation_id, status}` | Submit annotation |
| GET | `/annotations/{id}` | — | `{annotation_id, task_id, annotation_data, flags}` | Get annotation |
| GET | `/annotations` | `?task_id=&annotator_id=&status=` | `[{annotation_id, ...}]` | Query annotations |
| PATCH | `/annotations/{id}/review` | `{decision, reviewer_id, notes}` | `{annotation_id, review_status}` | Review decision |
| PATCH | `/annotations/{id}/adjudicate` | `{decision, adjudicator_id, modified_data}` | `{annotation_id, final_decision}` | Senior adjudication |

### Review

| Method | Endpoint | Request | Response | Description |
|--------|----------|---------|----------|-------------|
| GET | `/review/pending` | `?reviewer_id=&task_type=&limit=` | `[{annotation_id, task_id, ...}]` | Get pending review queue |
| POST | `/review/assign` | `{task_type, reviewer_skills}` | `{annotation_id, reviewer_id}` | Auto-assign reviewer |
| GET | `/review/escalations` | `?status=` | `[{annotation_id, reasons, ...}]` | List escalated items |
| GET | `/review/{annotation_id}/history` | — | `[{stage, reviewer, decision, timestamp}]` | Review provenance |

### Audit

| Method | Endpoint | Request | Response | Description |
|--------|----------|---------|----------|-------------|
| POST | `/audit/batch` | `{batch_id, sample_size, strategy}` | `{audit_id, status}` | Trigger batch audit |
| GET | `/audit/{id}` | — | `{audit_id, sample, error_rate, decision}` | Audit result |
| GET | `/audit/batch/{batch_id}` | — | `[{audit_id, error_rate, decision}]` | Batch audit history |
| POST | `/audit/acceptance-sample` | `{batch_id, confidence, aql, rql}` | `{plan, n1, n2, decision_rule}` | Compute acceptance plan |

### Datasets

| Method | Endpoint | Request | Response | Description |
|--------|----------|---------|----------|-------------|
| POST | `/datasets/version` | `{dataset_name, parent_version, batch_ids}` | `{version_id, semver, snapshot_path}` | Create dataset version |
| GET | `/datasets` | — | `[{dataset_name, latest_version, stats}]` | List datasets |
| GET | `/datasets/{name}/versions` | — | `[{semver, created_at, stats, qa_summary}]` | List versions |
| GET | `/datasets/{name}/versions/{semver}` | — | `{manifest, provenance}` | Get version detail |
| GET | `/datasets/{name}/versions/{semver}/diff/{other}` | — | `{added, removed, modified, stats}` | Diff two versions |
| GET | `/datasets/{name}/versions/{semver}/export` | `?format=coco&split=train` | `binary|json` | Export dataset |
| GET | `/datasets/{name}/provenance` | `?annotation_id=` | `{lineage_dag}` | Trace annotation lineage |

### Metrics

| Method | Endpoint | Request | Response | Description |
|--------|----------|---------|----------|-------------|
| GET | `/metrics/iaa` | `?task_type=&window=7d&batch_id=` | `{cohen, fleiss, krippendorff, ci}` | IAA metrics |
| GET | `/metrics/gold-accuracy` | `?stratum=&window=30d` | `{overall, by_stratum, trend}` | Gold panel accuracy |
| GET | `/metrics/audit-pass-rate` | `?task_type=&window=` | `{pass_rate, samples, ci}` | Audit pass rate |
| GET | `/metrics/throughput` | `?task_type=&granularity=day` | `{dates, counts, latencies}` | Throughput time series |
| GET | `/metrics/overview` | `?window=7d` | `{iaa, accuracy, throughput, sla, alerts}` | Dashboard summary |
| GET | `/metrics/annotator/{id}` | — | `{id, accuracy, speed, iaa, trends}` | Per-annotator metrics |

## 9. Integration Patterns

### Label Studio Adapter

```python
class LabelStudioAdapter:
    def __init__(self, api_url: str, api_key: str):
        self.client = LabelStudio(api_url, api_key)

    def push_task(self, task: Task) -> str:
        ls_task = {
            "data": {
                "image": task.data_manifest["image_url"],
                "pre_label": task.prelabel_data,
            },
            "predictions": [
                {
                    "model_version": "platform-v1",
                    "result": self._convert_to_ls_format(task.prelabel_data),
                    "score": task.metrics.get("confidence", 0.0),
                }
            ],
        }
        result = self.client.tasks.create(ls_task)
        return result.id

    def pull_annotation(self, ls_task_id: str) -> AnnotationPayload:
        ls_ann = self.client.annotations.list(task=ls_task_id)
        return self._convert_from_ls_format(ls_ann)

    def sync_guidelines(self, schema_version: str):
        guidelines = self._load_guidelines(schema_version)
        self.client.projects.update(
            project_id=self.project_id,
            **{"label_config": guidelines["xml_template"]}
        )

    def _convert_from_ls_format(self, ls_annotation) -> NormalizedAnnotation:
        results = ls_annotation.result
        labels = []
        for r in results:
            if r.type == "rectanglelabels":
                labels.append(Label(
                    class_id=r.value["rectanglelabels"][0]["id"],
                    class_name=r.value["rectanglelabels"][0]["label"],
                    bbox=BBox(
                        x=r.value["x"] / 100,
                        y=r.value["y"] / 100,
                        width=r.value["width"] / 100,
                        height=r.value["height"] / 100,
                    ),
                    confidence=r.get("score", 1.0),
                ))
            elif r.type == "polygon":
                points = [(p[0], p[1]) for p in r.value["points"]]
                labels.append(Label(
                    class_id=r.value["polygonlabels"][0]["id"],
                    class_name=r.value["polygonlabels"][0]["label"],
                    segmentation=Polygon(points=points),
                    confidence=r.get("score", 1.0),
                ))
        return NormalizedAnnotation(
            task_id=ls_annotation.task,
            annotator_id=ls_annotation.created_by,
            labels=labels,
            metadata={"integration": "label_studio", "ls_id": ls_annotation.id},
        )
```

### CVAT Adapter

```python
class CVATAdapter:
    def __init__(self, api_url: str, credentials: tuple):
        self.api_url = api_url
        self.auth = credentials

    def push_task(self, task: Task):
        payload = {
            "name": f"task-{task.id}",
            "project_id": self._get_project_id(task.task_type),
            "labels": self._get_labels_schema(task.schema_version),
            "data": {
                "image_quality": 70,
                "chunk_size": 20,
                "storage_method": "share",
            },
        }
        # Upload files
        files = [self._upload_file(f) for f in task.data_manifest["files"]]
        resp = self._post("/api/tasks", json=payload | {"files": files})
        return resp.json()["id"]

    def pull_annotation(self, cvat_task_id: str, format: str = "coco"):
        self._request_export(cvat_task_id, format)
        data = self._download_export(cvat_task_id, format)
        return self._convert_from_cvat_format(data, format)

    def update_review(self, cvat_job_id: str, status: str, message: str = ""):
        self._patch(f"/api/jobs/{cvat_job_id}", json={
            "status": status,
            "review_message": message,
        })
```

## 10. Storage Strategy

### Object Store Layout

```
s3://annotation-platform/
├── raw/                                    # Immutable raw data
│   └── {sha256[:3]}/
│       └── {sha256}.{ext}                  # Content-addressed
│
├── labels/                                 # Versioned annotation groups
│   └── {schema_version}/
│       └── {image_sha256}/
│           └── {label_group_hash}.parquet
│
├── snapshots/                              # Immutable dataset versions
│   └── {dataset_name}/
│       └── {semver}.snapshot.json
│       └── {semver}.index.parquet          # Materialized index
│
├── exports/                                # On-demand format exports
│   └── {dataset_name}/
│       └── {semver}_{format}_{split}.{ext}
│
├── models/                                 # Pre-labeling model registry
│   └── {model_name}/
│       └── {version}/
│           ├── model.pkl
│           ├── config.json
│           └── metrics.json
│
├── audit/                                  # Audit evidence
│   └── {audit_id}/
│       ├── sample_manifest.json
│       ├── results.json
│       └── evidence.zip
│
└── temp/                                   # Transient upload staging
    └── {session_id}/
```

### Database Table Layout (PostgreSQL)

| Schema | Table | Purpose | Estimated Growth |
|--------|-------|---------|-----------------|
| public | tasks | Task lifecycle | 50K/day |
| public | annotations | Human annotations | 150K/day |
| public | annotators | User profiles | 100 total |
| public | batches | Logical batch grouping | 200/day |
| public | gold_panel | Gold standard items | 2K active |
| public | gold_results | Gold hits per annotator | 20K/day |
| public | audits | Batch audit records | 200/day |
| public | iaa_reports | IAA computation history | 200/day |
| public | dataset_versions | Immutable snapshots | 10/day |
| public | review_decisions | Review audit trail | 100K/day |
| public | escalation_reasons | Escalation evidence | 2K/day |
| public | guidelines_versions | Versioned guidelines | 5/month |
| public | spec_revisions | Spec change log | 2/month |

### Caching Strategy (Redis)

```
redis://annotation-platform/
├── queue:{type}:{skill}:{priority}         # Task queues (sorted sets)
├── task:{task_id}                          # Task cache (hash, TTL 1h)
├── annotator:{id}:metrics                  # Annotator metric cache (hash, TTL 5m)
├── gold:{stratum}                          # Gold panel cache (set, TTL 30m)
├── iaa:{batch_id}                          # IAA result cache (hash, TTL 1h)
├── lock:task:{task_id}                     # Distributed lock (TTL 30s)
├── lock:annotator:{id}                     # Assignment lock (TTL 30s)
├── publish/                                # Pub/sub channels for real-time
│   ├── task:completed
│   ├── annotation:submitted
│   └── annotation:flagged
├── rate_limit:{annotator_id}              # Session rate limiter
└── session:{annotator_id}:{session_id}    # Active session tracking
```

### Storage Format Selection Matrix

| Data Type | Hot Path | Cold Path | Archive |
|-----------|----------|-----------|---------|
| Annotations | PostgreSQL (JSONB) | Parquet on S3 | Gzip'd Parquet + Glacier |
| Gold panel | PostgreSQL + Redis | Parquet on S3 | Glacier Deep Archive |
| Audit records | PostgreSQL | PostgreSQL with partitioning | S3 export + drop |
| Dataset versions | S3 (Parquet index) | S3 (Parquet compressed) | Glacier |
| Raw data | S3 (original) | S3 (original) | Glacier |
| IAA reports | PostgreSQL | PostgreSQL + S3 export | S3 export |
| Metrics | Prometheus (30d) | Thanos (1y) | S3 cold bucket |
