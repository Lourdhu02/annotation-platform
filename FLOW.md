# Multi-Modal Annotation Quality & Pipeline Platform — Flows

## 1. End-to-End Annotation Lifecycle

```mermaid
flowchart TB
    subgraph STAGE0["STAGE 0: Ingestion & Validation"]
        A1[Raw Data Source] --> A2{Schema Validation}
        A2 -->|Pass| A3{Integrity Check}
        A2 -->|Fail| A4[Dead-Letter Queue]
        A3 -->|Pass| A5[De-identification]
        A3 -->|Corrupt| A4
        A5 --> A6[Generate Ingest Manifest]
        A6 --> A7[Store Raw in Object Store]
        A7 --> A8[Router: Classify by Task Type]
    end

    subgraph STAGE1["STAGE 1: Task Creation & Queueing"]
        B1[Task Creator Service] --> B2[Split into Work Units]
        B2 --> B3[Attach Schema Version]
        B3 --> B4[Compute Priority Score]
        B4 --> B5[Push to Sharded Queue]
        B5 --> B6{Triple-Shard Router}
        B6 -->|Type: detection| B7[Queue: detection]
        B6 -->|Type: segmentation| B8[Queue: segmentation]
        B6 -->|Type: tracking| B9[Queue: tracking]
        B7 --> B10[Skill Tier Sub-Queue]
        B8 --> B10
        B9 --> B10
        B10 --> B11[Priority Sub-Queue: P0/P1/P2/P3]
    end

    subgraph STAGE2["STAGE 2: Model-Assisted Pre-Labeling"]
        C1[Dequeue Task] --> C2{Active Learning Gate}
        C2 -->|High Entropy| C3[Route to Human-First]
        C2 -->|Medium Entropy| C4[Model Pre-Label]
        C2 -->|Low Entropy| C5[Model Auto-Label]
        C4 --> C6[Ensemble Inference: YOLO + SAM + DINO]
        C6 --> C7[Compute Confidence]
        C7 --> C8{Confidence Threshold}
        C8 -->|> 0.95| C9[Accept Pre-Label]
        C8 -->|0.80 - 0.95| C10[Flag for Human Review]
        C8 -->|< 0.80| C11[Route to Edge-Case Queue]
        C9 --> C12[Insert 10% Honeypot]
        C12 --> C13[Human Annotation Queue]
        C10 --> C13
        C11 --> C13
        C3 --> C13
    end

    subgraph STAGE3["STAGE 3: Human Annotation"]
        D1[Annotator Pulls Task] --> D2[Load Embedded Guidelines v2.1.0]
        D2 --> D3[Display Pre-Label Overlay]
        D3 --> D4{Real-Time Validation}
        D4 -->|Invalid| D5[Reject Submission]
        D5 --> D6[Show Error Reason]
        D6 --> D3
        D4 -->|Valid| D7[Submit Annotation]
        D7 --> D8{T0 QA Trigger}
        D8 -->|Pass| D9[Store Annotation]
        D8 -->|Flag| D10[Flagged: Confidence Drop Detected]
        D10 --> D11[Auto-Route to Annotator Re-check]
        D11 --> D3
        D9 --> D12[Update Annotator Metrics]
        D12 -->|Gold Hit?| D13{Gold Panel Check}
        D13 -->|Correct| D14[Increment Accuracy Score]
        D13 -->|Incorrect| D15{Consecutive Failures >= 3?}
        D15 -->|Yes| D16[Pause Session - Show Refresher]
        D15 -->|No| D17[Flag for Monitoring]
    end

    subgraph STAGE4["STAGE 4: Multi-Pass Review"]
        E1[Annotation Submitted] --> E2[Peer Review Assignment]
        E2 --> E3{Peer Reviewer: Annotator II}
        E3 -->|Approve| E4[T1 QA Trigger: IAA Check]
        E3 -->|Request Changes| E5[Return to Annotator]
        E5 --> E6[Annotator Revises]
        E6 --> E3
        E3 -->|Escalate| E7[Senior Adjudication Queue]
        E4 --> E8{IAA >= 0.80?}
        E8 -->|Yes| E9[Pass to Gold Audit]
        E8 -->|No| E10{IAA 0.667 - 0.80?}
        E10 -->|Yes| E11[Conditional Pass - Flag Review]
        E10 -->|No| E12[Full Batch Rework]
        E12 --> E13[Trigger Spec Review]
        E9 --> E14{Gold Audit}
        E14 -->|Pass| E15[Statistical Audit Queue]
        E14 -->|Fail| E7
        E7 --> E16{Senior Adjudicator}
        E16 -->|Accept| E17[Final Decision: Accepted]
        E16 -->|Reject| E18[Final Decision: Rejected]
        E16 -->|Modify| E19[Final Decision: Modified]
        E16 -->|Rework Path| E5
        E15 --> E20[Pass to Stage 5]
    end

    subgraph STAGE5["STAGE 5: Statistical Audit"]
        F1[Batch Ready] --> F2[Define Strata: easy / medium / hard / expert]
        F2 --> F3[Compute Optimal Sample Sizes]
        F3 --> F4[Stratified Random Sampling]
        F4 --> F5[Senior Reviewer Inspects Sample]
        F5 --> F6{Error Rate < Threshold?}
        F6 -->|Yes, AQL Met| F7[Accept Batch]
        F6 -->|No, RQL Exceeded| F8{Double Sampling?}
        F8 -->|Sample 2| F9[Inspect Second Sample]
        F9 --> F10{Combined Error Rate?}
        F10 -->|Acceptable| F7
        F10 -->|Unacceptable| F11[Reject Batch - Full Rework]
        F11 --> F12[Root-Cause Analysis]
        F12 --> F13[Spec/Annotator Adjustment]
        F13 --> F14[Re-enter at Stage 3]
        F7 --> F15[Generate Audit Report]
        F15 --> F16[Finalize Batch]
    end

    subgraph STAGE6["STAGE 6: Dataset Versioning & Release"]
        G1[Approved Batch] --> G2[Content-Address Labels]
        G2 --> G3[Build Label Group Index]
        G3 --> G4[Compute Dataset Snapshot]
        G4 --> G5[Compute Content Hash]
        G5 --> G6[Increment Semver: v4.2.1]
        G6 --> G7[Write Snapshot Manifest]
        G7 --> G8[Record Lineage DAG]
        G8 --> G9[Push to Object Store]
        G9 --> G10[Update Dataset Registry]
        G10 --> G11[Trigger Downstream]
        G11 --> G12[Export to Training Formats]
        G12 --> G13[Model Training Pipeline]
        G11 --> G14[Update Monitoring Dashboards]
    end

    STAGE0 --> STAGE1
    STAGE1 --> STAGE2
    STAGE2 --> STAGE3
    STAGE3 --> STAGE4
    STAGE4 --> STAGE5
    STAGE5 --> STAGE6
```

## 2. Multi-Pass Review Workflow

```mermaid
flowchart TB
    subgraph INITIAL["Annotator Completes Task"]
        A[Annotator] --> A1[Submits Annotation]
        A1 --> A2[Embedded Validation Passes]
        A2 --> A3[T0 QA: Pre-Review Check]
        A3 -->|Auto Pass| A4[Assign to Peer Queue]
        A3 -->|Auto Flag| A5[Return for Correction]
        A5 --> A
    end

    subgraph PEER["Pass 1: Peer Review"]
        B[Peer Reviewer Assigned] --> B1{Match Criteria}
        B1 -->|Skill Match >= 0.7| B2[Load Annotation]
        B1 -->|Skill Match < 0.7| B3[Reassign]
        B2 --> B4[Review Against Guidelines v2.1.0]
        B4 --> B5{Decision}
        B5 -->|Approve| B6[Pass to T1 QA]
        B5 -->|Minor Fixes| B7[Send Back to Annotator]
        B5 -->|Major Issues| B8[Escalate to Senior]
        B5 -->|Uncertain| B8
        B7 --> B9[Annotator Corrects]
        B9 --> B2
    end

    subgraph QA1["T1 QA Trigger: Post-Annotation"]
        C1[Receive Approval] --> C2{Compute IAA}
        C2 --> C3[Krippendorff's Alpha]
        C3 --> C4{Alpha >= 0.80?}
        C4 -->|Yes| C5[Pass to Gold Audit]
        C4 -->|0.667 - 0.80| C6[Conditional Pass]
        C6 --> C7[Attach Quality Flag]
        C7 --> C5
        C4 -->|< 0.667| C8[Trigger Spec Review]
        C8 --> C9[Flag Entire Batch]
        C9 --> C10[Route to Senior]
    end

    subgraph GOLD["Gold Panel Audit"]
        D1[Annotation + Gold Pair] --> D2{Match?}
        D2 -->|Correct| D3[Increment Gold Score]
        D2 -->|Incorrect| D4[Log Error Type]
        D4 --> D5{Annotator Gold Accuracy Trend?}
        D5 -->|Declining > 5%| D6[Flag Annotator for Calibration]
        D5 -->|Stable| D7[Single Error - Log Only]
        D3 --> D8[Pass to Audit Queue]
        D6 --> D8
    end

    subgraph SENIOR["Pass 3: Senior Adjudication"]
        E1[Escalated Task] --> E2[Senior Adjudicator]
        E2 --> E3[Review Original + Peer + IAA Report]
        E3 --> E4{Decision}
        E4 -->|Accept| E5[Final Label: Accepted]
        E4 -->|Reject| E6[Final Label: Rejected]
        E4 -->|Modify| E7[Final Label: Modified]
        E4 -->|Rework| E8[Return to Annotator]
        E7 --> E9[Save Modified Annotation]
        E9 --> E10[Record Adjudication Rationale]
        E10 --> E5
    end

    subgraph FINAL["T2 QA Trigger: Post-Review"]
        F1[Final Decision] --> F2{Run T2 Validation}
        F2 --> F3[Audit Reviewer Decision]
        F3 --> F4[Detect Reviewer Bias]
        F4 --> F5[Compare vs Gold Panel]
        F5 --> F6{Pass?}
        F6 -->|Yes| F7[Write to Dataset Store]
        F6 -->|No| F8[Elevate to Second Senior]
        F8 --> E1
    end

    A4 --> PEER
    B6 --> QA1
    C5 --> GOLD
    B8 --> SENIOR
    C10 --> SENIOR
    D8 --> FINAL
    E5 --> FINAL
```

## 3. IAA Computation and Disagreement Routing Flow

```mermaid
flowchart TB
    subgraph INPUT["Input Data"]
        A[Multi-Annotator Annotations] --> A1[Build Matrix: items x annotators]
        A1 --> A2[Handle Missing Data: NaN for unrated]
        A2 --> A3{Annotator Count}
    end

    subgraph METRIC["Metric Selection"]
        A3 -->|Exactly 2, complete| B1[Cohen's Kappa]
        A3 -->|3+, complete, nominal| B2[Fleiss' Kappa]
        A3 -->|Any count, missing data| B3[Krippendorff's Alpha]
        A3 -->|2, ordinal| B4[Weighted Cohen's Kappa]
        A3 -->|High prevalence bias| B5[Gwet's AC1]
        B1 --> B6[Confidence Intervals: Bootstrap 5000]
        B2 --> B6
        B3 --> B6
        B4 --> B6
        B5 --> B6
    end

    subgraph AGGREGATE["Aggregation Layer"]
        B6 --> C1[Compute Observed Agreement]
        C1 --> C2[Compute Chance-Corrected Agreement]
        C2 --> C3[Per-Class Breakdown]
        C3 --> C4[Confusion Matrix]
        C4 --> C5[Pairwise Annotator Comparison]
        C5 --> C6[IAA Report with CI]
    end

    subgraph GATES["Acceptance Gates"]
        C6 --> D1{Gate 1: Batch-Level α >= 0.80?}
        D1 -->|Yes| D2[Pass]
        D1 -->|No| D3{Gate 2: Batch-Level α >= 0.667?}
        D3 -->|Yes| D4[Conditional Pass - Flag]
        D3 -->|No| D5[Fail - Trigger Rework]
        D2 --> D6{Gate 3: Per-Class κ >= 0.75?}
        D4 --> D6
        D6 -->|All Classes Pass| D7[Batch Approved]
        D6 -->|Any Class Fails| D8[Route to Disagreement Analysis]
    end

    subgraph CLUSTER["Disagreement Cluster Analysis"]
        D8 --> E1[Build Disagreement Vectors]
        E1 --> E2[Hierarchical Clustering]
        E2 --> E3[Identify Disagreement Clusters]
        E3 --> E4[Cluster 1: Systematic Annotator Bias]
        E3 --> E5[Cluster 2: Spec Ambiguity]
        E3 --> E6[Cluster 3: Edge Cases]
        E3 --> E7[Cluster 4: Random Noise]
    end

    subgraph ROOT_CAUSE["Root Cause Diagnosis"]
        E4 --> F1{Frequency threshold > 5%?}
        E5 --> F2{Class overlap > 0.3 IoU?}
        E6 --> F3{Entropy > 0.8?}
        F1 -->|Yes| G1[Root Cause: Annotator Drift]
        F1 -->|No| G2[Root Cause: Random Error]
        F2 -->|Yes| G3[Root Cause: Spec Ambiguity]
        F2 -->|No| G2
        F3 -->|Yes| G4[Root Cause: Edge Case]
        F3 -->|No| G2
    end

    subgraph ROUTE["Remediation Routing"]
        G1 --> H1[Route to Annotator Calibration]
        G2 --> H2[Log + Accept Noise Floor]
        G3 --> H3[Route to Spec Revision]
        G4 --> H4[Route to Senior Adjudication]
        H1 --> H5[Annotator Retraining Plan]
        H3 --> H6[Guideline Edit + Version Bump]
        H4 --> H7[Senior Review + Gold Panel Insert]
        H5 --> H8[Update Annotator Profile]
        H6 --> H9[New Guideline Version Deployed]
        H7 --> H10[Edge Case Added to Gold Panel]
    end

    INPUT --> METRIC
    METRIC --> AGGREGATE
    AGGREGATE --> GATES
    D8 --> CLUSTER
    CLUSTER --> ROOT_CAUSE
    ROOT_CAUSE --> ROUTE
```

## 4. Gold Panel Calibration and Rotation Lifecycle

```mermaid
flowchart TB
    subgraph CREATE["Gold Panel Creation"]
        A[Adjudicated Annotation Pool] --> A1[Stratify by Difficulty]
        A1 --> A2[Stratify by Class]
        A2 --> A3[Stratify by Modality]
        A3 --> A4[Stratify by Data Source]
        A4 --> A5[Sample: 300 easy / 400 medium / 200 hard / 100 expert]
        A5 --> A6[3+ Independent Adjudications Required]
        A6 --> A7[Compute Gold Ground Truth]
        A7 --> A8[Assign Panel IDs]
        A8 --> A9[Register in Gold Panel Table]
    end

    subgraph ACTIVE["Active Use Phase"]
        B[Gold Panel Active] --> B1[Honeypot Injection: P=0.05 per task]
        B1 --> B2[Annotator Labels Task]
        B2 --> B3{Is Gold Hit?}
        B3 -->|Yes| B4[Compare to Gold Standard]
        B3 -->|No| B5[Normal Processing]
        B4 --> B6[Record Result: Correct / Incorrect]
        B6 --> B7[Update Rolling Annotator Accuracy]
        B7 --> B8{Accuracy < Threshold?}
        B8 -->|No| B1
        B8 -->|Yes| B9[Flag Annotator]
        B9 --> B10[Suspend or Retrain]
    end

    subgraph RECALIB["Recalibration Cycle: Every 2 Weeks"]
        C[Recalibration Trigger] --> C1[Run Gold Panel Against All Active Annotators]
        C1 --> C2[Compute Per-Annotator Accuracy]
        C2 --> C3[Compute Per-Class Accuracy]
        C3 --> C4[Compute Per-Stratum Accuracy]
        C4 --> C5[Drift Detection vs Previous]
        C5 --> C6{Global Accuracy Change > 2%?}
        C6 -->|Yes| C7[Alert: Systematic Drift]
        C6 -->|No| C8{Per-Stratum Change > 5%?}
        C8 -->|Yes| C9[Alert: Stratum-Specific Drift]
        C8 -->|No| C10[No Significant Drift]
        C7 --> C11[Root-Cause Investigation]
        C9 --> C11
        C11 --> C12[Guideline Revision / Retraining]
        C12 --> C13[Update Calibration Records]
    end

    subgraph ROTATE["Rotation: Every 4 Weeks"]
        D[Rotation Trigger] --> D1[Identify 20% of Panel for Retirement]
        D1 --> D2[Select New Adjudicated Items]
        D2 --> D3[Require 3+ Adjudications]
        D3 --> D4[Replace Retired Items]
        D4 --> D5[Update Stratum Balance]
        D5 --> D6[Increment Rotation Round]
        D6 --> D7[Panels older than 12 weeks: Archive]
    end

    subgraph REBUILD["Full Rebuild: Every 12 Weeks"]
        E[Full Rebuild Trigger] --> E1[Archive Entire Gold Panel]
        E1 --> E2[Select Fresh Pool from Latest Adjudicated]
        E2 --> E3[Re-run Stratified Sampling]
        E3 --> E4[Require Fresh 3+ Adjudications]
        E4 --> E5[Build New Panel]
        E5 --> E6[Baseline All Annotators Against New Panel]
        E6 --> E7[Set Rotation Round to 0]
    end

    subgraph ADHOC["Ad-Hoc Insertion"]
        F[Edge Case Identified] --> F1[Senior Adjudicator Reviews]
        F1 --> F2[Adjudicate to Gold Standard]
        F2 --> F3[Assign Difficulty Score]
        F3 --> F4[Insert Into Gold Panel]
        F4 --> F5[Increment Active Count]
        F5 --> F6{Size Exceeds Max?}
        F6 -->|Yes| F7[Retire Lowest-Value Item]
        F6 -->|No| F8[Continue]
    end

    CREATE --> ACTIVE
    ACTIVE --> RECALIB
    ACTIVE --> ROTATE
    ACTIVE --> REBUILD
    ACTIVE --> ADHOC
    RECALIB --> ACTIVE
    ROTATE --> ACTIVE
```

## 5. Statistical Audit Sampling Flow

```mermaid
flowchart TB
    subgraph BATCH["Batch Submitted for Audit"]
        A[Batch: 5000 Annotations] --> A1[Define Strata]
        A1 --> A2[Stratum 1: Easy - n=3000, expected error=2%]
        A2 --> A3[Stratum 2: Medium - n=1500, expected error=5%]
        A3 --> A4[Stratum 3: Hard - n=500, expected error=12%]
    end

    subgraph SAMPLE["Sample Size Computation"]
        A4 --> B1{Estimation or Acceptance?}
        B1 -->|Error Rate Estimation| B2[CI Method]
        B1 -->|Accept/Reject Decision| B3[Acceptance Sampling]
        B2 --> B4[Input: 95% confidence, d=0.02 margin]
        B4 --> B5[Compute: n = Z² * p * (1-p) / d²]
        B5 --> B6[n_easy=235, n_medium=456, n_hard=295]
        B6 --> B7[Total Sample: 986]
        B3 --> B8[Input: AQL=2%, RQL=8%, α=0.05, β=0.10]
        B8 --> B9[Double Sampling Plan: n1=50, n2=80]
        B9 --> B10[Plan: Accept on ≤1 error (n1), ≤3 (n1+n2)]
    end

    subgraph EXECUTE["Sample Execution"]
        B7 --> C1[Stratified Random Draw]
        C1 --> C2[Extract 986 Annotations]
        C2 --> C3[Assign to Senior Auditors]
        C3 --> C4[Auditor Inspects Each Sample]
        C4 --> C5[Record: Correct / Error Type]
    end

    subgraph EVALUATE["Evaluation"]
        C5 --> D1[Compute Strata Error Rates]
        D1 --> D2[e_easy=1.8%, e_medium=4.2%, e_hard=9.5%]
        D2 --> D3[Compute Overall: e=3.1%]
        D3 --> D4[Compute 95% CI: [2.1%, 4.5%]]
        D4 --> D5{Upper CI < AQL?}
        D5 -->|Yes| D6[Accept Batch]
        D5 -->|No| D7{Lower CI > RQL?}
        D7 -->|Yes| D8[Reject Batch]
        D7 -->|No| D9[Conditional - Expand Sample]
        D9 --> D10[Double Sample]
        D10 --> D11[Re-evaluate Combined]
        D11 --> D12{Acceptable?}
        D12 -->|Yes| D6
        D12 -->|No| D8
    end

    subgraph POST["Post-Audit Actions"]
        D6 --> E1[Batch Approved]
        E1 --> E2[Update Batch Status: audited]
        E2 --> E3[Route to Dataset Store]
        D8 --> E4[Batch Rejected]
        E4 --> E5[Flag All Tasks for Rework]
        E5 --> E6[Root-Cause Analysis]
        E6 --> E7[Tag Error Types]
        E7 --> E8[Systematic?]
        E8 -->|Yes| E9[Adjust Spec or Retrain]
        E8 -->|No| E10[Return to Annotator Pool]
        E9 --> E11[Update Calibration Records]
        E10 --> E12[Reset Batch Status: pending]
    end

    BATCH --> SAMPLE
    SAMPLE --> EXECUTE
    EXECUTE --> EVALUATE
    EVALUATE --> POST
```

## 6. Model-Assisted QA Flagging Flow

```mermaid
flowchart TB
    subgraph TRIGGER["Flagging Triggers"]
        A[Annotation Submitted] --> A1{Check Triggers}
        A1 --> B1[Trigger 1: ML Confidence Gap]
        A1 --> B2[Trigger 2: Temporal Inconsistency]
        A1 --> B3[Trigger 3: Object Coverage Alert]
        A1 --> B4[Trigger 4: Class Distribution Anomaly]
        A1 --> B5[Trigger 5: Geometry Violation]
        A1 --> B6[Trigger 6: Annotator Behavioral Signal]
    end

    subgraph LOGIC["Trigger Logic"]
        B1 --> C1[|human_conf - model_conf| > 0.3?]
        C1 -->|Yes| C2[Flag: Confidence Gap]
        B2 --> C3[Same track, prev frame: car, current: truck?]
        C3 -->|Yes| C4[Flag: Track ID Jump]
        B3 --> C5[Model detects N objects, human labels N-2?]
        C5 -->|Yes| C6[Flag: Missing Object]
        B4 --> C7[Class proportion > 3σ from batch mean?]
        C7 -->|Yes| C8[Flag: Distribution Anomaly]
        B5 --> C9[bbox outside frame or zero area?]
        C9 -->|Yes| C10[Flag: Invalid Geometry]
        B6 --> C11[Annotator avg time 8s, this task 45s?]
        C11 -->|Yes| C12[Flag: Behavioral Anomaly]
    end

    subgraph SEVERITY["Severity Scoring"]
        C2 --> D1[Severity = 0.8]
        C4 --> D2[Severity = 0.9]
        C6 --> D3[Severity = 0.6]
        C8 --> D4[Severity = 0.4]
        C10 --> D5[Severity = 1.0]
        C12 --> D6[Severity = 0.3]
        D1 --> D7[Compute Composite Score]
        D2 --> D7
        D3 --> D7
        D4 --> D7
        D5 --> D7
        D6 --> D7
        D7 --> D8{Composite Score Threshold}
    end

    subgraph ACTION["Action Routing"]
        D8 -->|>= 0.9| E1[Immediate Escalation to Senior]
        D8 -->|0.6 - 0.9| E2[Return to Annotator with Flag]
        D8 -->|0.3 - 0.6| E3[Attach Warning to Task]
        D8 -->|< 0.3| E4[Log Only - No Action]
        E1 --> E5[Senior Reviews Full Context]
        E5 --> E6{Confirmed Error?}
        E6 -->|Yes| E7[Correct + Log Error Pattern]
        E6 -->|No| E8[Dismiss Flag - Update Model]
        E2 --> E9[Annotator Reviews Flag]
        E9 --> E10{Annotator Agrees?}
        E10 -->|Yes| E11[Annotator Corrects]
        E10 -->|No| E12[Annotator Disputes]
        E12 --> E13[Auto-Escalate to Senior]
        E3 --> E14[QA Reviewer Sees Warning]
        E14 --> E15[Spot-Check During Review]
    end

    subgraph FEEDBACK["Feedback Loop"]
        E8 --> F1[Update ML Confidence Calibration]
        E7 --> F2[Add Error Case to Training Set]
        F2 --> F3[Periodic Model Retraining Trigger]
        E11 --> F4[Update Annotator Flag Profile]
        F4 --> F5[Flag Rate Threshold Monitoring]
        F5 --> F6{Per-Annotator Flag Rate > 2σ?}
        F6 -->|Yes| F7[Investigate Annotator or Guidelines]
        F6 -->|No| F8[Continue Normal Operations]
    end

    TRIGGER --> LOGIC
    LOGIC --> SEVERITY
    SEVERITY --> ACTION
    ACTION --> FEEDBACK
```

## 7. Dataset Versioning and Release Pipeline

```mermaid
flowchart TB
    subgraph INPUT["Approved Annotations Arrive"]
        A[Batch Approved by Audit] --> A1[Collect Label Groups]
        A1 --> A2[Verify Content Hashes Match]
        A2 -->|Mismatch| A3[Reject Batch - Integrity Error]
        A2 -->|Match| A4[Proceed to Versioning]
    end

    subgraph BUILD["Snapshot Build"]
        A4 --> B1[Load Previous Snapshot Manifest]
        B1 --> B2[Difference Computation]
        B2 --> B3[New Labels: +12,450]
        B3 --> B4[Modified Labels: +230]
        B4 --> B5[Compute New Content Hash]
        B5 --> B6{Bump Type}
        B6 -->|Schema Changed| B7[Major Bump: v4.0.0 -> v5.0.0]
        B6 -->|Labels Changed| B8[Minor Bump: v4.2.0 -> v4.3.0]
        B6 -->|Metadata Only| B9[Patch Bump: v4.2.1 -> v4.2.2]
        B7 --> B10[Generate Semver]
        B8 --> B10
        B9 --> B10
    end

    subgraph MANIFEST["Manifest Generation"]
        B10 --> C1[Write Snapshot Manifest JSON]
        C1 --> C2[Embed: dataset_name, semver, parent]
        C2 --> C3[Embed: schema_version, content_hash]
        C3 --> C4[Embed: stats - counts, splits, classes]
        C4 --> C5[Embed: qa_summary - gold, iaa, audit]
        C5 --> C6[Embed: provenance - lineage DAG]
        C6 --> C7[Embed: batch references]
        C7 --> C8[Sign Manifest with HMAC-SHA256]
    end

    subgraph STORE["Storage & Index"]
        C8 --> D1[Upload Manifest to Object Store]
        D1 --> D2[/snapshots/production-v4/v4.3.0.snapshot.json]
        D2 --> D3[Write Parquet Index]
        D3 --> D4[/snapshots/production-v4/v4.3.0.index.parquet]
        D4 --> D5[Update PostgreSQL Registry]
        D5 --> D6[INSERT INTO dataset_versions]
    end

    subgraph VERIFY["Verification"]
        D6 --> E1[Verify Content Hash]
        E1 --> E2[Recompute Tree Hash]
        E2 --> E3{Matches Manifest?}
        E3 -->|Yes| E4[Verification Passed]
        E3 -->|No| E5[Integrity Failure - Alert]
        E5 --> E6[Rollback Version Record]
        E6 --> E7[Investigate Storage Corruption]
    end

    subgraph EXPORT["Multi-Format Export"]
        E4 --> F1[Trigger Export Pipeline]
        F1 --> F2[Format: COCO JSON]
        F2 --> F3[Export: train/val/test splits]
        F1 --> F4[Format: YOLO .txt]
        F4 --> F5[Export: per-image label files]
        F1 --> F6[Format: Parquet]
        F6 --> F7[Export: columnar + metadata]
        F1 --> F8[Format: JSONL]
        F8 --> F9[Export: line-delimited]
        F1 --> F10[Format: Pascal VOC XML]
        F10 --> F11[Export: per-image XML]
        F3 --> F12[Upload Exports to Object Store]
        F5 --> F12
        F7 --> F12
        F9 --> F12
        F11 --> F12
    end

    subgraph RELEASE["Release & Notify"]
        F12 --> G1[Update Latest Version Pointer]
        G1 --> G2[Tag in Registry: production-v4@latest = v4.3.0]
        G2 --> G3[Push Event: dataset.released]
        G3 --> G4[Notify Downstream Pipelines]
        G4 --> G5[Webhook: Model Training Trigger]
        G4 --> G6[Webhook: Monitoring Dashboard]
        G4 --> G7[Webhook: Data Science Team]
        G5 --> G8[Training Pipeline Pulls New Version]
        G6 --> G9[Dashboard Updates: v4.3.0 Stats]
        G7 --> G10[Release Notes Sent]
    end

    subgraph LINEAGE["Lineage & Audit"]
        G10 --> H1[Record Full Lineage Chain]
        H1 --> H2[Source Data: S3://raw/{sha256}/...]
        H2 --> H3[Annotations: PostgreSQL annotation IDs]
        H3 --> H4[Reviewers: PostgreSQL reviewer IDs]
        H4 --> H5[Adjudications: PostgreSQL adjudication IDs]
        H5 --> H6[Audits: PostgreSQL audit IDs]
        H6 --> H7[Version: PostgreSQL version record]
        H7 --> H8[Training Runs: External MLflow Registry]
        H8 --> H9[Production Models: External Model Registry]
        H9 --> H10[Predictions: External Prediction Store]
    end

    INPUT --> BUILD
    BUILD --> MANIFEST
    MANIFEST --> STORE
    STORE --> VERIFY
    VERIFY --> EXPORT
    EXPORT --> RELEASE
    RELEASE --> LINEAGE
```

## Aggregate Quality Signal Correlation

```mermaid
flowchart LR
    subgraph SIGNALS["Quality Signals"]
        A1[Gold Panel Accuracy]
        A2[Krippendorff's Alpha]
        A3[Audit Pass Rate]
        A4[Annotator Rework Rate]
        A5[Flag Rate from Model QA]
        A6[Spec Revision Count]
    end

    subgraph CORRELATIONS["Correlation Analysis"]
        A1 --> B1{Throughput vs Accuracy}
        A2 --> B2{IAA vs Spec Clarity}
        A3 --> B3{Audit vs Batch Difficulty}
        A4 --> B4{Rework vs Annotator Experience}
        A5 --> B5{Flag Rate vs Model Confidence}
        A6 --> B6{Spec Changes vs IAA Recovery}
        B1 --> C1[Pearson r = -0.12]
        B2 --> C2[Pearson r = 0.74]
        B3 --> C3[Pearson r = -0.68]
        B4 --> C4[Pearson r = -0.53]
        B5 --> C5[Pearson r = 0.41]
        B6 --> C6[Pearson r = 0.62]
    end

    subgraph DASHBOARD["QLI - Quality Leadership Index"]
        C1 --> D1[Component 1: Accuracy Score - 35%]
        C2 --> D1
        C3 --> D2[Component 2: Consistency Score - 30%]
        C4 --> D2
        C5 --> D3[Component 3: Efficiency Score - 20%]
        C6 --> D4[Component 4: Improvement Score - 15%]
        D1 --> E[QLI = 0.35*Acc + 0.30*Con + 0.20*Eff + 0.15*Imp]
        D2 --> E
        D3 --> E
        D4 --> E
        E --> F[Target QLI >= 85 / 100]
    end
```
