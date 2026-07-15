# Annotation Tools, Format Converters, QC Frameworks & Storage

## Annotation Platforms
- **Label Studio** – OSS (Apache 2.0), multi-modal (image, text,
  audio, video, time-series), ML backends, custom XML templates.
- **CVAT** – OSS (MIT), CV-focused (image, video, 3D), SAM 3,
  COCO/YOLO/VOC export, review pipeline.
- **Labelbox** – Enterprise, MAL, multi-stage review, consensus.
- **Encord** – Enterprise, DICOM/NIfTI, active learning, QA tiers.
- **SuperAnnotate** – CV+NLP, layered QA, managed workforce opt.

## Format Converters
- **panlabel** – Rust CLI: COCO ↔ YOLO ↔ VOC ↔ LabelStudio ↔
  CVAT ↔ KITTI ↔ OpenImages ↔ TFOD (strickvl/panlabel).
- **cocoyolo** – Python: bidirectional COCO↔YOLO, RLE/hole-aware,
  disjoint-region strategies (vittorio-prodomo/cocoyolo).
- **yolococo** – Python: YOLO↔COCO + COCO dataset merger.
- **Ultralytics JSON2YOLO** – part of ultralytics package.

## QC & IAA Frameworks
- **DataLabel** – Serverless: Cohen's κ, Fleiss' κ, Krippendorff's
  α, HTML dashboards, majority/average/strict merge strategies.
- **iaa-kit** – Pure NumPy IAA with bootstrap CIs, missing-data.
- **Concordia** – JS/Node IAA toolkit with bootstrap CIs, CLI.
- **AgreeKit** – Tool-agnostic inter-coder reliability (Kripp.
  α, Cohen's/Fleiss' κ), adjudication workflows.

## Storage & Pipeline
- Redis (in-flight state) + PostgreSQL (durable audit trail).
- Cloud storage (AWS S3, GCP, Azure) via zero-migration arch.
