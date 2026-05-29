# forge-pipeline

Pipeline orchestration that composes decomposers, transforms, and assemblers into runnable graphs.

This is the orchestration layer — takes individual `forge-*` crates and wires them into executable pipelines.

## Core Types

- **`Pipeline`** — Orchestrates stages, runs them sequentially, produces reports
- **`PipelineStage`** — Trait for pluggable pipeline stages (decompose, transform, assemble, filter)
- **`PipelineConfig`** / **`StageConfig`** — Serializable configuration with per-stage parameters
- **`PipelineReport`** / **`StageReport`** — Detailed execution reports with timing, compression ratios, and tile counts
- **`StageOutput`** — Output from a single stage execution

## Built-in Stages

| Stage | Type | Description |
|-------|------|-------------|
| `TextDecomposeStage` | Decompose | Splits text into tiles by chunk size |
| `CsvDecomposeStage` | Decompose | Parses CSV rows into key:value tiles |
| `JsonDecomposeStage` | Decompose | Decomposes JSON arrays/objects into tiles |
| `FilterStage` | Filter | Filters tiles by field/op/value (contains, equals, etc.) |
| `SortStage` | Transform | Sorts tiles by a field value |
| `MapStage` | Transform | Applies a mapping function to each tile |
| `TextAssembleStage` | Assemble | Reassembles text tiles into a single document |
| `CsvAssembleStage` | Assemble | Reassembles CSV tiles with headers |
| `JsonAssembleStage` | Assemble | Reassembles tiles into a JSON array |

## Usage

```rust
use forge_pipeline::*;
use std::collections::HashMap;
use uuid::Uuid;

let config = PipelineConfig {
    id: Uuid::new_v4(),
    name: "my-pipeline".into(),
    stages: vec![
        StageConfig {
            name: "text_decompose".into(),
            stage_type: StageType::Decompose,
            params: HashMap::new(),
        },
        StageConfig {
            name: "text_assemble".into(),
            stage_type: StageType::Assemble,
            params: HashMap::new(),
        },
    ],
    metadata: HashMap::new(),
};

let pipeline = Pipeline::new(config)
    .add_stage(Box::new(TextDecomposeStage))
    .add_stage(Box::new(TextAssembleStage));

// Run the pipeline
let (output, report) = pipeline.run(b"hello world").unwrap();

// Dry run (estimates without executing)
let estimate = pipeline.dry_run(b"hello world");

// Validate stage configuration
pipeline.validate().unwrap();
```

## Dependencies

- `serde` + `serde_json` — Serialization
- `uuid` — Pipeline and stage identification
