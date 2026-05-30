use std::collections::HashMap;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub id: Uuid,
    pub name: String,
    pub stages: Vec<StageConfig>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageConfig {
    pub name: String,
    pub stage_type: StageType,
    pub params: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StageType {
    Decompose,
    Transform,
    Assemble,
    Branch,
    Merge,
    Filter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageReport {
    pub name: String,
    pub stage_type: StageType,
    pub duration_ms: u64,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub tiles_count: usize,
    pub cr: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineReport {
    pub pipeline_id: Uuid,
    pub stages: Vec<StageReport>,
    pub total_duration_ms: u64,
    pub overall_cr: f64,
    pub input_bytes: usize,
    pub output_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct StageOutput {
    pub data: Vec<u8>,
    pub tiles_count: usize,
    pub cr: f64,
    pub duration_ms: u64,
}

pub type Result<T> = std::result::Result<T, PipelineError>;

#[derive(Debug)]
pub enum PipelineError {
    StageNotFound(String),
    StageExecution(String),
    Validation(String),
    EmptyPipeline,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::StageNotFound(s) => write!(f, "stage not found: {}", s),
            PipelineError::StageExecution(s) => write!(f, "stage execution failed: {}", s),
            PipelineError::Validation(s) => write!(f, "validation error: {}", s),
            PipelineError::EmptyPipeline => write!(f, "no stages in pipeline"),
        }
    }
}

impl std::error::Error for PipelineError {}

// ---------------------------------------------------------------------------
// PipelineStage trait
// ---------------------------------------------------------------------------

pub trait PipelineStage: Send + Sync {
    fn name(&self) -> &str;
    fn stage_type(&self) -> StageType;
    fn execute(&self, input: &[u8], params: &HashMap<String, String>) -> Result<StageOutput>;
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

pub struct Pipeline {
    config: PipelineConfig,
    stages: Vec<Box<dyn PipelineStage>>,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Pipeline {
            config,
            stages: Vec::new(),
        }
    }

    pub fn add_stage(mut self, stage: Box<dyn PipelineStage>) -> Self {
        self.stages.push(stage);
        self
    }

    pub fn run(&self, input: &[u8]) -> Result<(Vec<u8>, PipelineReport)> {
        if self.stages.is_empty() {
            return Err(PipelineError::EmptyPipeline);
        }

        let start = Instant::now();
        let mut current = input.to_vec();
        let input_bytes = input.len();
        let mut stage_reports = Vec::new();

        for stage in &self.stages {
            let params = self
                .config
                .stages
                .iter()
                .find(|sc| sc.name == stage.name())
                .map(|sc| sc.params.clone())
                .unwrap_or_default();

            let output = stage.execute(&current, &params)?;
            let report = StageReport {
                name: stage.name().to_string(),
                stage_type: stage.stage_type(),
                duration_ms: output.duration_ms,
                input_bytes: current.len(),
                output_bytes: output.data.len(),
                tiles_count: output.tiles_count,
                cr: output.cr,
            };
            current = output.data;
            stage_reports.push(report);
        }

        let total_duration_ms = start.elapsed().as_millis() as u64;
        let output_bytes = current.len();
        let overall_cr = if input_bytes > 0 {
            (output_bytes as f64) / (input_bytes as f64)
        } else {
            1.0
        };

        let report = PipelineReport {
            pipeline_id: self.config.id,
            stages: stage_reports,
            total_duration_ms,
            overall_cr,
            input_bytes,
            output_bytes,
        };

        Ok((current, report))
    }

    pub fn run_stage(&self, stage_name: &str, input: &[u8]) -> Result<StageOutput> {
        let stage = self
            .stages
            .iter()
            .find(|s| s.name() == stage_name)
            .ok_or_else(|| PipelineError::StageNotFound(stage_name.to_string()))?;

        let params = self
            .config
            .stages
            .iter()
            .find(|sc| sc.name == stage_name)
            .map(|sc| sc.params.clone())
            .unwrap_or_default();

        stage.execute(input, &params)
    }

    pub fn dry_run(&self, input: &[u8]) -> PipelineReport {
        let mut stage_reports = Vec::new();
        let mut estimated_bytes = input.len();

        for stage in &self.stages {
            let est_ratio = match stage.stage_type() {
                StageType::Decompose => 1.1,
                StageType::Transform => 1.0,
                StageType::Assemble => 0.9,
                StageType::Filter => 0.5,
                StageType::Branch => 2.0,
                StageType::Merge => 0.7,
            };
            let out_bytes = (estimated_bytes as f64 * est_ratio) as usize;
            let tiles = match stage.stage_type() {
                StageType::Decompose => (input.len() / 64).max(1),
                StageType::Assemble => 1,
                _ => (input.len() / 128).max(1),
            };

            stage_reports.push(StageReport {
                name: stage.name().to_string(),
                stage_type: stage.stage_type(),
                duration_ms: 0,
                input_bytes: estimated_bytes,
                output_bytes: out_bytes,
                tiles_count: tiles,
                cr: est_ratio,
            });
            estimated_bytes = out_bytes;
        }

        let total_duration_ms = self.stages.len() as u64 * 10; // rough estimate
        let overall_cr = if !input.is_empty() {
            (estimated_bytes as f64) / (input.len() as f64)
        } else {
            1.0
        };

        PipelineReport {
            pipeline_id: self.config.id,
            stages: stage_reports,
            total_duration_ms,
            overall_cr,
            input_bytes: input.len(),
            output_bytes: estimated_bytes,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.stages.is_empty() {
            return Err(PipelineError::Validation("pipeline has no stages".into()));
        }

        // Check stage names are unique
        let mut names = std::collections::HashSet::new();
        for stage in &self.stages {
            if !names.insert(stage.name().to_string()) {
                return Err(PipelineError::Validation(format!(
                    "duplicate stage name: {}",
                    stage.name()
                )));
            }
        }

        // Check that config stages match actual stages
        for stage in &self.stages {
            let found = self
                .config
                .stages
                .iter()
                .any(|sc| sc.name == stage.name());
            if !found {
                return Err(PipelineError::Validation(format!(
                    "stage '{}' has no matching config",
                    stage.name()
                )));
            }
        }

        Ok(())
    }

    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }
}

// ---------------------------------------------------------------------------
// Built-in stages
// ---------------------------------------------------------------------------

// -- TextDecomposeStage --

pub struct TextDecomposeStage;

impl PipelineStage for TextDecomposeStage {
    fn name(&self) -> &str {
        "text_decompose"
    }
    fn stage_type(&self) -> StageType {
        StageType::Decompose
    }
    fn execute(&self, input: &[u8], params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let chunk_size: usize = params
            .get("chunk_size")
            .and_then(|v| v.parse().ok())
            .unwrap_or(64);

        let chars: Vec<char> = text.chars().collect();
        let chunks: Vec<String> = chars
            .chunks(chunk_size)
            .map(|c| c.iter().collect::<String>())
            .collect();

        let tiles_count = chunks.len();
        let data = chunks.join("\n---TILE---\n").into_bytes();

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- CsvDecomposeStage --

pub struct CsvDecomposeStage;

impl PipelineStage for CsvDecomposeStage {
    fn name(&self) -> &str {
        "csv_decompose"
    }
    fn stage_type(&self) -> StageType {
        StageType::Decompose
    }
    fn execute(&self, input: &[u8], params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let delimiter = params
            .get("delimiter")
            .map(|d| d.chars().next().unwrap_or(','))
            .unwrap_or(',');

        let mut lines = text.lines().peekable();
        let header = lines.next().unwrap_or("").to_string();
        let columns: Vec<&str> = header.split(delimiter).collect();

        let mut tiles = Vec::new();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let values: Vec<&str> = line.split(delimiter).collect();
            let mut tile = Vec::new();
            for (i, col) in columns.iter().enumerate() {
                let val = values.get(i).unwrap_or(&"");
                tile.push(format!("{}:{}", col.trim(), val.trim()));
            }
            tiles.push(tile.join("|"));
        }

        let tiles_count = tiles.len();
        let data = tiles.join("\n").into_bytes();

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- JsonDecomposeStage --

pub struct JsonDecomposeStage;

impl PipelineStage for JsonDecomposeStage {
    fn name(&self) -> &str {
        "json_decompose"
    }
    fn stage_type(&self) -> StageType {
        StageType::Decompose
    }
    fn execute(&self, input: &[u8], _params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let parsed: serde_json::Value =
            serde_json::from_slice(input).map_err(|e| PipelineError::StageExecution(e.to_string()))?;

        let tiles = match parsed {
            serde_json::Value::Array(arr) => arr
                .iter()
                .enumerate()
                .map(|(i, v)| format!("tile_{}:{}", i, v))
                .collect::<Vec<_>>(),
            serde_json::Value::Object(map) => map
                .into_iter()
                .map(|(k, v)| format!("{}:{}", k, v))
                .collect::<Vec<_>>(),
            other => vec![format!("root:{}", other)],
        };

        let tiles_count = tiles.len();
        let data = tiles.join("\n").into_bytes();

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- FilterStage --

#[allow(dead_code)]
pub struct FilterStage {
    field: String,
    op: String,
    value: String,
}

impl FilterStage {
    pub fn new(field: impl Into<String>, op: impl Into<String>, value: impl Into<String>) -> Self {
        FilterStage {
            field: field.into(),
            op: op.into(),
            value: value.into(),
        }
    }
}

impl PipelineStage for FilterStage {
    fn name(&self) -> &str {
        "filter"
    }
    fn stage_type(&self) -> StageType {
        StageType::Filter
    }
    fn execute(&self, input: &[u8], _params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let lines: Vec<&str> = text.lines().collect();

        let filtered: Vec<&str> = lines
            .iter()
            .filter(|line| {
                let lower = line.to_lowercase();
                match self.op.as_str() {
                    "contains" => lower.contains(&self.value.to_lowercase()),
                    "equals" => lower == self.value.to_lowercase(),
                    "starts_with" => lower.starts_with(&self.value.to_lowercase()),
                    "ends_with" => lower.ends_with(&self.value.to_lowercase()),
                    "not_contains" => !lower.contains(&self.value.to_lowercase()),
                    _ => true,
                }
            })
            .copied()
            .collect();

        let tiles_count = filtered.len();
        let data = filtered.join("\n").into_bytes();

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- SortStage --

pub struct SortStage {
#[allow(dead_code)]
    field: String,
    descending: bool,
}

impl SortStage {
    pub fn new(field: impl Into<String>, descending: bool) -> Self {
        SortStage {
            field: field.into(),
            descending,
        }
    }
}

impl PipelineStage for SortStage {
    fn name(&self) -> &str {
        "sort"
    }
    fn stage_type(&self) -> StageType {
        StageType::Transform
    }
    fn execute(&self, input: &[u8], _params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let mut lines: Vec<&str> = text.lines().collect();

        // Sort by the field prefix (e.g., "name:alice" sorts by "alice" part)
        lines.sort_by(|a, b| {
            let a_val = a.split(':').nth(1).unwrap_or(a);
            let b_val = b.split(':').nth(1).unwrap_or(b);
            if self.descending {
                b_val.cmp(a_val)
            } else {
                a_val.cmp(b_val)
            }
        });

        let tiles_count = lines.len();
        let data = lines.join("\n").into_bytes();

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- MapStage --

pub struct MapStage {
    transform_fn: fn(&str) -> String,
    label: String,
}

impl MapStage {
    pub fn new(label: impl Into<String>, transform_fn: fn(&str) -> String) -> Self {
        MapStage {
            transform_fn,
            label: label.into(),
        }
    }
}

impl PipelineStage for MapStage {
    fn name(&self) -> &str {
        &self.label
    }
    fn stage_type(&self) -> StageType {
        StageType::Transform
    }
    fn execute(&self, input: &[u8], _params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let mapped: Vec<String> = text.lines().map(|line| (self.transform_fn)(line)).collect();

        let tiles_count = mapped.len();
        let data = mapped.join("\n").into_bytes();

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- TextAssembleStage --

pub struct TextAssembleStage;

impl PipelineStage for TextAssembleStage {
    fn name(&self) -> &str {
        "text_assemble"
    }
    fn stage_type(&self) -> StageType {
        StageType::Assemble
    }
    fn execute(&self, input: &[u8], _params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let assembled = text.replace("\n---TILE---\n", "\n").into_bytes();

        let tiles_count = 1;
        let cr = (assembled.len() as f64) / (input.len().max(1) as f64);

        Ok(StageOutput {
            data: assembled,
            tiles_count,
            cr,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- CsvAssembleStage --

pub struct CsvAssembleStage {
    headers: String,
}

impl CsvAssembleStage {
    pub fn new(headers: impl Into<String>) -> Self {
        CsvAssembleStage {
            headers: headers.into(),
        }
    }
}

impl PipelineStage for CsvAssembleStage {
    fn name(&self) -> &str {
        "csv_assemble"
    }
    fn stage_type(&self) -> StageType {
        StageType::Assemble
    }
    fn execute(&self, input: &[u8], _params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let mut rows = vec![self.headers.clone()];

        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let values: Vec<&str> = line.split('|').map(|p| p.split(':').nth(1).unwrap_or("")).collect();
            rows.push(values.join(","));
        }

        let data = rows.join("\n").into_bytes();
        let tiles_count = 1;

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// -- JsonAssembleStage --

pub struct JsonAssembleStage;

impl PipelineStage for JsonAssembleStage {
    fn name(&self) -> &str {
        "json_assemble"
    }
    fn stage_type(&self) -> StageType {
        StageType::Assemble
    }
    fn execute(&self, input: &[u8], _params: &HashMap<String, String>) -> Result<StageOutput> {
        let start = Instant::now();
        let text = String::from_utf8_lossy(input);
        let mut arr = Vec::new();

        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let mut obj = serde_json::Map::new();
            let parts: Vec<&str> = line.split('|').collect();
            for part in parts {
                if let Some((k, v)) = part.split_once(':') {
                    let v = v.trim().trim_matches('"');
                    if let Ok(n) = v.parse::<i64>() {
                        obj.insert(k.trim().to_string(), serde_json::Value::Number(n.into()));
                    } else if let Ok(f) = v.parse::<f64>() {
                        obj.insert(
                            k.trim().to_string(),
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(f)
                                    .unwrap_or_else(|| serde_json::Number::from(0)),
                            ),
                        );
                    } else {
                        obj.insert(
                            k.trim().to_string(),
                            serde_json::Value::String(v.to_string()),
                        );
                    }
                }
            }
            arr.push(serde_json::Value::Object(obj));
        }

        let data =
            serde_json::to_string_pretty(&serde_json::Value::Array(arr)).unwrap_or_default().into_bytes();
        let tiles_count = 1;

        Ok(StageOutput {
            cr: (data.len() as f64) / (input.len().max(1) as f64),
            data,
            tiles_count,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(name: &str, stages: Vec<StageConfig>) -> PipelineConfig {
        PipelineConfig {
            id: Uuid::new_v4(),
            name: name.to_string(),
            stages,
            metadata: HashMap::new(),
        }
    }

    fn make_stage_config(name: &str, stage_type: StageType) -> StageConfig {
        StageConfig {
            name: name.to_string(),
            stage_type,
            params: HashMap::new(),
        }
    }

    #[test]
    fn test_pipeline_creation() {
        let config = make_config("test", vec![]);
        let pipeline = Pipeline::new(config);
        assert_eq!(pipeline.stage_count(), 0);
    }

    #[test]
    fn test_add_stages() {
        let config = make_config(
            "test",
            vec![make_stage_config("text_decompose", StageType::Decompose)],
        );
        let pipeline = Pipeline::new(config).add_stage(Box::new(TextDecomposeStage));
        assert_eq!(pipeline.stage_count(), 1);
    }

    #[test]
    fn test_empty_pipeline_errors() {
        let config = make_config("test", vec![]);
        let pipeline = Pipeline::new(config);
        let result = pipeline.run(b"hello");
        assert!(result.is_err());
    }

    #[test]
    fn test_text_decompose_stage() {
        let config = make_config(
            "test",
            vec![make_stage_config("text_decompose", StageType::Decompose)],
        );
        let pipeline = Pipeline::new(config).add_stage(Box::new(TextDecomposeStage));
        let (output, report) = pipeline.run(b"hello world").unwrap();
        assert!(!output.is_empty());
        assert_eq!(report.stages.len(), 1);
        assert_eq!(report.stages[0].name, "text_decompose");
    }

    #[test]
    fn test_csv_decompose_stage() {
        let config = make_config(
            "test",
            vec![make_stage_config("csv_decompose", StageType::Decompose)],
        );
        let pipeline = Pipeline::new(config).add_stage(Box::new(CsvDecomposeStage));
        let input = b"name,age\nalice,30\nbob,25";
        let (output, report) = pipeline.run(input).unwrap();
        let text = String::from_utf8_lossy(&output);
        assert!(text.contains("name:alice|age:30"));
        assert_eq!(report.stages[0].tiles_count, 2);
    }

    #[test]
    fn test_json_decompose_stage() {
        let config = make_config(
            "test",
            vec![make_stage_config("json_decompose", StageType::Decompose)],
        );
        let pipeline = Pipeline::new(config).add_stage(Box::new(JsonDecomposeStage));
        let input = br#"[{"name":"alice","age":30},{"name":"bob","age":25}]"#;
        let (output, report) = pipeline.run(input).unwrap();
        let text = String::from_utf8_lossy(&output);
        assert!(text.contains("tile_0:"));
        assert!(text.contains("alice"));
        assert_eq!(report.stages[0].tiles_count, 2);
    }

    #[test]
    fn test_filter_stage() {
        let config = make_config(
            "test",
            vec![make_stage_config("filter", StageType::Filter)],
        );
        let filter = FilterStage::new("content", "contains", "tile_0");
        let pipeline = Pipeline::new(config).add_stage(Box::new(filter));
        let input = b"tile_0:hello\ntile_1:world\ntile_0:foo";
        let (output, report) = pipeline.run(input).unwrap();
        let text = String::from_utf8_lossy(&output);
        assert!(text.contains("tile_0:hello"));
        assert!(text.contains("tile_0:foo"));
        assert!(!text.contains("tile_1:world"));
        assert_eq!(report.stages[0].tiles_count, 2);
    }

    #[test]
    fn test_sort_stage() {
        let config = make_config(
            "test",
            vec![make_stage_config("sort", StageType::Transform)],
        );
        let sort = SortStage::new("val", false);
        let pipeline = Pipeline::new(config).add_stage(Box::new(sort));
        let input = b"name:charlie\nname:alice\nname:bob";
        let (output, _) = pipeline.run(input).unwrap();
        let text = String::from_utf8_lossy(&output);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "name:alice");
        assert_eq!(lines[1], "name:bob");
        assert_eq!(lines[2], "name:charlie");
    }

    #[test]
    fn test_multi_stage_pipeline() {
        let config = make_config(
            "multi",
            vec![
                make_stage_config("text_decompose", StageType::Decompose),
                make_stage_config("text_assemble", StageType::Assemble),
            ],
        );
        let pipeline = Pipeline::new(config)
            .add_stage(Box::new(TextDecomposeStage))
            .add_stage(Box::new(TextAssembleStage));
        let (output, report) = pipeline.run(b"hello world").unwrap();
        assert_eq!(report.stages.len(), 2);
        assert!(output.len() > 0);
    }

    #[test]
    fn test_pipeline_report() {
        let config = make_config(
            "test",
            vec![make_stage_config("text_decompose", StageType::Decompose)],
        );
        let pipeline = Pipeline::new(config).add_stage(Box::new(TextDecomposeStage));
        let (_, report) = pipeline.run(b"test data").unwrap();
        assert_eq!(report.pipeline_id, pipeline.config().id);
        assert_eq!(report.input_bytes, 9);
        assert!(report.total_duration_ms < 1000);
        assert_eq!(report.stages.len(), 1);
    }

    #[test]
    fn test_dry_run() {
        let config = make_config(
            "test",
            vec![
                make_stage_config("text_decompose", StageType::Decompose),
                make_stage_config("text_assemble", StageType::Assemble),
            ],
        );
        let pipeline = Pipeline::new(config)
            .add_stage(Box::new(TextDecomposeStage))
            .add_stage(Box::new(TextAssembleStage));
        let report = pipeline.dry_run(b"test input");
        assert_eq!(report.stages.len(), 2);
        assert_eq!(report.input_bytes, 10);
    }

    #[test]
    fn test_validate_empty_pipeline() {
        let config = make_config("test", vec![]);
        let pipeline = Pipeline::new(config);
        assert!(pipeline.validate().is_err());
    }

    #[test]
    fn test_validate_success() {
        let config = make_config(
            "test",
            vec![make_stage_config("text_decompose", StageType::Decompose)],
        );
        let pipeline = Pipeline::new(config).add_stage(Box::new(TextDecomposeStage));
        assert!(pipeline.validate().is_ok());
    }

    #[test]
    fn test_run_single_stage_by_name() {
        let config = make_config(
            "test",
            vec![make_stage_config("text_decompose", StageType::Decompose)],
        );
        let pipeline = Pipeline::new(config).add_stage(Box::new(TextDecomposeStage));
        let output = pipeline.run_stage("text_decompose", b"hello").unwrap();
        assert!(output.tiles_count >= 1);
    }

    #[test]
    fn test_run_stage_not_found() {
        let config = make_config("test", vec![]);
        let pipeline = Pipeline::new(config);
        let result = pipeline.run_stage("nonexistent", b"test");
        assert!(result.is_err());
    }

    #[test]
    fn test_map_stage() {
        let config = make_config(
            "test",
            vec![make_stage_config("uppercase", StageType::Transform)],
        );
        let map = MapStage::new("uppercase", |line| line.to_uppercase());
        let pipeline = Pipeline::new(config).add_stage(Box::new(map));
        let (output, _) = pipeline.run(b"hello\nworld").unwrap();
        let text = String::from_utf8_lossy(&output);
        assert!(text.contains("HELLO"));
        assert!(text.contains("WORLD"));
    }

    #[test]
    fn test_csv_round_trip() {
        let config = make_config(
            "test",
            vec![
                make_stage_config("csv_decompose", StageType::Decompose),
                make_stage_config("csv_assemble", StageType::Assemble),
            ],
        );
        let pipeline = Pipeline::new(config)
            .add_stage(Box::new(CsvDecomposeStage))
            .add_stage(Box::new(CsvAssembleStage::new("name,age")));
        let input = b"name,age\nalice,30\nbob,25";
        let (output, report) = pipeline.run(input).unwrap();
        let text = String::from_utf8_lossy(&output);
        assert!(text.contains("alice,30"));
        assert!(text.contains("bob,25"));
        assert_eq!(report.stages.len(), 2);
    }

    #[test]
    fn test_json_round_trip() {
        let config = make_config(
            "test",
            vec![
                make_stage_config("json_decompose", StageType::Decompose),
                make_stage_config("json_assemble", StageType::Assemble),
            ],
        );
        let pipeline = Pipeline::new(config)
            .add_stage(Box::new(JsonDecomposeStage))
            .add_stage(Box::new(JsonAssembleStage));
        let input = br#"[{"name":"alice","age":30}]"#;
        let (output, _) = pipeline.run(input).unwrap();
        let text = String::from_utf8_lossy(&output);
        assert!(text.contains("alice"));
    }
}
