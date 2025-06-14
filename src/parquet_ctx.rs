use std::sync::Arc;

use anyhow::Result;
use arrow_schema::SchemaRef;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::Path;
use parquet::{
    arrow::{async_reader::ParquetObjectReader, parquet_to_arrow_schema},
    file::metadata::ParquetMetaData,
};

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataDisplay {
    pub file_size: u64,
    pub uncompressed_size: u64,
    pub compression_ratio: f64,
    pub row_group_count: u64,
    pub row_count: u64,
    pub columns: u64,
    pub has_row_group_stats: bool,
    pub has_column_index: bool,
    pub has_offset_index: bool,
    pub has_bloom_filter: bool,
    pub schema: SchemaRef,
    pub metadata: Arc<ParquetMetaData>,
    pub metadata_len: u64,
}

impl MetadataDisplay {
    pub fn from_metadata(metadata: Arc<ParquetMetaData>, metadata_len: u64) -> Result<Self> {
        let compressed_size = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.compressed_size())
            .sum::<i64>() as u64;
        let uncompressed_size = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size())
            .sum::<i64>() as u64;

        let schema = parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )?;
        let first_row_group = metadata.row_groups().first();
        let first_column = first_row_group.and_then(|rg| rg.columns().first());

        let has_column_index = metadata
            .column_index()
            .and_then(|ci| ci.first().map(|c| !c.is_empty()))
            .unwrap_or(false);
        let has_offset_index = metadata
            .offset_index()
            .and_then(|ci| ci.first().map(|c| !c.is_empty()))
            .unwrap_or(false);

        Ok(Self {
            file_size: compressed_size,
            uncompressed_size,
            compression_ratio: compressed_size as f64 / uncompressed_size as f64,
            row_group_count: metadata.num_row_groups() as u64,
            row_count: metadata.file_metadata().num_rows() as u64,
            columns: schema.fields.len() as u64,
            has_row_group_stats: first_column
                .map(|c| c.statistics().is_some())
                .unwrap_or(false),
            has_column_index,
            has_offset_index,
            has_bloom_filter: first_column
                .map(|c| c.bloom_filter_offset().is_some())
                .unwrap_or(false),
            schema: Arc::new(schema),
            metadata,
            metadata_len,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl std::fmt::Display for MetadataDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "File Size: {} MB\nRow Groups: {}\nTotal Rows: {}\nColumns: {}\nFeatures: {}{}{}{}",
            self.file_size as f64 / 1_048_576.0, // Convert bytes to MB
            self.row_group_count,
            self.row_count,
            self.columns,
            if self.has_row_group_stats {
                "✓ Statistics "
            } else {
                "✗ Statistics "
            },
            if self.has_column_index {
                "✓ Column Index "
            } else {
                "✗ Column Index "
            },
            if self.has_offset_index {
                "✓ Offset Index "
            } else {
                "✗ Offset Index "
            },
            if self.has_bloom_filter {
                "✓ Bloom Filter"
            } else {
                "✗ Bloom Filter"
            },
        )
    }
}

#[derive(Debug, Clone)]
pub struct ParquetResolved {
    reader: ParquetObjectReader,
    table_name: String,
    path: Path,
    object_store_url: ObjectStoreUrl,
    metadata: MetadataDisplay,
}

impl PartialEq for ParquetResolved {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.path == other.path
            && self.object_store_url == other.object_store_url
    }
}

impl ParquetResolved {
    pub fn new(
        reader: ParquetObjectReader,
        table_name: String,
        path: Path,
        object_store_url: ObjectStoreUrl,
        display_info: MetadataDisplay,
    ) -> Self {
        Self {
            reader,
            table_name,
            path,
            object_store_url,
            metadata: display_info,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn metadata(&self) -> &MetadataDisplay {
        &self.metadata
    }

    pub fn reader(&self) -> &ParquetObjectReader {
        &self.reader
    }
}
