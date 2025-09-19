use std::sync::Arc;

use anyhow::Result;
use arrow_schema::SchemaRef;
use byte_unit::{Byte, UnitType};
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::Path;
use parquet::{
    arrow::{async_reader::ParquetObjectReader, parquet_to_arrow_schema},
    file::{metadata::ParquetMetaData, page_index::index::Index},
};

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataDisplay {
    pub file_size: u64,
    pub compressed_row_group_size: u64,
    pub uncompressed_size: u64,
    pub compression_ratio: f64,
    pub row_group_count: u64,
    pub row_count: u64,
    pub columns: u64,
    pub has_row_group_stats: bool,
    pub has_column_index: bool,
    pub has_offset_index: bool,
    pub has_bloom_filter: bool,
    pub total_bloom_filter_size: u64,
    pub schema: SchemaRef,
    pub metadata: Arc<ParquetMetaData>,
    pub metadata_memory_size: u64,
    pub footer_size: u64,
}

impl MetadataDisplay {
    pub fn from_metadata(
        metadata: Arc<ParquetMetaData>,
        metadata_memory_size: u64,
        file_size: u64,
        footer_size: u64,
    ) -> Result<Self> {
        let compressed_row_group_size = metadata
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
            .and_then(|ci| ci.first().map(|row_group_indexes| {
                // Debug: Print the actual Vec content
                web_sys::console::log_1(&format!("=== Vec<Index> Content Debug ===").into());
                web_sys::console::log_1(&format!("row_group_indexes.len() = {}", row_group_indexes.len()).into());
                
                for (i, index) in row_group_indexes.iter().enumerate() {
                    web_sys::console::log_1(&format!("  Index[{}] = {:?}", i, index).into());
                    web_sys::console::log_1(&format!("  Index[{}] is Index::NONE? {}", i, matches!(index, Index::NONE)).into());
                }
                
                let any_non_none = row_group_indexes.iter().any(|index| !matches!(index, Index::NONE));
                web_sys::console::log_1(&format!("any_non_none result = {}", any_non_none).into());
                
                any_non_none
            }))
            .unwrap_or(false);
            
        // Debug logging for file-level column index check
        web_sys::console::log_1(&"=== File-level Column Index Debug ===".into());
        if let Some(column_indexes) = metadata.column_index() {
            web_sys::console::log_1(&format!("Found column indexes for {} row groups", column_indexes.len()).into());
            if let Some(first_rg_indexes) = column_indexes.first() {
                web_sys::console::log_1(&format!("First row group has {} column indexes", first_rg_indexes.len()).into());
                let first_is_empty = first_rg_indexes.is_empty();
                web_sys::console::log_1(&format!("First row group indexes is_empty: {}", first_is_empty).into());
                web_sys::console::log_1(&format!("File-level has_column_index will be: {}", !first_is_empty).into());
            }
        } else {
            web_sys::console::log_1(&"No column indexes found at file level".into());
        }

        let has_offset_index = metadata
            .offset_index()
            .and_then(|ci| ci.first().map(|c| !c.is_empty()))
            .unwrap_or(false);

        let has_bloom_filter = first_column
            .map(|c| c.bloom_filter_offset().is_some())
            .unwrap_or(false);

        // Calculate total bloom filter size across all row groups and columns
        let total_bloom_filter_size = metadata
            .row_groups()
            .iter()
            .flat_map(|rg| rg.columns())
            .filter_map(|col| col.bloom_filter_length())
            .map(|len| len as u64)
            .sum();

        Ok(Self {
            file_size,
            compressed_row_group_size,
            uncompressed_size,
            compression_ratio: compressed_row_group_size as f64 / uncompressed_size as f64,
            row_group_count: metadata.num_row_groups() as u64,
            row_count: metadata.file_metadata().num_rows() as u64,
            columns: schema.fields.len() as u64,
            has_row_group_stats: first_column
                .map(|c| c.statistics().is_some())
                .unwrap_or(false),
            has_column_index,
            has_offset_index,
            has_bloom_filter,
            total_bloom_filter_size,
            schema: Arc::new(schema),
            metadata,
            metadata_memory_size,
            footer_size,
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
            "File Size: {:.2}\nCompressed Row Groups: {:.2}\nFooter Size: {:.2}\nMemory Size: {:.2}\nBloom Filter Size: {:.2}\nRow Groups: {}\nTotal Rows: {}\nColumns: {}\nFeatures: {}{}{}{}",
            Byte::from_u64(self.file_size).get_appropriate_unit(UnitType::Binary),
            Byte::from_u64(self.compressed_row_group_size).get_appropriate_unit(UnitType::Binary),
            Byte::from_u64(self.footer_size).get_appropriate_unit(UnitType::Binary),
            Byte::from_u64(self.metadata_memory_size).get_appropriate_unit(UnitType::Binary),
            Byte::from_u64(self.total_bloom_filter_size).get_appropriate_unit(UnitType::Binary),
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
