use crate::Result;
use crate::ids::Names;
use crate::input::Id;
use crate::rollup::{CampaignDays, CampaignTally, UserRow};
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::TypePtr;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

const USERS: &str = "
message user_daily {
    REQUIRED BYTE_ARRAY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CAMPAIGNS: &str = "
message campaign_daily {
    REQUIRED BYTE_ARRAY campaign_id (UTF8);
    REQUIRED BYTE_ARRAY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

pub struct Parquet {
    users: TypePtr,
    campaigns: TypePtr,
    properties: Arc<WriterProperties>,
}

fn column<T: DataType, W: Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    values: impl IntoIterator<Item = T::T>,
    def_levels: Option<&[i16]>,
) -> Result<()> {
    let values: Vec<_> = values.into_iter().collect();
    let mut writer = group.next_column()?.expect("schema and columns agree");
    writer.typed::<T>().write_batch(&values, def_levels, None)?;
    writer.close()?;
    Ok(())
}

fn bytes(value: &[u8]) -> ByteArray {
    ByteArray::from(value.to_vec())
}

// Encode the required UTF-8 user column directly into standard PLAIN pages.
// Each packed key can append its bytes without allocating an owned string.
fn user_ids<W: Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    descriptor: parquet::schema::types::ColumnDescPtr,
    rows: &[UserRow<'_>],
    names: &Names,
) -> Result<()> {
    use bytes::Bytes;
    use parquet::basic::Encoding;
    use parquet::column::page::{CompressedPage, Page, PageWriter};
    use parquet::column::writer::ColumnCloseResult;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::file::writer::{SerializedPageWriter, TrackedWrite};
    let mut encoded = TrackedWrite::new(Vec::<u8>::new());
    let mut uncompressed = 0usize;
    let mut encoder = snap::raw::Encoder::new();
    for batch in rows.chunks(8192) {
        let mut plain = Vec::with_capacity(batch.len() * 16);
        for row in batch {
            names.with_bytes(row.person, |id| -> Result<()> {
                plain.extend_from_slice(&i32::try_from(id.len())?.to_le_bytes());
                plain.extend_from_slice(id);
                Ok(())
            })?;
        }
        let compressed = encoder.compress_vec(&plain)?;
        let n = plain.len();
        let page = Page::DataPage {
            buf: compressed.into(),
            num_values: batch.len() as u32,
            encoding: Encoding::PLAIN,
            def_level_encoding: Encoding::RLE,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        };
        let page =
            SerializedPageWriter::new(&mut encoded).write_page(CompressedPage::new(page, n))?;
        uncompressed += page.uncompressed_size;
    }
    let bytes = Bytes::from(encoded.into_inner()?);
    let metadata = ColumnChunkMetaData::builder(descriptor)
        .set_encodings(vec![Encoding::PLAIN, Encoding::RLE])
        .set_compression(Compression::SNAPPY)
        .set_num_values(rows.len() as i64)
        .set_data_page_offset(0)
        .set_total_compressed_size(bytes.len() as i64)
        .set_total_uncompressed_size(uncompressed as i64)
        .build()?;
    let close = ColumnCloseResult {
        bytes_written: bytes.len() as u64,
        rows_written: rows.len() as u64,
        metadata,
        bloom_filter: None,
        column_index: None,
        offset_index: None,
    };
    group.append_column(&bytes, close)?;
    Ok(())
}

impl Parquet {
    pub fn new() -> Result<Self> {
        Ok(Self {
            users: Arc::new(parse_message_type(USERS)?),
            campaigns: Arc::new(parse_message_type(CAMPAIGNS)?),
            properties: Arc::new(
                WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .set_statistics_enabled(EnabledStatistics::None)
                    .build(),
            ),
        })
    }

    fn writer(
        &self,
        path: &Path,
        schema: &TypePtr,
    ) -> Result<SerializedFileWriter<BufWriter<File>>> {
        let file = BufWriter::new(File::create(path)?);
        Ok(SerializedFileWriter::new(
            file,
            schema.clone(),
            self.properties.clone(),
        )?)
    }

    pub fn users(&self, rows: &[UserRow<'_>], names: &Names, path: &Path) -> Result<()> {
        let mut file = self.writer(path, &self.users)?;
        if !rows.is_empty() {
            let user = file.schema_descr().column(0);
            let mut group = file.next_row_group()?;
            user_ids(&mut group, user, rows, names)?;
            column::<Int32Type, _>(&mut group, rows.iter().map(|r| r.day), None)?;
            column::<Int64Type, _>(&mut group, rows.iter().map(|r| r.sessions), None)?;
            column::<Int64Type, _>(&mut group, rows.iter().map(|r| r.events), None)?;
            column::<Int64Type, _>(&mut group, rows.iter().map(|r| r.revenue), None)?;
            let levels: Vec<_> = rows
                .iter()
                .map(|row| i16::from(row.category.is_some()))
                .collect();
            column::<ByteArrayType, _>(
                &mut group,
                rows.iter().filter_map(|r| r.category.map(bytes)),
                Some(&levels),
            )?;
            group.close()?;
        }
        file.close()?;
        Ok(())
    }

    pub fn campaigns(
        &self,
        days: CampaignDays,
        names: &[(Id<'_>, Id<'_>)],
        path: &Path,
    ) -> Result<()> {
        struct Row<'a> {
            name: Id<'a>,
            channel: Id<'a>,
            day: i32,
            tally: CampaignTally,
        }
        let mut rows = Vec::new();
        for (day, tallies) in days {
            for (tally, &(name, channel)) in tallies.into_iter().zip(names) {
                if tally.sessions != 0 || tally.assists != 0 {
                    rows.push(Row {
                        name,
                        channel,
                        day,
                        tally,
                    });
                }
            }
        }
        let mut file = self.writer(path, &self.campaigns)?;
        if !rows.is_empty() {
            let mut group = file.next_row_group()?;
            column::<ByteArrayType, _>(&mut group, rows.iter().map(|r| bytes(r.name)), None)?;
            column::<ByteArrayType, _>(&mut group, rows.iter().map(|r| bytes(r.channel)), None)?;
            column::<Int32Type, _>(&mut group, rows.iter().map(|r| r.day), None)?;
            column::<Int64Type, _>(&mut group, rows.iter().map(|r| r.tally.sessions), None)?;
            column::<Int64Type, _>(&mut group, rows.iter().map(|r| r.tally.purchasing), None)?;
            column::<Int64Type, _>(&mut group, rows.iter().map(|r| r.tally.revenue), None)?;
            column::<Int64Type, _>(&mut group, rows.iter().map(|r| r.tally.assists), None)?;
            group.close()?;
        }
        file.close()?;
        Ok(())
    }
}
