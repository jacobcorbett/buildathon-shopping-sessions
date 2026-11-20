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

    pub fn users(&self, rows: &[UserRow<'_>], names: &Names<'_>, path: &Path) -> Result<()> {
        let mut file = self.writer(path, &self.users)?;
        if !rows.is_empty() {
            let mut group = file.next_row_group()?;
            column::<ByteArrayType, _>(
                &mut group,
                rows.iter().map(|r| ByteArray::from(names.bytes(r.person))),
                None,
            )?;
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
