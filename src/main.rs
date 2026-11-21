mod ids;
mod input;
mod output;
mod rollup;

use ahash::RandomState;
use rayon::prelude::*;
use serde_json::Value;
use std::{env, error::Error, fs, io, path::PathBuf, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn stage<T>(name: &str, run: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let result = run();
    println!("{name}: {:.3} s", start.elapsed().as_secs_f64());
    result
}

fn path(variable: &str, fallback: &str) -> PathBuf {
    env::var_os(variable).map_or_else(|| PathBuf::from(fallback), PathBuf::from)
}

fn main() -> Result<()> {
    let workers = env::var("NCPU")
        .map(|s| s.parse())
        .unwrap_or(Ok(8usize))?
        .max(1);
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build_global()?;
    let data = path("DATA_DIR", "/data");
    let output = path("OUTPUT_DIR", "/output");
    let users = output.join("user_daily");
    let campaigns = output.join("campaign_daily");
    fs::create_dir_all(&users)?;
    fs::create_dir_all(&campaigns)?;

    let manifest: Value = serde_json::from_reader(fs::File::open(data.join("manifest.json"))?)?;
    let bound = |name: &str| {
        manifest[name]
            .as_str()
            .and_then(|value| input::timestamp(value.as_bytes()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("invalid {name}")))
    };
    let event_files = input::map_dir(&data.join("events"))?;
    let click_files = input::map_dir(&data.join("clicks"))?;
    let links = input::map_dir(&data.join("links"))?;
    let products = input::map(&data.join("products.csv"))?;
    let campaign_file = input::map(&data.join("campaigns.csv"))?;
    let dimensions = rollup::Dimensions::new(&products, &campaign_file);
    let names = ids::Names::default();
    let people = input::People::new(
        event_files
            .iter()
            .chain(&click_files)
            .chain(&links)
            .map(|f| f.len())
            .sum(),
    );
    let context = rollup::Context {
        people: &people,
        start: bound("window_start")?,
        end: bound("window_end")?,
        workers,
        dimensions: &dimensions,
        names: &names,
        salt: RandomState::new().hash_one(0u64),
    };
    if context.end.abs_diff(context.start) <= u64::from(u32::MAX)
        && dimensions.categories.len() <= 256
    {
        run::<u64>(
            &context,
            event_files,
            click_files,
            links,
            &users,
            &campaigns,
        )
    } else {
        run::<rollup::Wide>(
            &context,
            event_files,
            click_files,
            links,
            &users,
            &campaigns,
        )
    }
}

fn run<'a, P: rollup::Payload>(
    context: &rollup::Context<'a>,
    event_files: Vec<memmap2::Mmap>,
    click_files: Vec<memmap2::Mmap>,
    links: Vec<memmap2::Mmap>,
    users: &std::path::Path,
    campaigns: &std::path::Path,
) -> Result<()> {
    let dimensions = context.dimensions;
    let names = context.names;
    let (aliases, pending) = rayon::join(
        || {
            stage("resolve linked IDs", || {
                input::aliases(&links, names, context.people)
            })
        },
        || stage("parse events", || context.read_events::<P>(&event_files)),
    );
    let (_, (events, clicks)) = rayon::join(
        move || {
            drop(links);
            drop(event_files);
        },
        || {
            rayon::join(
                || {
                    stage("deduplicate and route events", || {
                        context.deduplicate_events(pending, &aliases)
                    })
                },
                || {
                    stage("read and validate clicks", || {
                        context.clicks(&click_files, &aliases)
                    })
                },
            )
        },
    );
    let parquet = output::Parquet::new()?;
    let (_, partials) = rayon::join(
        move || drop(click_files),
        || -> Result<Vec<_>> {
            stage("sessionise, attribute and write users", || {
                let groups: Vec<_> = events.into_iter().zip(clicks).collect();
                groups
                    .into_par_iter()
                    .chunks(rollup::USER_GROUPS / input::PARTITIONS)
                    .enumerate()
                    .map(|(partition, groups)| {
                        let mut rows = Vec::new();
                        let mut days = rollup::CampaignDays::new();
                        for (events, clicks) in groups {
                            rollup::aggregate(
                                events,
                                context.start,
                                clicks,
                                dimensions,
                                context.people,
                                names,
                                &mut rows,
                                &mut days,
                            );
                        }
                        if partition == 0 || !rows.is_empty() {
                            parquet.users(
                                &rows,
                                names,
                                &users.join(format!("part-{partition:03}.parquet")),
                            )?;
                        }
                        Ok(days)
                    })
                    .collect()
            })
        },
    );
    let mut days = rollup::CampaignDays::new();
    for partial in partials? {
        for (day, values) in partial {
            let total = days
                .entry(day)
                .or_insert_with(|| vec![rollup::CampaignTally::default(); values.len()]);
            for (target, source) in total.iter_mut().zip(values) {
                target.add(source);
            }
        }
    }
    stage("write campaigns", || {
        parquet.campaigns(
            days,
            &dimensions.campaigns,
            &campaigns.join("part-0.parquet"),
        )
    })?;
    Ok(())
}
