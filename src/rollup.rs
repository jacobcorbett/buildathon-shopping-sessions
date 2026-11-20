use crate::input::{self, DAY, Id, PARTITIONS, Shards, WEEK};
use ahash::{AHashMap, RandomState};
use memmap2::Mmap;
use rayon::prelude::*;
use std::cmp::Ordering;

const INVALID_TIME: i64 = i64::MIN;
const SESSION_GAP: i64 = 1_800_000;

pub struct Dimensions<'a> {
    pub categories: Vec<Id<'a>>,
    pub products: AHashMap<Id<'a>, usize>,
    pub unknown: usize,
    pub campaigns: Vec<(Id<'a>, Id<'a>)>,
    pub campaign_index: AHashMap<Id<'a>, usize>,
}

impl<'a> Dimensions<'a> {
    pub fn new(products: &'a [u8], campaigns: &'a [u8]) -> Self {
        let mut categories: Vec<_> = input::lines(products)
            .filter_map(input::fields::<2>)
            .map(|[_, category]| category)
            .chain(std::iter::once(b"unknown".as_slice()))
            .collect();
        categories.sort_unstable();
        categories.dedup();
        let products = input::lines(products)
            .filter_map(input::fields::<2>)
            .map(|[id, category]| (id, categories.binary_search(&category).unwrap()))
            .collect();
        let unknown = categories.binary_search(&b"unknown".as_slice()).unwrap();
        let campaigns: Vec<_> = std::iter::once((b"organic".as_slice(), b"organic".as_slice()))
            .chain(
                input::lines(campaigns)
                    .filter_map(input::fields::<2>)
                    .map(|[id, channel]| (id, channel)),
            )
            .collect();
        let campaign_index = campaigns
            .iter()
            .enumerate()
            .skip(1)
            .map(|(index, &(id, _))| (id, index))
            .collect();
        Self {
            categories,
            products,
            unknown,
            campaigns,
            campaign_index,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Event<'a> {
    person: Id<'a>,
    time: i64,
    price: u32,
    category: u32,
}

struct PendingEvent<'a> {
    id: Id<'a>,
    ingest: i64,
    event: Event<'a>,
}

pub struct Click<'a> {
    person: Id<'a>,
    time: i64,
    id: Id<'a>,
    campaign: usize,
}

pub struct Context<'a> {
    pub start: i64,
    pub end: i64,
    pub workers: usize,
    pub dimensions: &'a Dimensions<'a>,
    pub aliases: &'a AHashMap<Id<'a>, Id<'a>>,
    pub hash: RandomState,
}

impl<'a> Context<'a> {
    #[inline]
    fn partition(&self, id: Id<'a>) -> usize {
        self.hash.hash_one(id) as usize & (PARTITIONS - 1)
    }

    #[inline]
    fn person(&self, user: Id<'a>) -> Id<'a> {
        self.aliases.get(user).copied().unwrap_or(user)
    }

    fn parse_event(&self, line: &'a [u8]) -> Option<(usize, PendingEvent<'a>)> {
        let [id, user, ts, ingest, kind, product, price] = input::fields(line)?;
        if id.is_empty() {
            return None;
        }
        let ingest = input::timestamp(ingest)?;
        let mut event = Event {
            person: user,
            time: INVALID_TIME,
            price: 0,
            category: 0,
        };
        // Preserve the deduplication key even when the payload fails validation.
        // A later valid copy must not replace the earliest invalid copy.
        if !user.is_empty() && !product.is_empty() {
            if let Some(time) = input::timestamp(ts)
                .filter(|&time| time >= self.start && time < self.end && time >= ingest - DAY)
            {
                match kind {
                    b"view" | b"cart" => event.time = time,
                    b"purchase" => {
                        if let Some(price) =
                            input::number(price).filter(|value| (1..=10_000_000).contains(value))
                        {
                            event.time = time;
                            event.price = price as u32;
                            event.category = self
                                .dimensions
                                .products
                                .get(product)
                                .copied()
                                .unwrap_or(self.dimensions.unknown)
                                as u32;
                        }
                    }
                    _ => {}
                }
            }
        }
        Some((self.partition(id), PendingEvent { id, ingest, event }))
    }

    pub fn events(&self, files: &'a [Mmap]) -> Vec<Shards<Event<'a>>> {
        let by_id = input::route(files, self.workers, |line| self.parse_event(line));
        let by_person = by_id
            .into_par_iter()
            .map(|chunks| {
                let capacity = chunks.iter().map(Vec::len).sum();
                let mut winners = AHashMap::with_capacity(capacity);
                for item in chunks.into_iter().flatten() {
                    winners
                        .entry(item.id)
                        .and_modify(|winner: &mut (i64, Event<'a>)| {
                            if item.ingest < winner.0 {
                                *winner = (item.ingest, item.event);
                            }
                        })
                        .or_insert((item.ingest, item.event));
                }
                let mut shards = input::buckets();
                for (_, mut event) in winners.into_values() {
                    if event.time != INVALID_TIME {
                        event.person = self.person(event.person);
                        shards[self.partition(event.person)].push(event);
                    }
                }
                shards
            })
            .collect();
        input::transpose(by_person)
    }

    pub fn clicks(&self, files: &'a [Mmap]) -> Vec<Shards<Click<'a>>> {
        input::route(files, self.workers, |line| {
            let [id, user, campaign, ts] = input::fields(line)?;
            if id.is_empty() || user.is_empty() {
                return None;
            }
            let &campaign = self.dimensions.campaign_index.get(campaign)?;
            let time = input::timestamp(ts)?;
            if time < self.start - WEEK || time >= self.end {
                return None;
            }
            let person = self.person(user);
            // Repeated click IDs have identical values. Keeping copies cannot
            // change the winning click or whether a campaign is active.
            Some((
                self.partition(person),
                Click {
                    person,
                    time,
                    id,
                    campaign,
                },
            ))
        })
    }
}

pub struct UserRow<'a> {
    pub person: Id<'a>,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub category: Option<Id<'a>>,
}

#[derive(Default, Clone, Copy)]
pub struct CampaignTally {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assists: i64,
}

impl CampaignTally {
    pub fn add(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.purchasing += other.purchasing;
        self.revenue += other.revenue;
        self.assists += other.assists;
    }
}

pub type CampaignDays = AHashMap<i32, Vec<CampaignTally>>;

#[derive(Clone, Copy)]
struct Session {
    start: i64,
    revenue: i64,
}

fn day(timestamp: i64) -> i32 {
    timestamp.div_euclid(DAY) as i32
}

#[derive(Clone, Copy, Default)]
struct CampaignState {
    clicks: usize,
    active_since: usize,
    position: usize,
}

struct Attribution {
    states: Vec<CampaignState>,
    active: Vec<usize>,
}

impl Attribution {
    fn new(campaigns: usize) -> Self {
        Self {
            states: vec![CampaignState::default(); campaigns],
            active: Vec::new(),
        }
    }

    fn attribute(&mut self, sessions: &[Session], clicks: &[Click<'_>], days: &mut CampaignDays) {
        let mut lower = 0;
        let mut upper = 0;
        let campaign_count = self.states.len();
        let mut daily = days
            .entry(day(sessions[0].start))
            .or_insert_with(|| vec![CampaignTally::default(); campaign_count]);
        let mut current_day = day(sessions[0].start);
        for (index, session) in sessions.iter().enumerate() {
            // Close assist counts at the UTC session day boundary, then carry
            // forward the campaigns that are still active.
            let session_day = day(session.start);
            if session_day != current_day {
                self.flush(index, daily);
                daily = days
                    .entry(session_day)
                    .or_insert_with(|| vec![CampaignTally::default(); campaign_count]);
                current_day = session_day;
            }
            // Add first, then expire. A campaign that loses an old click and
            // gains a new one between sessions can remain continuously active.
            while upper < clicks.len() && clicks[upper].time <= session.start {
                let campaign = clicks[upper].campaign;
                let state = &mut self.states[campaign];
                if state.clicks == 0 {
                    state.active_since = index;
                    state.position = self.active.len();
                    self.active.push(campaign);
                }
                state.clicks += 1;
                upper += 1;
            }
            while lower < upper && clicks[lower].time < session.start - WEEK {
                let campaign = clicks[lower].campaign;
                let state = &mut self.states[campaign];
                state.clicks -= 1;
                if state.clicks == 0 {
                    daily[campaign].assists += (index - state.active_since) as i64;
                    let position = state.position;
                    self.active.swap_remove(position);
                    if let Some(&moved) = self.active.get(position) {
                        self.states[moved].position = position;
                    }
                }
                lower += 1;
            }
            let winner = if lower < upper {
                clicks[upper - 1].campaign
            } else {
                0
            };
            let tally = &mut daily[winner];
            tally.sessions += 1;
            tally.purchasing += i64::from(session.revenue > 0);
            tally.revenue += session.revenue;
        }
        self.flush(sessions.len(), daily);
        for campaign in self.active.drain(..) {
            self.states[campaign] = CampaignState::default();
        }
    }

    fn flush(&mut self, index: usize, daily: &mut [CampaignTally]) {
        for &campaign in &self.active {
            let state = &mut self.states[campaign];
            daily[campaign].assists += (index - state.active_since) as i64;
            state.active_since = index;
        }
    }
}

fn top_category<'a>(totals: &[i64], names: &[Id<'a>]) -> Option<Id<'a>> {
    let mut best = 0;
    let mut category = None;
    for (index, &total) in totals.iter().enumerate() {
        if total > best {
            best = total;
            category = Some(names[index]);
        }
    }
    category
}

fn sessionise<'a>(
    events: &[Event<'a>],
    categories: &[Id<'a>],
    category_totals: &mut [i64],
    sessions: &mut Vec<Session>,
    user_rows: &mut Vec<UserRow<'a>>,
) {
    let person = events[0].person;
    let mut session = Session {
        start: events[0].time,
        revenue: 0,
    };
    let mut day_row = UserRow {
        person,
        day: day(session.start),
        sessions: 1,
        events: 0,
        revenue: 0,
        category: None,
    };
    let mut previous = session.start;
    for event in events {
        if event.time - previous > SESSION_GAP {
            sessions.push(session);
            session = Session {
                start: event.time,
                revenue: 0,
            };
            let start_day = day(session.start);
            if start_day != day_row.day {
                day_row.category = top_category(category_totals, categories);
                user_rows.push(day_row);
                category_totals.fill(0);
                day_row = UserRow {
                    person,
                    day: start_day,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                    category: None,
                };
            }
            day_row.sessions += 1;
        }
        previous = event.time;
        day_row.events += 1;
        if event.price != 0 {
            let price = i64::from(event.price);
            session.revenue += price;
            day_row.revenue += price;
            category_totals[event.category as usize] += price;
        }
    }
    sessions.push(session);
    day_row.category = top_category(category_totals, categories);
    user_rows.push(day_row);
    category_totals.fill(0);
}

pub fn aggregate<'a>(
    mut events: Vec<Event<'a>>,
    mut clicks: Vec<Click<'a>>,
    dimensions: &Dimensions<'a>,
) -> (Vec<UserRow<'a>>, CampaignDays) {
    events.sort_unstable_by(|a, b| a.person.cmp(b.person).then(a.time.cmp(&b.time)));
    // Reverse click IDs within a timestamp, so the latest element wins ties.
    clicks.sort_unstable_by(|a, b| {
        a.person
            .cmp(b.person)
            .then(a.time.cmp(&b.time))
            .then_with(|| b.id.cmp(a.id))
    });
    let mut click_groups = clicks
        .chunk_by(|left, right| left.person == right.person)
        .peekable();
    let mut user_rows = Vec::new();
    let mut days = CampaignDays::new();
    let mut session_buffer = Vec::new();
    let mut category_totals = vec![0; dimensions.categories.len()];
    let mut attribution = Attribution::new(dimensions.campaigns.len());
    for group in events.chunk_by(|left, right| left.person == right.person) {
        let person = group[0].person;
        let mut matching_clicks = &[][..];
        while let Some(next) = click_groups.peek() {
            match next[0].person.cmp(person) {
                Ordering::Less => {
                    click_groups.next();
                }
                Ordering::Equal => {
                    matching_clicks = click_groups.next().unwrap();
                    break;
                }
                Ordering::Greater => break,
            }
        }
        sessionise(
            group,
            &dimensions.categories,
            &mut category_totals,
            &mut session_buffer,
            &mut user_rows,
        );
        attribution.attribute(&session_buffer, matching_clicks, &mut days);
        session_buffer.clear();
    }
    (user_rows, days)
}
