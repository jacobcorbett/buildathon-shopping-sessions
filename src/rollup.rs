use crate::ids::{Key, Names};
use crate::input::{self, DAY, Id, PARTITIONS, Shards, WEEK};
use ahash::{AHashMap, RandomState};
use memmap2::Mmap;
use rayon::prelude::*;

pub const USER_GROUPS: usize = PARTITIONS * 8;
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

pub trait Payload: Copy + Send + Sync {
    fn encode(time: i64, price: u32, category: u32, origin: i64) -> Self;
    fn sort(items: &mut [Self]);
    fn time(self, origin: i64) -> i64;
    fn price(self) -> u32;
    fn category(self) -> u32;
}

#[derive(Clone, Copy)]
pub struct Wide {
    time: i64,
    price: u32,
    category: u32,
}

impl Payload for Wide {
    fn sort(items: &mut [Self]) {
        items.sort_unstable_by_key(|item| item.time);
    }
    fn encode(time: i64, price: u32, category: u32, _: i64) -> Self {
        Self {
            time,
            price,
            category,
        }
    }
    fn time(self, _: i64) -> i64 {
        self.time
    }
    fn price(self) -> u32 {
        self.price
    }
    fn category(self) -> u32 {
        self.category
    }
}

// The caller checks the window and category count. Valid prices fit in
// 24 bits by the track rules. Invalid records have a zero person key.
impl Payload for u64 {
    fn sort(items: &mut [Self]) {
        items.sort_unstable();
    }
    fn encode(time: i64, price: u32, category: u32, origin: i64) -> Self {
        ((time.wrapping_sub(origin) as u64) << 32) | (u64::from(category) << 24) | u64::from(price)
    }
    fn time(self, origin: i64) -> i64 {
        origin + (self >> 32) as i64
    }
    fn price(self) -> u32 {
        self as u32 & 0xff_ffff
    }
    fn category(self) -> u32 {
        (self as u32) >> 24
    }
}

#[derive(Clone, Copy)]
pub struct Event<P> {
    person: Key,
    payload: P,
}

#[derive(Clone, Copy)]
pub struct PendingEvent<P> {
    id: Key,
    ingest: i64,
    event: Event<P>,
}

#[derive(Clone, Copy)]
pub struct Click<'a> {
    person: Key,
    time: i64,
    id: Id<'a>,
    campaign: usize,
}

pub struct Context<'a> {
    pub people: &'a input::People,
    pub start: i64,
    pub end: i64,
    pub workers: usize,
    pub dimensions: &'a Dimensions<'a>,
    pub names: &'a Names<'a>,
    pub hash: RandomState,
}

impl<'a> Context<'a> {
    #[inline]
    fn partition(&self, id: Key) -> usize {
        self.hash.hash_one(id) as usize & (PARTITIONS - 1)
    }

    #[inline]
    fn parse_event<P: Payload>(&self, line: &'a [u8]) -> Option<(usize, PendingEvent<P>)> {
        let [id, user, ts, ingest, kind, product, price] = input::fields(line)?;
        if id.is_empty() {
            return None;
        }
        let ingest = input::timestamp(ingest)?;
        let mut data = Wide {
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
                    b"view" | b"cart" => data.time = time,
                    b"purchase" => {
                        if let Some(price) =
                            input::number(price).filter(|value| (1..=10_000_000).contains(value))
                        {
                            data.time = time;
                            data.price = price as u32;
                            data.category = self
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
        let event = Event {
            person: if data.time != INVALID_TIME {
                self.names.key(user)
            } else {
                0
            },
            payload: P::encode(data.time, data.price, data.category, self.start),
        };
        let id = self.names.key(id);
        Some((self.partition(id), PendingEvent { id, ingest, event }))
    }

    pub fn read_events<P: Payload>(&self, files: &'a [Mmap]) -> Vec<Shards<PendingEvent<P>>> {
        input::route(
            files,
            self.workers,
            72,
            PARTITIONS,
            |line| self.parse_event(line),
            |batch| {
                for (_, item) in &*batch {
                    self.people.prefetch(item.event.person);
                }
                for (_, item) in batch {
                    if item.event.person != 0 {
                        item.event.person = self.people.person(item.event.person) + 1;
                    }
                }
            },
        )
    }

    pub fn deduplicate_events<P: Payload>(
        &self,
        by_id: Vec<Shards<PendingEvent<P>>>,
        aliases: &input::Aliases,
    ) -> Vec<Shards<Event<P>>> {
        let by_person = by_id
            .into_par_iter()
            .map(|chunks| {
                let capacity: usize = chunks.iter().map(Vec::len).sum();
                // Two bitmaps identify hash slots hit more than once. Only
                // those records need the exact-key table and ingest comparison.
                let slots = (capacity * 8 + 1).next_power_of_two().max(64);
                let mask = slots - 1;
                let mut seen = vec![0u64; slots / 64];
                let mut repeated = vec![0u64; slots / 64];
                let slot = |key| (self.hash.hash_one(key) as usize / PARTITIONS) & mask;
                for item in chunks.iter().flatten() {
                    let pos = slot(item.id);
                    let bit = 1u64 << (pos % 64);
                    repeated[pos / 64] |= seen[pos / 64] & bit;
                    seen[pos / 64] |= bit;
                }
                let mut shards: Shards<Event<P>> = (0..USER_GROUPS)
                    .map(|_| Vec::with_capacity(capacity / USER_GROUPS))
                    .collect();
                let mut emit = |mut event: Event<P>| {
                    if event.person != 0 {
                        event.person = aliases.person(event.person - 1);
                        shards[event.person as usize % USER_GROUPS].push(event);
                    }
                };
                let mut winners = AHashMap::with_capacity(capacity / 8);
                for chunk in &chunks {
                    for batch in chunk.chunks(32) {
                        for item in batch {
                            aliases.prefetch(item.event.person.wrapping_sub(1));
                        }
                        for item in batch {
                            let pos = slot(item.id);
                            if repeated[pos / 64] & (1u64 << (pos % 64)) == 0 {
                                emit(item.event);
                            } else {
                                let winner = winners.entry(item.id).or_insert(item);
                                if item.ingest < winner.ingest {
                                    *winner = item;
                                }
                            }
                        }
                    }
                }
                for winner in winners.into_values() {
                    emit(winner.event);
                }
                shards
            })
            .collect();
        input::transpose(by_person, USER_GROUPS)
    }

    pub fn clicks(&self, files: &'a [Mmap], aliases: &input::Aliases) -> Vec<Shards<Click<'a>>> {
        input::route(
            files,
            self.workers,
            48,
            USER_GROUPS,
            |line| {
                let [id, user, campaign, ts] = input::fields(line)?;
                if id.is_empty() || user.is_empty() {
                    return None;
                }
                let &campaign = self.dimensions.campaign_index.get(campaign)?;
                let time = input::timestamp(ts)?;
                if time < self.start - WEEK || time >= self.end {
                    return None;
                }
                let user = self.names.key(user);
                let person = user;
                // Repeated click IDs have identical values. Keeping copies cannot
                // change the winning click or whether a campaign is active.
                Some((
                    person as usize % USER_GROUPS,
                    Click {
                        person,
                        time,
                        id,
                        campaign,
                    },
                ))
            },
            |batch| {
                for (_, click) in &*batch {
                    self.people.prefetch(click.person);
                }
                for (part, click) in batch {
                    click.person = aliases.person(self.people.person(click.person));
                    *part = click.person as usize % USER_GROUPS;
                }
            },
        )
    }
}

pub struct UserRow<'a> {
    pub person: Key,
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

fn sessionise<'a, P: Payload>(
    person: Key,
    events: &[P],
    origin: i64,
    categories: &[Id<'a>],
    category_totals: &mut [i64],
    sessions: &mut Vec<Session>,
    user_rows: &mut Vec<UserRow<'a>>,
) {
    let mut session = Session {
        start: events[0].time(origin),
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
        if event.time(origin) - previous > SESSION_GAP {
            sessions.push(session);
            session = Session {
                start: event.time(origin),
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
        previous = event.time(origin);
        day_row.events += 1;
        if event.price() != 0 {
            let price = i64::from(event.price());
            session.revenue += price;
            day_row.revenue += price;
            category_totals[event.category() as usize] += price;
        }
    }
    sessions.push(session);
    day_row.category = top_category(category_totals, categories);
    user_rows.push(day_row);
    category_totals.fill(0);
}

#[derive(Default)]
struct Group {
    person: Key,
    n_events: usize,
    n_clicks: usize,
    event_end: usize,
    click_end: usize,
}

pub fn aggregate<'a, P: Payload>(
    events: Shards<Event<P>>,
    origin: i64,
    clicks: Shards<Click<'a>>,
    dimensions: &Dimensions<'a>,
    people: &input::People,
    rows: &mut Vec<UserRow<'a>>,
    days: &mut CampaignDays,
) {
    let count: usize = events.iter().map(Vec::len).sum();
    if count == 0 {
        return;
    }
    let mut groups: Vec<Group> = Vec::new();
    // All handles in a shard share their low bits. The quotient directly
    // indexes each person without a second hash or a comparison sort.
    let mut lookup = vec![usize::MAX; people.direct_len().div_ceil(USER_GROUPS)];
    let mut event_groups = Vec::with_capacity(count);
    for event in events.iter().flatten() {
        let slot = event.person as usize / USER_GROUPS;
        if slot >= lookup.len() {
            lookup.resize(slot + 1, usize::MAX);
        }
        let id = if lookup[slot] != usize::MAX {
            lookup[slot]
        } else {
            let id = groups.len();
            groups.push(Group {
                person: people.key(event.person),
                ..Group::default()
            });
            lookup[slot] = id;
            id
        };
        groups[id].n_events += 1;
        event_groups.push(id);
    }
    let mut click_groups = Vec::new();
    for click in clicks.iter().flatten() {
        let id = lookup
            .get(click.person as usize / USER_GROUPS)
            .copied()
            .filter(|&id| id != usize::MAX);
        if let Some(i) = id {
            groups[i].n_clicks += 1;
        }
        click_groups.push(id.unwrap_or(usize::MAX));
    }
    let (mut event_offset, mut click_offset) = (0, 0);
    for group in &mut groups {
        group.event_end = event_offset;
        event_offset += group.n_events;
        group.click_end = click_offset;
        click_offset += group.n_clicks;
    }
    let mut sorted_events = vec![P::encode(origin, 0, 0, origin); event_offset];
    for (event, id) in events.into_iter().flatten().zip(event_groups) {
        let end = &mut groups[id].event_end;
        sorted_events[*end] = event.payload;
        *end += 1;
    }
    let mut sorted_clicks = vec![
        Click {
            person: 0,
            time: 0,
            id: &[],
            campaign: 0
        };
        click_offset
    ];
    for (click, id) in clicks.into_iter().flatten().zip(click_groups) {
        if id != usize::MAX {
            let end = &mut groups[id].click_end;
            sorted_clicks[*end] = click;
            *end += 1;
        }
    }
    let mut sessions = Vec::new();
    let mut totals = vec![0; dimensions.categories.len()];
    let mut attribution = Attribution::new(dimensions.campaigns.len());
    for group in groups {
        let events = &mut sorted_events[group.event_end - group.n_events..group.event_end];
        let clicks = &mut sorted_clicks[group.click_end - group.n_clicks..group.click_end];
        P::sort(events);
        clicks.sort_unstable_by(|a, b| a.time.cmp(&b.time).then_with(|| b.id.cmp(a.id)));
        sessionise(
            group.person,
            events,
            origin,
            &dimensions.categories,
            &mut totals,
            &mut sessions,
            rows,
        );
        attribution.attribute(&sessions, clicks, days);
        sessions.clear();
    }
}
