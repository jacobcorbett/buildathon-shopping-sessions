use crate::ids::{Key, Names};
use crate::input::{self, DAY, Id, PARTITIONS, Shards, WEEK};
use ahash::AHashMap;
use memmap2::Mmap;
use rayon::prelude::*;

pub const USER_GROUPS: usize = PARTITIONS * 2;
const ID_PARTS: usize = 1024;
// Keep nearby handles together so reading original user IDs is cache-local.
const PERSON_TILE: usize = 64;

#[inline]
fn person_group(id: Key) -> usize {
    (id as usize / PERSON_TILE) % USER_GROUPS
}
#[inline]
fn person_slot(id: Key) -> usize {
    (id as usize / (USER_GROUPS * PERSON_TILE)) * PERSON_TILE + (id as usize % PERSON_TILE)
}
#[inline]
fn group_person(slot: usize, group: usize) -> Key {
    ((slot / PERSON_TILE * USER_GROUPS + group) * PERSON_TILE + slot % PERSON_TILE) as Key
}
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
    // True when every validated event has an unsigned 32-bit offset from the window start.
    const COMPACT_TIME: bool;
    type Click: ClickData;
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
    const COMPACT_TIME: bool = false;
    type Click = WideClick;
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
    const COMPACT_TIME: bool = true;
    type Click = CompactClick;
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

// Grouped, deduplicated events awaiting the per-person scatter.
// Purchases and large handles retain their full payload in the exact array.
pub struct EventShard<P> {
    rows: Vec<u64>,
    exact: Vec<Event<P>>,
}
impl<P: Payload> EventShard<P> {
    fn new(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(if P::COMPACT_TIME { capacity } else { 0 }),
            exact: Vec::with_capacity(if P::COMPACT_TIME {
                capacity / 24
            } else {
                capacity
            }),
        }
    }
    fn len(&self) -> usize {
        self.rows.len() + self.exact.len()
    }
    #[inline(always)]
    fn push(&mut self, event: Event<P>, origin: i64) {
        if P::COMPACT_TIME && event.person <= Key::from(u32::MAX) && event.payload.price() == 0 {
            let elapsed = event.payload.time(origin).wrapping_sub(origin) as u32;
            self.rows.push((event.person << 32) | u64::from(elapsed));
        } else {
            self.exact.push(event);
        }
    }
    fn first(&self) -> Option<Key> {
        self.rows
            .first()
            .map(|r| r >> 32)
            .or_else(|| self.exact.first().map(|e| e.person))
    }
    fn people(&self, mut visit: impl FnMut(Key)) {
        for &row in &self.rows {
            visit(row >> 32);
        }
        for event in &self.exact {
            visit(event.person);
        }
    }
    fn unpack(self, origin: i64, mut visit: impl FnMut(Event<P>)) {
        for row in self.rows {
            visit(Event {
                person: row >> 32,
                payload: P::encode(origin + i64::from(row as u32), 0, 0, origin),
            });
        }
        for event in self.exact {
            visit(event);
        }
    }
}

#[derive(Clone, Copy)]
pub struct PendingEvent<P> {
    id: Key,
    ingest: i64,
    event: Event<P>,
}

pub trait ClickData: Copy + Send + Sync + Default {
    fn new(id: Key, time: i64, campaign: usize, origin: i64) -> Self;
    fn id(self) -> Key;
    fn time(self, origin: i64) -> i64;
    fn campaign(self) -> usize;
}
#[derive(Clone, Copy, Default)]
pub struct WideClick {
    id: Key,
    time: i64,
    campaign: usize,
}
impl ClickData for WideClick {
    fn new(id: Key, time: i64, campaign: usize, _: i64) -> Self {
        Self { id, time, campaign }
    }
    fn id(self) -> Key {
        self.id
    }
    fn time(self, _: i64) -> i64 {
        self.time
    }
    fn campaign(self) -> usize {
        self.campaign
    }
}
#[derive(Clone, Copy, Default)]
pub struct CompactClick {
    id: Key,
    stamp: u64,
}
impl ClickData for CompactClick {
    fn new(id: Key, time: i64, campaign: usize, origin: i64) -> Self {
        Self {
            id,
            stamp: (((time - origin) as u64) << 32) | campaign as u64,
        }
    }
    fn id(self) -> Key {
        self.id
    }
    fn time(self, origin: i64) -> i64 {
        origin + (self.stamp >> 32) as i64
    }
    fn campaign(self) -> usize {
        self.stamp as u32 as usize
    }
}
#[derive(Clone, Copy)]
pub struct Click<C: ClickData> {
    person: Key,
    data: C,
}

pub struct Context<'a> {
    pub people: &'a input::People,
    pub start: i64,
    pub end: i64,
    pub workers: usize,
    pub dimensions: &'a Dimensions<'a>,
    pub names: &'a Names,
    pub salt: Key,
}

impl<'a> Context<'a> {
    #[inline]
    fn partition(&self, id: Key) -> usize {
        id as usize & (ID_PARTS - 1)
    }

    #[inline(always)]
    fn parse_event<P: Payload>(&self, line: &[u8]) -> Option<(usize, PendingEvent<P>)> {
        let [id, user, ts, ingest, kind, product, price] = input::event_row(line)?;
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
        // A reversible permutation retains exact identity; its bits select
        // a routing partition, filter word and fingerprints.
        let id = (self.names.key(id) ^ self.salt)
            .wrapping_mul(0x9e3779b97f4a7c15)
            .swap_bytes();
        let hint = self.people.hint(event.person);
        Some((
            hint * ID_PARTS + self.partition(id),
            PendingEvent { id, ingest, event },
        ))
    }

    pub fn read_events<P: Payload>(&self, files: &[Mmap]) -> Vec<Shards<PendingEvent<P>>> {
        input::route(
            files,
            self.workers,
            72,
            ID_PARTS,
            |line| self.parse_event(line),
            |batch| {
                for (part, item) in batch {
                    if item.event.person != 0 {
                        item.event.person = self
                            .people
                            .person_hinted(item.event.person, *part / ID_PARTS)
                            + 1;
                    }
                    *part %= ID_PARTS;
                }
            },
        )
    }

    pub fn deduplicate_events<P: Payload>(
        &self,
        by_id: Vec<Shards<PendingEvent<P>>>,
        aliases: &input::Aliases,
    ) -> Vec<Vec<EventShard<P>>> {
        let by_person: Vec<Vec<EventShard<P>>> = by_id
            .into_par_iter()
            .chunks(ID_PARTS / PARTITIONS)
            .map(|buckets| {
                let capacity: usize = buckets.iter().flatten().map(Vec::len).sum();
                let mut shards: Vec<EventShard<P>> = (0..USER_GROUPS)
                    .map(|_| EventShard::new(capacity / USER_GROUPS))
                    .collect();
                let mut emit = |mut event: Event<P>| {
                    if event.person != 0 {
                        event.person = aliases.person(event.person - 1);
                        shards[person_group(event.person)].push(event, self.start);
                    }
                };
                for chunks in buckets {
                    let capacity: usize = chunks.iter().map(Vec::len).sum();

                    let words = (capacity * 8 + 1).next_power_of_two().div_ceil(64);
                    let mut seen = vec![0u64; words];
                    let mut repeated = vec![0u64; words];
                    let position = |key: Key| {
                        let word = (key as usize / ID_PARTS) & (words - 1);
                        let mask = (1u64 << ((key >> 35) & 63))
                            | (1u64 << ((key >> 48) & 63))
                            | (1u64 << ((key >> 57) & 63));
                        (word, mask)
                    };
                    for item in chunks.iter().flatten() {
                        let (i, mask) = position(item.id);
                        let prior = seen[i];
                        if prior & mask == mask {
                            repeated[i] |= mask;
                        }
                        seen[i] = prior | mask;
                    }
                    let mut winners = AHashMap::with_capacity(capacity / 8);
                    for chunk in &chunks {
                        for batch in chunk.chunks(32) {
                            for item in batch {
                                aliases.prefetch(item.event.person.wrapping_sub(1));
                            }
                            for item in batch {
                                let (i, mask) = position(item.id);
                                if repeated[i] & mask != mask {
                                    emit(item.event);
                                } else {
                                    use std::collections::hash_map::Entry;
                                    match winners.entry(item.id) {
                                        Entry::Vacant(e) => {
                                            e.insert(item);
                                        }
                                        Entry::Occupied(mut e) => {
                                            if item.ingest < e.get().ingest {
                                                e.insert(item);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for winner in winners.into_values() {
                        emit(winner.event);
                    }
                }
                shards
            })
            .collect();
        let mut output = input::buckets(USER_GROUPS);
        for task in by_person {
            for (i, part) in task.into_iter().enumerate() {
                if part.len() != 0 {
                    output[i].push(part);
                }
            }
        }
        output
    }

    pub fn clicks<C: ClickData>(
        &self,
        files: &[Mmap],
        aliases: &input::Aliases,
    ) -> Vec<Shards<Click<C>>> {
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
                    self.people.hint(person),
                    Click {
                        person,
                        data: C::new(self.names.key(id), time, campaign, self.start - WEEK),
                    },
                ))
            },
            |batch| {
                for (hint, click) in batch.iter_mut() {
                    click.person = self.people.person_hinted(click.person, *hint);
                    aliases.prefetch(click.person);
                }
                for (part, click) in batch {
                    click.person = aliases.person(click.person);
                    *part = person_group(click.person);
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

    fn attribute<C: ClickData>(
        &mut self,
        sessions: &[Session],
        clicks: &[C],
        days: &mut CampaignDays,
        origin: i64,
    ) {
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
            while upper < clicks.len() && clicks[upper].time(origin) <= session.start {
                let campaign = clicks[upper].campaign();
                let state = &mut self.states[campaign];
                if state.clicks == 0 {
                    state.active_since = index;
                    state.position = self.active.len();
                    self.active.push(campaign);
                }
                state.clicks += 1;
                upper += 1;
            }
            while lower < upper && clicks[lower].time(origin) < session.start - WEEK {
                let campaign = clicks[lower].campaign();
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
                clicks[upper - 1].campaign()
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

#[derive(Default, Clone, Copy)]
struct Group {
    events: usize,
    clicks: usize,
}

pub fn aggregate<'a, P: Payload>(
    events: Vec<EventShard<P>>,
    origin: i64,
    clicks: Shards<Click<P::Click>>,
    dimensions: &Dimensions<'a>,
    people: &input::People,
    names: &Names,
    rows: &mut Vec<UserRow<'a>>,
    days: &mut CampaignDays,
) {
    let count: usize = events.iter().map(EventShard::len).sum();
    if count == 0 {
        return;
    }
    // Each shard owns a stripe of tiles. The local slot retains position
    // within its tile. Reuse the per-slot counts as backwards scatter cursors.
    let shard = person_group(events.iter().find_map(EventShard::first).unwrap());
    let mut groups = vec![
        Group::default();
        people.direct_len().div_ceil(USER_GROUPS * PERSON_TILE) * PERSON_TILE
    ];
    for part in &events {
        part.people(|person| {
            let slot = person_slot(person);
            if slot >= groups.len() {
                groups.resize(slot + 1, Group::default());
            }
            groups[slot].events += 1;
        });
    }
    for click in clicks.iter().flatten() {
        if let Some(group) = groups.get_mut(person_slot(click.person)) {
            group.clicks += 1;
        }
    }
    let (mut event_offset, mut click_offset) = (0, 0);
    for group in &mut groups {
        event_offset += group.events;
        group.events = event_offset;
        click_offset += group.clicks;
        group.clicks = click_offset;
    }
    let mut sorted_events = vec![P::encode(origin, 0, 0, origin); event_offset];
    for part in events {
        part.unpack(origin, |event| {
            let group = &mut groups[person_slot(event.person)];
            group.events -= 1;
            sorted_events[group.events] = event.payload;
        });
    }
    let mut sorted_clicks = vec![P::Click::default(); click_offset];
    for click in clicks.into_iter().flatten() {
        if let Some(group) = groups.get_mut(person_slot(click.person)) {
            group.clicks -= 1;
            sorted_clicks[group.clicks] = click.data;
        }
    }
    groups.push(Group {
        events: event_offset,
        clicks: click_offset,
    });
    let mut sessions = Vec::new();
    let mut totals = vec![0; dimensions.categories.len()];
    let mut attribution = Attribution::new(dimensions.campaigns.len());
    for (slot, pair) in groups.windows(2).enumerate() {
        if pair[0].events == pair[1].events {
            continue;
        }
        let person = people.key(group_person(slot, shard));
        let events = &mut sorted_events[pair[0].events..pair[1].events];
        let clicks = &mut sorted_clicks[pair[0].clicks..pair[1].clicks];
        P::sort(events);
        clicks.sort_unstable_by(|a, b| {
            a.time(origin - WEEK)
                .cmp(&b.time(origin - WEEK))
                .then_with(|| names.compare(b.id(), a.id()))
        });
        sessionise(
            person,
            events,
            origin,
            &dimensions.categories,
            &mut totals,
            &mut sessions,
            rows,
        );
        attribution.attribute(&sessions, clicks, days, origin - WEEK);
        sessions.clear();
    }
}
