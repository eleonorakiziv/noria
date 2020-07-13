use chickenize::Chickenize;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use noria::{Modification, Operation, TableOperation};
use prelude::*;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use vec_map::VecMap;

/// Base is used to represent the root nodes of the Noria data flow graph.
///
/// These nodes perform no computation, and their job is merely to persist all received updates and
/// forward them to interested downstream operators. A base node should only be sent updates of the
/// type corresponding to the node's type.
#[derive(Debug, Serialize, Deserialize)]
pub struct Base {
    primary_key: Option<Vec<usize>>,
    defaults: Vec<DataType>,
    dropped: Vec<usize>,
    unmodified: bool,
    on_remove: OnRemove,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum OnRemove {
    Anonymize(Vec<usize>),
    Clear,
    // Keep
}

impl Base {
    /// Create a non-durable base node operator.
    pub fn new(defaults: Vec<DataType>) -> Self {
        let mut base = Base::default();
        base.defaults = defaults;
        base
    }

    pub fn new_with_remove_option(on_remove: OnRemove) -> Self {
        let mut base = Base::default();
        base.on_remove = on_remove;
        base
    }

    /// Builder with a known primary key.
    pub fn with_key(mut self, primary_key: Vec<usize>) -> Base {
        self.primary_key = Some(primary_key);
        self
    }

    pub fn key(&self) -> Option<&[usize]> {
        self.primary_key.as_ref().map(|cols| &cols[..])
    }

    /// Add a new column to this base node.
    pub fn add_column(&mut self, default: DataType) -> usize {
        assert!(
            !self.defaults.is_empty(),
            "cannot add columns to base nodes without\
             setting default values for initial columns"
        );
        self.defaults.push(default);
        self.unmodified = false;
        self.defaults.len() - 1
    }

    /// Drop a column from this base node.
    pub fn drop_column(&mut self, column: usize) {
        assert!(
            !self.defaults.is_empty(),
            "cannot add columns to base nodes without\
             setting default values for initial columns"
        );
        assert!(column < self.defaults.len());
        self.unmodified = false;

        // note that we don't need to *do* anything for dropped columns when we receive records.
        // the only thing that matters is that new Mutators remember to inject default values for
        // dropped columns.
        self.dropped.push(column);
    }

    pub fn get_dropped(&self) -> VecMap<DataType> {
        self.dropped
            .iter()
            .map(|&col| (col, self.defaults[col].clone()))
            .collect()
    }

    crate fn fix(&self, row: &mut Vec<DataType>) {
        if self.unmodified {
            return;
        }

        if row.len() != self.defaults.len() {
            let rlen = row.len();
            row.extend(self.defaults.iter().skip(rlen).cloned());
        }
    }
}

/// A Base clone must have a different unique_id so that no two copies write to the same file.
/// Resetting the writer to None in the original copy is not enough to guarantee that, as the
/// original object can still re-open the log file on-demand from Base::persist_to_log.
impl Clone for Base {
    fn clone(&self) -> Base {
        Base {
            primary_key: self.primary_key.clone(),

            defaults: self.defaults.clone(),
            dropped: self.dropped.clone(),
            unmodified: self.unmodified.clone(),
            on_remove: self.on_remove.clone(),
        }
    }
}

impl Default for Base {
    fn default() -> Self {
        Base {
            primary_key: None,

            defaults: Vec::new(),
            dropped: Vec::new(),
            unmodified: true,
            on_remove: OnRemove::Clear,
        }
    }
}

fn key_val(i: usize, col: usize, r: &TableOperation) -> &DataType {
    match *r {
        TableOperation::Insert(ref row) => &row[col],
        TableOperation::Delete { ref key } => &key[i],
        TableOperation::Update { ref key, .. } => &key[i],
        TableOperation::InsertOrUpdate { ref row, .. } => &row[col],
    }
}

fn key_of<'a>(key_cols: &'a [usize], r: &'a TableOperation) -> impl Iterator<Item = &'a DataType> {
    key_cols
        .iter()
        .enumerate()
        .map(move |(i, col)| key_val(i, *col, r))
}

impl Base {
    pub(in crate::node) fn take(&mut self) -> Self {
        Clone::clone(self)
    }

    pub(in crate::node) fn process(
        &mut self,
        us: LocalNodeIndex,
        mut ops: Vec<TableOperation>,
        state: &StateMap,
    ) -> Records {
        if self.primary_key.is_none() || ops.is_empty() {
            return ops
                .into_iter()
                .map(|r| {
                    if let TableOperation::Insert(mut r) = r {
                        self.fix(&mut r);
                        Record::Positive(r)
                    } else if let TableOperation::Delete { key } = r {
                        Record::Negative(key)
                    } else {
                        unreachable!("unkeyed base got non-insert operation {:?}", r);
                    }
                })
                .collect();
        }

        let key_cols = &self.primary_key.as_ref().unwrap()[..];
        ops.sort_by(|a, b| key_of(key_cols, a).cmp(key_of(key_cols, b)));

        // starting key
        let mut this_key: Vec<_> = key_of(key_cols, &ops[0]).cloned().collect();

        // starting record state
        let db = state
            .get(us)
            .expect("base with primary key must be materialized");

        let get_current = |current_key: &'_ _| {
            match db.lookup(key_cols, &KeyType::from(current_key)) {
                LookupResult::Some(rows) => {
                    match rows.len() {
                        0 => None,
                        1 => rows.into_iter().next(),
                        n => {
                            // primary key, so better be unique!
                            assert_eq!(n, 1, "key {:?} not unique (n = {})!", current_key, n);
                            unreachable!();
                        }
                    }
                }
                LookupResult::Missing => unreachable!(),
            }
        };
        let mut current = get_current(&this_key);
        let mut was = current.clone();

        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            if this_key.iter().cmp(key_of(key_cols, &op)) != Ordering::Equal {
                if current != was {
                    if let Some(was) = was {
                        results.push(Record::Negative(was.into_owned()));
                    }
                    if let Some(current) = current {
                        results.push(Record::Positive(current.into_owned()));
                    }
                }

                this_key = key_of(key_cols, &op).cloned().collect();
                current = get_current(&this_key);
                was = current.clone();
            }

            let update = match op {
                TableOperation::Insert(row) => {
                    if let Some(ref was) = was {
                        eprintln!("base ignoring {:?} since it already has {:?}", row, was);
                    } else {
                        //assert!(was.is_none());
                        current = Some(Cow::Owned(row));
                    }
                    continue;
                }
                TableOperation::Delete { .. } => {
                    if current.is_some() {
                        current = None;
                    } else {
                        // supposed to delete a non-existing row?
                        // TODO: warn?
                    }
                    continue;
                }
                TableOperation::Update { set, .. } => set,
                TableOperation::InsertOrUpdate { row, update } => {
                    if current.is_none() {
                        current = Some(Cow::Owned(row));
                        continue;
                    }
                    update
                }
            };

            if current.is_none() {
                // supposed to update a non-existing row?
                // TODO: also warn here?
                continue;
            }

            let mut future = current.unwrap().into_owned();
            for (col, op) in update.into_iter().enumerate() {
                // XXX: make sure user doesn't update primary key?
                match op {
                    Modification::Set(v) => future[col] = v,
                    Modification::Apply(op, v) => {
                        let old: i128 = future[col].clone().into();
                        let delta: i128 = v.into();
                        future[col] = match op {
                            Operation::Add => (old + delta).into(),
                            Operation::Sub => (old - delta).into(),
                        };
                    }
                    Modification::None => {}
                }
            }
            current = Some(Cow::Owned(future));
        }

        // we may have changed things in the last iteration of the loop above
        if current != was {
            if let Some(was) = was {
                results.push(Record::Negative(was.into_owned()));
            }
            if let Some(current) = current {
                results.push(Record::Positive(current.into_owned()));
            }
        }

        for r in &mut results {
            self.fix(r);
        }

        results.into()
    }

    pub(in crate::node) fn suggest_indexes(&self, n: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        if self.primary_key.is_some() {
            Some((n, self.primary_key.as_ref().unwrap().clone()))
                .into_iter()
                .collect()
        } else {
            HashMap::new()
        }
    }
    pub(in crate::node) fn notify_leave(
        &mut self,
        us: LocalNodeIndex,
        state: &StateMap,
    ) -> (Vec<TableOperation>, Vec<TableOperation>) {
        let db = state
            .get(us)
            .expect("base with primary key must be materialized");
        let mut negatives: Vec<Record> = db
            .cloned_records()
            .into_iter()
            .map(|row| Record::Negative(row))
            .collect();

        let mut positives = Vec::new();

        match &self.on_remove {
            OnRemove::Anonymize(to_anonymize) => {
                for row in negatives.clone().into_iter() {
                    let (mut vec, _pos) = row.extract();

                    for c in 0..vec.len() {
                        if to_anonymize.contains(&c) {
                            if vec[c].is_integer() {
                                vec[c] = 0.into();
                            } else if vec[c].is_string() {
                                let curr: String = (&vec[c]).clone().into();
                                vec[c] = curr.clone().chicken().into();
                            } else if vec[c].is_datetime() {
                                let default = NaiveDateTime::new(
                                    NaiveDate::from_ymd(1970, 1, 1),
                                    NaiveTime::from_hms_milli(0, 0, 0, 0),
                                );
                                vec[c] = DataType::Timestamp(default);
                            } else {
                                unimplemented!(
                                    "only strings, integers and timestamps support anonymization"
                                );
                            }
                        }
                    }
                    positives.push(vec);
                }
            }
            _ => {}
        }
        let mut p_key: Vec<usize> = Vec::new();
        match &self.primary_key {
            Some(k) => p_key = (*k).clone(),
            None => {}
        }

        let mut neg_ops: Vec<TableOperation> = Vec::new();
        for row in negatives.iter_mut() {
            if p_key.is_empty() {
                // if there is no key, include all the cols
                p_key = (0..row.rec().len()).collect();
            }
            let keys_vec: Vec<DataType> = row
                .rec()
                .into_iter()
                .enumerate()
                .filter(|&(i, _)| p_key.contains(&i))
                .map(|(_, v)| v.clone())
                .collect();
            neg_ops.push(TableOperation::Delete { key: keys_vec });
        }

        let pos_ops: Vec<TableOperation> = positives
            .into_iter()
            .map(|r| TableOperation::Insert(r.to_vec()))
            .collect();
        (neg_ops, pos_ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works_default() {
        let b = Base::default();

        assert!(b.primary_key.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    #[test]
    fn it_works_new() {
        let b = Base::new(vec![]);

        assert!(b.primary_key.is_none());

        assert_eq!(b.defaults.len(), 0);
        assert_eq!(b.dropped.len(), 0);
        assert_eq!(b.unmodified, true);
    }

    fn test_lots_of_changes_in_same_batch(mut state: Box<dyn State>) {
        use node;
        use prelude::*;

        // most of this is from MockGraph
        let mut graph = Graph::new();
        let source = graph.add_node(Node::new(
            "source",
            &["because-type-inference"],
            node::NodeType::Source,
        ));

        let b = Base::new(vec![]).with_key(vec![0, 2]);
        let global = graph.add_node(Node::new("b", &["x", "y", "z"], b));
        graph.add_edge(source, global, ());
        let local = unsafe { LocalNodeIndex::make(0 as u32) };
        let mut ip: IndexPair = global.into();
        ip.set_local(local);
        graph
            .node_weight_mut(global)
            .unwrap()
            .set_finalized_addr(ip);

        let mut remap = HashMap::new();
        remap.insert(global, ip);
        graph.node_weight_mut(global).unwrap().on_commit(&remap);
        graph.node_weight_mut(global).unwrap().add_to(0.into());

        for (_, col) in graph[global].suggest_indexes(global) {
            state.add_key(&col[..], None);
        }

        let mut states = StateMap::new();
        states.insert(local, state);
        let n = graph[global].take();
        let mut n = n.finalize(&graph);

        let mut one = move |u: Vec<TableOperation>| {
            let mut m = n.get_base_mut().unwrap().process(local, u, &states);
            node::materialize(&mut m, None, states.get_mut(local));
            m
        };

        assert_eq!(
            one(vec![
                TableOperation::Insert(vec![1.into(), "a".into(), 1.into()]),
                TableOperation::Insert(vec![2.into(), "2a".into(), 1.into()]),
                TableOperation::Delete {
                    key: vec![1.into(), 1.into()],
                },
                TableOperation::Insert(vec![1.into(), "b".into(), 1.into()]),
                TableOperation::InsertOrUpdate {
                    row: vec![1.into(), "c".into(), 1.into()],
                    update: vec![
                        Modification::None,
                        Modification::Set("never".into()),
                        Modification::None,
                    ],
                },
                TableOperation::InsertOrUpdate {
                    row: vec![1.into(), "also never".into(), 1.into()],
                    update: vec![
                        Modification::None,
                        Modification::Set("d".into()),
                        Modification::None,
                    ],
                },
                TableOperation::Update {
                    key: vec![1.into(), 1.into()],
                    set: vec![
                        Modification::None,
                        Modification::Set("e".into()),
                        Modification::None,
                    ],
                },
                TableOperation::Update {
                    key: vec![2.into(), 1.into()],
                    set: vec![
                        Modification::None,
                        Modification::Set("2x".into()),
                        Modification::None,
                    ],
                },
                TableOperation::Delete {
                    key: vec![1.into(), 1.into()],
                },
                TableOperation::Delete {
                    key: vec![2.into(), 1.into()],
                },
            ]),
            Records::default()
        );
    }

    #[test]
    fn lots_of_changes_in_same_batch() {
        let state = MemoryState::default();
        test_lots_of_changes_in_same_batch(box state);
    }

    #[test]
    fn lots_of_changes_in_same_batch_persistent() {
        let state = PersistentState::new(
            String::from("lots_of_changes_in_same_batch_persistent"),
            None,
            &PersistenceParameters::default(),
        );

        test_lots_of_changes_in_same_batch(box state);
    }
}
