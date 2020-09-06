use crate::controller::domain_handle::{DomainHandle, DomainShardHandle};
use crate::controller::migrate::materialization::Materializations;
use crate::controller::recipe::Schema;
use crate::controller::schema;
use crate::controller::{ControllerState, Migration, Recipe};
use crate::controller::{Worker, WorkerIdentifier};
use crate::coordination::{CoordinationMessage, CoordinationPayload, DomainDescriptor};
use dataflow::node::special::Base;
use dataflow::node::ParentInfo;
use dataflow::ops::project::Project;
use dataflow::ops::union::Emit;
use dataflow::payload::{ChPermStep, MessagePurpose};
use dataflow::prelude::*;
use dataflow::{node, payload::ControlReplyPacket, prelude::Packet, DomainBuilder, DomainConfig};
use hyper::{self, Method, StatusCode};
use mio::net::TcpListener;
use nom_sql::ColumnSpecification;
use noria::builders::*;
use noria::channel::tcp::{SendError, TcpSender};
use noria::consensus::{Authority, Epoch, STATE_KEY};
use noria::data::{Lease, PermissionsChange};
use noria::debug::stats::{DomainStats, GraphStats, NodeStats};
use noria::prelude::SyncView;
use noria::{ActivationResult, SyncTable};
use petgraph::visit::Bfs;
use slog::Logger;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{cell, io, thread, time};
use tokio::prelude::*;

/// `Controller` is the core component of the alternate Soup implementation.
///
/// It keeps track of the structure of the underlying data flow graph and its domains. `Controller`
/// does not allow direct manipulation of the graph. Instead, changes must be instigated through a
/// `Migration`, which can be performed using `ControllerInner::migrate`. Only one `Migration` can
/// occur at any given point in time.
pub(super) struct ControllerInner {
    pub(super) ingredients: Graph,
    pub(super) source: NodeIndex,
    pub(super) ndomains: usize,
    pub(super) sharding: Option<usize>,

    pub(super) domain_config: DomainConfig,

    /// Parameters for persistence code.
    pub(super) persistence: PersistenceParameters,
    pub(super) materializations: Materializations,

    /// Current recipe
    recipe: Recipe,

    pub(super) domains: HashMap<DomainIndex, DomainHandle>,
    pub(in crate::controller) domain_nodes: HashMap<DomainIndex, Vec<NodeIndex>>,
    pub(super) channel_coordinator: Arc<ChannelCoordinator>,
    pub(super) debug_channel: Option<SocketAddr>,

    /// Map from worker address to the address the worker is listening on for reads.
    read_addrs: HashMap<WorkerIdentifier, SocketAddr>,
    pub(super) workers: HashMap<WorkerIdentifier, Worker>,

    /// State between migrations
    pub(super) remap: HashMap<DomainIndex, HashMap<NodeIndex, IndexPair>>,

    // Keeps track of the highest index in the domain
    pub(super) max_local_index: HashMap<DomainIndex, u32>,

    pub(super) epoch: Epoch,

    pending_recovery: Option<(Vec<String>, usize)>,

    quorum: usize,
    heartbeat_every: Duration,
    healthcheck_every: Duration,
    last_checked_workers: Instant,

    log: slog::Logger,

    pub(in crate::controller) replies: DomainReplies,
    pub global_table: Option<SyncTable>,
    pub global_view: Option<SyncView>,
}

/// Table struct is used to serialize/deserialize table information
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Table {
    name: String,
    fields: Vec<String>,
    ni: NodeIndex,
    primary_key: Option<Vec<usize>>,
    rows: Option<Vec<Vec<DataType>>>,
    resub_key: Option<Vec<usize>>,
    permissions: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Tables {
    t: Vec<Table>,
}

pub(in crate::controller) struct DomainReplies(
    futures::sync::mpsc::UnboundedReceiver<ControlReplyPacket>,
);

impl DomainReplies {
    fn read_n_domain_replies(&mut self, n: usize) -> Vec<ControlReplyPacket> {
        let mut crps = Vec::with_capacity(n);

        // TODO
        // TODO: it's so stupid to spin here now...
        // TODO
        loop {
            match self.0.poll() {
                Ok(Async::NotReady) => thread::yield_now(),
                Ok(Async::Ready(Some(crp))) => {
                    crps.push(crp);
                    if crps.len() == n {
                        return crps;
                    }
                }
                Ok(Async::Ready(None)) => {
                    unreachable!("got unexpected EOF from domain reply channel");
                }
                Err(e) => {
                    unimplemented!("failed to read control reply packet: {:?}", e);
                }
            }
        }
    }

    pub(in crate::controller) fn wait_for_acks(&mut self, d: &DomainHandle) {
        for r in self.read_n_domain_replies(d.shards()) {
            match r {
                ControlReplyPacket::Ack(_) => {}
                r => unreachable!("got unexpected non-ack control reply: {:?}", r),
            }
        }
    }

    fn wait_for_statistics(
        &mut self,
        d: &DomainHandle,
    ) -> Vec<(DomainStats, HashMap<NodeIndex, NodeStats>)> {
        let mut stats = Vec::with_capacity(d.shards());
        for r in self.read_n_domain_replies(d.shards()) {
            match r {
                ControlReplyPacket::Statistics(d, s) => stats.push((d, s)),
                r => unreachable!("got unexpected non-stats control reply: {:?}", r),
            }
        }
        stats
    }
}

pub(super) fn graphviz(
    graph: &Graph,
    detailed: bool,
    materializations: &Materializations,
) -> String {
    let mut s = String::new();

    let indentln = |s: &mut String| s.push_str("    ");

    // header.
    s.push_str("digraph {{\n");

    // global formatting.
    indentln(&mut s);
    if detailed {
        s.push_str("node [shape=record, fontsize=10]\n");
    } else {
        s.push_str("graph [ fontsize=24 fontcolor=\"#0C6fA9\", outputorder=edgesfirst ]\n");
        s.push_str("edge [ color=\"#0C6fA9\", style=bold ]\n");
        s.push_str("node [ color=\"#0C6fA9\", shape=box, style=\"rounded,bold\" ]\n");
    }

    // node descriptions.
    for index in graph.node_indices() {
        let node = &graph[index];
        let materialization_status = materializations.get_status(index, node);
        indentln(&mut s);
        s.push_str(&format!("n{}", index.index()));
        s.push_str(&node.describe(index, detailed, materialization_status));
    }

    // edges.
    for (_, edge) in graph.raw_edges().iter().enumerate() {
        indentln(&mut s);
        s.push_str(&format!(
            "n{} -> n{} [ {} ]",
            edge.source().index(),
            edge.target().index(),
            if graph[edge.source()].is_egress() {
                "color=\"#CCCCCC\""
            } else if graph[edge.source()].is_source() {
                "style=invis"
            } else {
                ""
            }
        ));
        s.push_str("\n");
    }

    // footer.
    s.push_str("}}");

    s
}

impl ControllerInner {
    pub(in crate::controller) fn topo_order(&self, new: &HashSet<NodeIndex>) -> Vec<NodeIndex> {
        let mut topo_list = Vec::with_capacity(new.len());
        let mut topo = petgraph::visit::Topo::new(&self.ingredients);
        while let Some(node) = topo.next(&self.ingredients) {
            if node == self.source {
                continue;
            }
            if self.ingredients[node].is_dropped() {
                continue;
            }
            if !new.contains(&node) {
                continue;
            }
            topo_list.push(node);
        }
        topo_list
    }

    pub(super) fn external_request<A: Authority + 'static>(
        &mut self,
        method: hyper::Method,
        path: String,
        query: Option<String>,
        body: Vec<u8>,
        authority: &Arc<A>,
    ) -> Result<Result<String, String>, StatusCode> {
        use serde_json as json;

        match (&method, path.as_ref()) {
            (&Method::GET, "/simple_graph") => return Ok(Ok(self.graphviz(false))),
            (&Method::POST, "/simple_graphviz") => {
                return Ok(Ok(json::to_string(&self.graphviz(false)).unwrap()));
            }
            (&Method::GET, "/graph") => return Ok(Ok(self.graphviz(true))),
            (&Method::POST, "/graphviz") => {
                return Ok(Ok(json::to_string(&self.graphviz(true)).unwrap()));
            }
            (&Method::GET, "/get_statistics") => {
                return Ok(Ok(json::to_string(&self.get_statistics()).unwrap()));
            }
            (&Method::POST, "/get_statistics") => {
                return Ok(Ok(json::to_string(&self.get_statistics()).unwrap()));
            }
            _ => {}
        }

        if self.pending_recovery.is_some() || self.workers.len() < self.quorum {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }

        match (method, path.as_ref()) {
            (Method::GET, "/flush_partial") => {
                Ok(Ok(json::to_string(&self.flush_partial()).unwrap()))
            }
            (Method::POST, "/inputs") => Ok(Ok(json::to_string(&self.inputs()).unwrap())),
            (Method::POST, "/outputs") => Ok(Ok(json::to_string(&self.outputs()).unwrap())),
            (Method::GET, "/instances") => Ok(Ok(json::to_string(&self.get_instances()).unwrap())),
            (Method::GET, "/nodes") => {
                // TODO(malte): this is a pretty yucky hack, but hyper doesn't provide easy access
                // to individual query variables unfortunately. We'll probably want to factor this
                // out into a helper method.
                let nodes = if let Some(query) = query {
                    let vars: Vec<_> = query.split('&').map(String::from).collect();
                    if let Some(n) = &vars.into_iter().find(|v| v.starts_with("w=")) {
                        self.nodes_on_worker(Some(&n[2..].parse().unwrap()))
                    } else {
                        self.nodes_on_worker(None)
                    }
                } else {
                    // all data-flow nodes
                    self.nodes_on_worker(None)
                };
                Ok(Ok(json::to_string(
                    &nodes
                        .into_iter()
                        .filter_map(|ni| {
                            let n = &self.ingredients[ni];
                            if n.is_internal() {
                                Some((ni, n.name(), n.description(true)))
                            } else if n.is_base() {
                                Some((ni, n.name(), "Base table".to_owned()))
                            } else if n.is_reader() {
                                Some((ni, n.name(), "Leaf view".to_owned()))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap()))
            }
            (Method::POST, "/table_builder") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| Ok(json::to_string(&self.table_builder(args)).unwrap())),
            (Method::POST, "/view_builder") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| Ok(json::to_string(&self.view_builder(args)).unwrap())),
            (Method::POST, "/extend_recipe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.extend_recipe(authority, args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/install_recipe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.install_recipe(authority, args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/set_security_config") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.set_security_config(args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/create_universe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.create_universe(args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/remove_node") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.remove_nodes(vec![args].as_slice())
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/remove_query") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.remove_query(authority, args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            (Method::POST, "/unsubscribe") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| self.unsubscribe(args).map(|r| json::to_string(&r).unwrap())),
            (Method::POST, "/set_lease") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| self.set_lease(args).map(|r| json::to_string(&r).unwrap())),
            (Method::POST, "/export_data") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| self.export_data(args).map(|r| json::to_string(&r).unwrap())),
            (Method::POST, "/import_data") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| self.import_data(args).map(|r| json::to_string(&r).unwrap())),
            (Method::POST, "/change_permissions") => json::from_slice(&body)
                .map_err(|_| StatusCode::BAD_REQUEST)
                .map(|args| {
                    self.change_permissions(args)
                        .map(|r| json::to_string(&r).unwrap())
                }),
            _ => Err(StatusCode::NOT_FOUND),
        }
    }

    pub(super) fn handle_register(
        &mut self,
        msg: &CoordinationMessage,
        remote: &SocketAddr,
        read_listen_addr: SocketAddr,
    ) -> Result<(), io::Error> {
        info!(
            self.log,
            "new worker registered from {:?}, which listens on {:?}", msg.source, remote
        );

        let sender = TcpSender::connect(remote)?;
        let ws = Worker::new(sender);
        self.workers.insert(msg.source, ws);
        self.read_addrs.insert(msg.source, read_listen_addr);

        if self.workers.len() >= self.quorum {
            if let Some((recipes, recipe_version)) = self.pending_recovery.take() {
                assert_eq!(self.workers.len(), self.quorum);
                assert_eq!(self.recipe.version(), 0);
                assert!(recipe_version + 1 >= recipes.len());

                info!(self.log, "Restoring graph configuration");
                self.recipe = Recipe::with_version(
                    recipe_version + 1 - recipes.len(),
                    Some(self.log.clone()),
                );
                for r in recipes {
                    self.apply_recipe(self.recipe.clone().extend(&r).unwrap())
                        .unwrap();
                }
            }
        }

        Ok(())
    }

    fn check_worker_liveness(&mut self) {
        let mut any_failed = false;

        // check if there are any newly failed workers
        if self.last_checked_workers.elapsed() > self.healthcheck_every {
            for (_addr, ws) in self.workers.iter() {
                if ws.healthy && ws.last_heartbeat.elapsed() > self.heartbeat_every * 4 {
                    any_failed = true;
                }
            }
            self.last_checked_workers = Instant::now();
        }

        // if we have newly failed workers, iterate again to find all workers that have missed >= 3
        // heartbeats. This is necessary so that we correctly handle correlated failures of
        // workers.
        if any_failed {
            let mut failed = Vec::new();
            for (addr, ws) in self.workers.iter_mut() {
                if ws.healthy && ws.last_heartbeat.elapsed() > self.heartbeat_every * 3 {
                    error!(self.log, "worker at {:?} has failed!", addr);
                    ws.healthy = false;
                    failed.push(addr.clone());
                }
            }
            self.handle_failed_workers(failed);
        }
    }

    fn handle_failed_workers(&mut self, failed: Vec<WorkerIdentifier>) {
        // first, translate from the affected workers to affected data-flow nodes
        let mut affected_nodes = Vec::new();
        for wi in failed {
            info!(self.log, "handling failure of worker {:?}", wi);
            affected_nodes.extend(self.get_failed_nodes(&wi));
        }

        // then, figure out which queries are affected (and thus must be removed and added again in
        // a migration)
        let affected_queries = self.recipe.queries_for_nodes(affected_nodes);
        let (recovery, mut original) = self.recipe.make_recovery(affected_queries);

        // activate recipe
        self.apply_recipe(recovery.clone())
            .expect("failed to apply recovery recipe");

        // we must do this *after* the migration, since the migration itself modifies the recipe in
        // `recovery`, and we currently need to clone it here.
        let tmp = self.recipe.clone();
        original.set_prior(tmp.clone());
        // somewhat awkward, but we must replace the stale `SqlIncorporator` state in `original`
        original.set_sql_inc(tmp.sql_inc().clone());

        // back to original recipe, which should add the query again
        self.apply_recipe(original)
            .expect("failed to activate original recipe");
    }

    pub(super) fn handle_heartbeat(&mut self, msg: &CoordinationMessage) -> Result<(), io::Error> {
        match self.workers.get_mut(&msg.source) {
            None => crit!(
                self.log,
                "got heartbeat for unknown worker {:?}",
                msg.source
            ),
            Some(ref mut ws) => {
                ws.last_heartbeat = Instant::now();
            }
        }

        self.check_worker_liveness();
        Ok(())
    }

    pub(super) fn handle_leases(&mut self) -> Result<(), String> {
        if self.global_view.is_none() {
            return Ok(());
        }
        let view = self.global_view.as_mut().unwrap();
        let rows = view.lookup(&[0.into()], true).unwrap();

        let expired: Vec<NodeIndex> = rows
            .into_iter()
            .map(|r| r[1].clone().into())
            .map(|i: u64| NodeIndex::from(i as u32))
            .map(|ni| &self.ingredients[ni])
            .filter(|&n| n.is_base() && n.is_expired())
            .map(|n| n.global_addr())
            .collect();
        for ex in expired.into_iter() {
            self.unsubscribe(ex.index() as u32)
                .expect("failed to remove the node");
            let key: DataType = ex.index().into();
            self.global_table
                .as_mut()
                .unwrap()
                .delete(vec![key])
                .expect("failed to delete an expired row from global table");
        }
        Ok(())
    }

    pub fn set_lease(&mut self, lease: Lease) -> Result<(), String> {
        if self.global_table.is_none() {
            return Err("failed to find global table".to_string());
        }
        let table = self.global_table.as_mut().unwrap();
        let ttl = lease.ttl;

        let shard_name = match lease.name {
            Some(name) => name,
            None => "",
        };
        let nodes: Vec<NodeIndex> = lease
            .nodes
            .into_iter()
            .map(|i: u32| NodeIndex::from(i))
            .collect();
        for ni in nodes.into_iter() {
            let node = &mut self.ingredients[ni];
            if node.is_base() {
                node.set_lease(ttl);
                table
                    .insert_or_update(vec![shard_name.into(), ni.index().into()], vec![])
                    .expect("failed to insert into global table");
            }
        }
        Ok(())
    }

    pub fn send_message(
        &mut self,
        ni: NodeIndex,
        data: Records,
        purpose: MessagePurpose,
        permissions: Option<u8>,
    ) {
        let node = &mut self.ingredients[ni.clone()];
        let permissions = match permissions {
            Some(perm) => perm,
            None => node.get_permissions(),
        };
        match self
            .domains
            .get_mut(&node.domain())
            .unwrap()
            .send_to_healthy(
                box Packet::Message {
                    link: Link::new(node.local_addr(), node.local_addr()),
                    data,
                    tracer: None,
                    purpose,
                    permissions,
                },
                &self.workers,
            ) {
            Ok(_) => (),
            Err(e) => match e {
                SendError::IoError(ref ioe) => {
                    if ioe.kind() == io::ErrorKind::BrokenPipe
                        && ioe.get_ref().unwrap().description() == "worker failed"
                    {
                        // message would have gone to a failed worker, so ignore error
                    } else {
                        panic!("failed to send negative records: {:?}", e);
                    }
                }
                _ => {
                    panic!("failed to send negative records nodes: {:?}", e);
                }
            },
        }
    }

    /// Construct `ControllerInner` with a specified listening interface
    pub(super) fn new(
        log: slog::Logger,
        state: ControllerState,
        drx: futures::sync::mpsc::UnboundedReceiver<ControlReplyPacket>,
    ) -> Self {
        let mut g = petgraph::Graph::new();
        let source = g.add_node(node::Node::new(
            "source",
            &["because-type-inference"],
            node::special::Source,
        ));

        let mut materializations = Materializations::new(&log);
        if !state.config.partial_enabled {
            materializations.disable_partial()
        }
        materializations.set_frontier_strategy(state.config.frontier_strategy);

        let cc = Arc::new(ChannelCoordinator::new());
        assert_ne!(state.config.quorum, 0);

        let pending_recovery = if !state.recipes.is_empty() {
            Some((state.recipes, state.recipe_version))
        } else {
            None
        };

        let mut recipe = Recipe::blank(Some(log.clone()));
        recipe.enable_reuse(state.config.reuse);

        ControllerInner {
            ingredients: g,
            source,
            ndomains: 0,

            materializations,
            sharding: state.config.sharding,
            domain_config: state.config.domain_config,
            persistence: state.config.persistence,
            heartbeat_every: state.config.heartbeat_every,
            healthcheck_every: state.config.healthcheck_every,
            recipe,
            quorum: state.config.quorum,
            log,

            domains: Default::default(),
            domain_nodes: Default::default(),
            channel_coordinator: cc,
            debug_channel: None,
            epoch: state.epoch,

            remap: HashMap::default(),
            max_local_index: HashMap::default(),

            read_addrs: HashMap::default(),
            workers: HashMap::default(),

            pending_recovery,
            last_checked_workers: Instant::now(),

            replies: DomainReplies(drx),
            global_table: None,
            global_view: None,
        }
    }

    /// Create a global channel for receiving tracer events.
    ///
    /// Only domains created after this method is called will be able to send trace events.
    ///
    /// This function may only be called once because the receiving end it returned.
    #[allow(unused)]
    fn create_tracer_channel(&mut self) -> TcpListener {
        assert!(self.debug_channel.is_none());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();
        self.debug_channel = Some(listener.local_addr().unwrap());
        listener
    }

    /// Controls the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes are written to disk, but the log is
    ///     deleted once the `Controller` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory.
    ///     Useful for baseline numbers.
    ///
    /// `queue_capacity` indicates the number of packets that should be buffered until
    /// flushing, and `flush_timeout` indicates the length of time to wait before flushing
    /// anyway.
    ///
    /// Must be called before any domains have been created.
    #[allow(unused)]
    fn with_persistence_options(&mut self, params: PersistenceParameters) {
        assert_eq!(self.ndomains, 0);
        self.persistence = params;
    }

    pub(in crate::controller) fn place_domain(
        &mut self,
        idx: DomainIndex,
        num_shards: Option<usize>,
        log: &Logger,
        nodes: Vec<(NodeIndex, bool)>,
    ) -> DomainHandle {
        // TODO: can we just redirect all domain traffic through the worker's connection?
        let mut assignments = Vec::new();
        let mut nodes = Some(
            nodes
                .into_iter()
                .map(|(ni, _)| {
                    let node = self.ingredients.node_weight_mut(ni).unwrap().take();
                    node.finalize(&self.ingredients)
                })
                .map(|nd| (nd.local_addr(), cell::RefCell::new(nd)))
                .collect(),
        );

        // TODO(malte): simple round-robin placement for the moment
        let mut wi = self.workers.iter_mut();

        // Send `AssignDomain` to each shard of the given domain
        for i in 0..num_shards.unwrap_or(1) {
            let nodes = if i == num_shards.unwrap_or(1) - 1 {
                nodes.take().unwrap()
            } else {
                nodes.clone().unwrap()
            };

            let domain = DomainBuilder {
                index: idx,
                shard: if num_shards.is_some() { Some(i) } else { None },
                nshards: num_shards.unwrap_or(1),
                config: self.domain_config.clone(),
                nodes,
                persistence_parameters: self.persistence.clone(),
            };

            let (identifier, w) = loop {
                if let Some((i, w)) = wi.next() {
                    if w.healthy {
                        break (*i, w);
                    }
                } else {
                    wi = self.workers.iter_mut();
                }
            };

            // send domain to worker
            info!(
                log,
                "sending domain {}.{} to worker {:?}",
                domain.index.index(),
                domain.shard.unwrap_or(0),
                w.sender.peer_addr()
            );
            let src = w.sender.local_addr().unwrap();
            w.sender
                .send(CoordinationMessage {
                    epoch: self.epoch,
                    source: src,
                    payload: CoordinationPayload::AssignDomain(domain),
                })
                .unwrap();

            assignments.push(identifier);
        }

        // Wait for all the domains to acknowledge.
        let mut txs = HashMap::new();
        let mut announce = Vec::new();
        let replies = self.replies.read_n_domain_replies(num_shards.unwrap_or(1));
        for r in replies {
            match r {
                ControlReplyPacket::Booted(shard, addr) => {
                    self.channel_coordinator.insert_remote((idx, shard), addr);
                    announce.push(DomainDescriptor::new(idx, shard, addr));
                    txs.insert(
                        shard,
                        self.channel_coordinator
                            .builder_for(&(idx, shard))
                            .unwrap()
                            .build_sync()
                            .unwrap(),
                    );
                }
                crp => {
                    unreachable!("got unexpected control reply packet: {:?}", crp);
                }
            }
        }

        // Tell all workers about the new domain(s)
        // TODO(jon): figure out how much of the below is still true
        // TODO(malte): this is a hack, and not an especially neat one. In response to a
        // domain boot message, we broadcast information about this new domain to all
        // workers, which inform their ChannelCoordinators about it. This is required so
        // that domains can find each other when starting up.
        // Moreover, it is required for us to do this *here*, since this code runs on
        // the thread that initiated the migration, and which will query domains to ask
        // if they're ready. No domain will be ready until it has found its neighbours,
        // so by sending out the information here, we ensure that we cannot deadlock
        // with the migration waiting for a domain to become ready when trying to send
        // the information. (We used to do this in the controller thread, with the
        // result of a nasty deadlock.)
        for endpoint in self.workers.values_mut() {
            for &dd in &announce {
                endpoint
                    .sender
                    .send(CoordinationMessage {
                        epoch: self.epoch,
                        source: endpoint.sender.local_addr().unwrap(),
                        payload: CoordinationPayload::DomainBooted(dd),
                    })
                    .unwrap();
            }
        }

        let shards = assignments
            .into_iter()
            .enumerate()
            .map(|(i, worker)| {
                let tx = txs.remove(&i).unwrap();
                DomainShardHandle { worker, tx }
            })
            .collect();

        DomainHandle {
            idx,
            shards,
            log: log.clone(),
        }
    }

    /// Set the `Logger` to use for internal log messages.
    ///
    /// By default, all log messages are discarded.
    #[allow(unused)]
    fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
        self.materializations.set_logger(&self.log);
    }

    /// Adds a new user universe.
    /// User universes automatically enforce security policies.
    fn add_universe<F, T>(&mut self, context: HashMap<String, DataType>, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T,
    {
        info!(self.log, "starting migration: new soup universe");
        let miglog = self.log.new(o!());
        let mut m = Migration {
            mainline: self,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            context,
            start: time::Instant::now(),
            log: miglog,
            union_nodes: Default::default(),
        };
        let r = f(&mut m);
        m.commit();
        r
    }

    /// Perform a new query schema migration.
    // crate viz for tests
    crate fn migrate<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Migration) -> T,
    {
        info!(self.log, "starting migration");
        let miglog = self.log.new(o!());
        let mut m = Migration {
            mainline: self,
            added: Default::default(),
            columns: Default::default(),
            readers: Default::default(),
            context: Default::default(),
            start: time::Instant::now(),
            log: miglog,
            union_nodes: Default::default(),
        };
        let r = f(&mut m);
        m.commit();
        r
    }

    #[cfg(test)]
    crate fn graph(&self) -> &Graph {
        &self.ingredients
    }

    /// Get a Vec of all known input nodes.
    ///
    /// Input nodes are here all nodes of type `Table`. The addresses returned by this function will
    /// all have been returned as a key in the map from `commit` at some point in the past.
    fn inputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .neighbors_directed(self.source, petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                let base = &self.ingredients[n];
                if base.is_base() {
                    Some((base.name().to_owned(), n))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get a Vec of all known output nodes.
    ///
    /// Output nodes here refers to nodes of type `Reader`, which is the nodes created in response
    /// to calling `.maintain` or `.stream` for a node during a migration.
    fn outputs(&self) -> BTreeMap<String, NodeIndex> {
        self.ingredients
            .externals(petgraph::EdgeDirection::Outgoing)
            .filter_map(|n| {
                let name = self.ingredients[n].name().to_owned();
                self.ingredients[n]
                    .with_reader(|r| {
                        // we want to give the the node address that is being materialized not that of
                        // the reader node itself.
                        (name, r.is_for())
                    })
                    .ok()
            })
            .collect()
    }

    fn find_view_for(&self, node: NodeIndex, name: &str) -> Option<NodeIndex> {
        // reader should be a child of the given node. however, due to sharding, it may not be an
        // *immediate* child. furthermore, once we go beyond depth 1, we may accidentally hit an
        // *unrelated* reader node. to account for this, readers keep track of what node they are
        // "for", and we simply search for the appropriate reader by that metric. since we know
        // that the reader must be relatively close, a BFS search is the way to go.
        let mut bfs = Bfs::new(&self.ingredients, node);
        while let Some(child) = bfs.next(&self.ingredients) {
            if self.ingredients[child]
                .with_reader(|r| r.is_for() == node)
                .unwrap_or(false)
                && self.ingredients[child].name() == name
            {
                return Some(child);
            }
        }
        None
    }

    /// Obtain a `ViewBuilder` that can be sent to a client and then used to query a given
    /// (already maintained) reader node called `name`.
    fn view_builder(&self, name: &str) -> Option<ViewBuilder> {
        // first try to resolve the node via the recipe, which handles aliasing between identical
        // queries.
        let node = match self.recipe.node_addr_for(name) {
            Ok(ni) => ni,
            Err(_) => {
                // if the recipe doesn't know about this query, traverse the graph.
                // we need this do deal with manually constructed graphs (e.g., in tests).
                *self.outputs().get(name)?
            }
        };

        self.find_view_for(node, name).map(|r| {
            let domain = self.ingredients[r].domain();
            let columns = self.ingredients[r].fields().to_vec();
            let schema = self.view_schema(r);
            let shards = (0..self.domains[&domain].shards())
                .map(|i| self.read_addrs[&self.domains[&domain].assignment(i)])
                .collect();

            ViewBuilder {
                node: r,
                columns,
                schema,
                shards,
            }
        })
    }

    fn view_schema(&self, view_ni: NodeIndex) -> Option<Vec<ColumnSpecification>> {
        let n = &self.ingredients[view_ni];
        let schema: Vec<_> = (0..n.fields().len())
            .map(|i| schema::column_schema(&self.ingredients, view_ni, &self.recipe, i, &self.log))
            .collect();

        if schema.iter().any(Option::is_none) {
            None
        } else {
            Some(schema.into_iter().map(Option::unwrap).collect())
        }
    }

    /// Obtain a TableBuild that can be used to construct a Table to perform writes and deletes
    /// from the given named base node.
    fn table_builder(&self, base: &str) -> Option<TableBuilder> {
        let ni = match self.recipe.node_addr_for(base) {
            Ok(ni) => ni,
            Err(_) => {
                let inputs = self.inputs();
                let res = inputs.get(base).clone();
                if res.is_none() {
                    return None;
                }
                *res?
            }
        };
        let node = &self.ingredients[ni];

        trace!(self.log, "creating table"; "for" => base);

        let mut key = self.ingredients[ni]
            .suggest_indexes(ni)
            .remove(&ni)
            .unwrap_or_else(Vec::new);
        let mut is_primary = false;
        if key.is_empty() {
            if let Sharding::ByColumn(col, _) = self.ingredients[ni].sharded_by() {
                key = vec![col];
            }
        } else {
            is_primary = true;
        }

        let txs = (0..self.domains[&node.domain()].shards())
            .map(|i| {
                self.channel_coordinator
                    .get_addr(&(node.domain(), i))
                    .unwrap()
            })
            .collect();

        let base_operator = node
            .get_base()
            .expect("asked to get table for non-base node");
        let columns: Vec<String> = node
            .fields()
            .iter()
            .enumerate()
            .filter(|&(n, _)| !base_operator.get_dropped().contains_key(n))
            .map(|(_, s)| s.clone())
            .collect();
        assert_eq!(
            columns.len(),
            node.fields().len() - base_operator.get_dropped().len()
        );
        let schema = self.recipe.schema_for(base).map(|s| match s {
            Schema::Table(s) => s,
            _ => panic!("non-base schema {:?} returned for table '{}'", s, base),
        });

        Some(TableBuilder {
            txs,
            ni: node.global_addr(),
            addr: node.local_addr(),
            key,
            key_is_primary: is_primary,
            dropped: base_operator.get_dropped(),
            table_name: node.name().to_owned(),
            columns,
            schema,
        })
    }

    /// Get statistics about the time spent processing different parts of the graph.
    fn get_statistics(&mut self) -> GraphStats {
        trace!(self.log, "asked to get statistics");
        let log = &self.log;
        let workers = &self.workers;
        let replies = &mut self.replies;
        // TODO: request stats from domains in parallel.
        let domains = self
            .domains
            .iter_mut()
            .flat_map(|(&di, s)| {
                trace!(log, "requesting stats from domain"; "di" => di.index());
                s.send_to_healthy(box Packet::GetStatistics, workers)
                    .unwrap();
                replies
                    .wait_for_statistics(&s)
                    .into_iter()
                    .enumerate()
                    .map(move |(i, s)| ((di, i), s))
            })
            .collect();

        GraphStats { domains }
    }

    fn get_instances(&self) -> Vec<(WorkerIdentifier, bool, Duration)> {
        self.workers
            .iter()
            .map(|(&id, ref status)| (id, status.healthy, status.last_heartbeat.elapsed()))
            .collect()
    }

    fn flush_partial(&mut self) -> u64 {
        // get statistics for current domain sizes
        // and evict all state from partial nodes
        let workers = &self.workers;
        let replies = &mut self.replies;
        let to_evict: Vec<_> = self
            .domains
            .iter_mut()
            .map(|(di, s)| {
                s.send_to_healthy(box Packet::GetStatistics, workers)
                    .unwrap();
                let to_evict: Vec<(NodeIndex, u64)> = replies
                    .wait_for_statistics(&s)
                    .into_iter()
                    .flat_map(move |(_, node_stats)| {
                        node_stats
                            .into_iter()
                            .filter_map(|(ni, ns)| match ns.materialized {
                                MaterializationStatus::Partial { .. } => Some((ni, ns.mem_size)),
                                _ => None,
                            })
                    })
                    .collect();
                (*di, to_evict)
            })
            .collect();

        let mut total_evicted = 0;
        for (di, nodes) in to_evict {
            for (ni, bytes) in nodes {
                let na = self.ingredients[ni].local_addr();
                self.domains
                    .get_mut(&di)
                    .unwrap()
                    .send_to_healthy(
                        box Packet::Evict {
                            node: Some(na),
                            num_bytes: bytes as usize,
                        },
                        workers,
                    )
                    .expect("failed to send domain flush message");
                total_evicted += bytes;
            }
        }

        warn!(
            self.log,
            "flushed {} bytes of partial domain state", total_evicted
        );

        total_evicted
    }

    pub(super) fn create_universe(
        &mut self,
        context: HashMap<String, DataType>,
    ) -> Result<(), String> {
        let log = self.log.clone();
        let mut r = self.recipe.clone();
        let groups = self.recipe.security_groups();

        let mut universe_groups = HashMap::new();

        let uid = context
            .get("id")
            .expect("Universe context must have id")
            .clone();
        let uid = &[uid];
        if context.get("group").is_none() {
            let x = Arc::new(Mutex::new(HashMap::new()));
            for g in groups {
                // TODO: this should use external APIs through noria::ControllerHandle
                // TODO: can this move to the client entirely?
                let rgb: Option<ViewBuilder> = self.view_builder(&g);
                // TODO: is it even okay to use wait() here?
                let view = rgb.map(|rgb| rgb.build(x.clone()).wait().unwrap()).unwrap();
                let my_groups: Vec<DataType> = view
                    .lookup(uid, true)
                    .wait()
                    .unwrap()
                    .1
                    .iter()
                    .map(|v| v[1].clone())
                    .collect();
                universe_groups.insert(g, my_groups);
            }
        }

        self.add_universe(context.clone(), |mut mig| {
            r.next();
            match r.create_universe(&mut mig, universe_groups) {
                Ok(ar) => {
                    info!(log, "{} expressions added", ar.expressions_added);
                    info!(log, "{} expressions removed", ar.expressions_removed);
                    Ok(())
                }
                Err(e) => {
                    crit!(log, "failed to create universe: {:?}", e);
                    Err("failed to create universe".to_owned())
                }
            }
            .unwrap();
        });

        self.recipe = r;
        Ok(())
    }

    fn set_security_config(&mut self, p: String) -> Result<(), String> {
        self.recipe.set_security_config(&p);
        Ok(())
    }

    fn apply_recipe(&mut self, mut new: Recipe) -> Result<ActivationResult, String> {
        let r = self.migrate(|mig| {
            new.activate(mig)
                .map_err(|e| format!("failed to activate recipe: {}", e))
        });

        match r {
            Ok(ref ra) => {
                let (removed_bases, removed_other): (Vec<_>, Vec<_>) = ra
                    .removed_leaves
                    .iter()
                    .cloned()
                    .partition(|ni| self.ingredients[*ni].is_base());

                // first remove query nodes in reverse topological order
                let mut topo_removals = Vec::with_capacity(removed_other.len());
                let mut topo = petgraph::visit::Topo::new(&self.ingredients);
                while let Some(node) = topo.next(&self.ingredients) {
                    if removed_other.contains(&node) {
                        topo_removals.push(node);
                    }
                }
                topo_removals.reverse();

                for leaf in topo_removals {
                    self.remove_leaf(leaf)?;
                }

                // now remove bases
                for base in removed_bases {
                    // TODO(malte): support removing bases that still have children?
                    let children: Vec<NodeIndex> = self
                        .ingredients
                        .neighbors_directed(base, petgraph::EdgeDirection::Outgoing)
                        .collect();
                    // TODO(malte): what about domain crossings? can ingress/egress nodes be left
                    // behind?
                    assert_eq!(children.len(), 0);
                    debug!(
                        self.log,
                        "Removing base \"{}\"",
                        self.ingredients[base].name();
                        "node" => base.index(),
                    );
                    // now drop the (orphaned) base
                    self.remove_nodes(vec![base].as_slice()).unwrap();
                }

                self.recipe = new;
            }
            Err(ref e) => {
                crit!(self.log, "failed to apply recipe: {}", e);
                // TODO(malte): a little yucky, since we don't really need the blank recipe
                let recipe = mem::replace(&mut self.recipe, Recipe::blank(None));
                self.recipe = recipe.revert();
            }
        }

        r
    }

    fn extend_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        add_txt: String,
    ) -> Result<ActivationResult, String> {
        // needed because self.apply_recipe needs to mutate self.recipe, so can't have it borrowed
        let new = mem::replace(&mut self.recipe, Recipe::blank(None));
        match new.extend(&add_txt) {
            Ok(new) => {
                let activation_result = self.apply_recipe(new);
                if authority
                    .read_modify_write(STATE_KEY, |state: Option<ControllerState>| match state {
                        None => unreachable!(),
                        Some(ref state) if state.epoch > self.epoch => Err(()),
                        Some(mut state) => {
                            state.recipe_version = self.recipe.version();
                            state.recipes.push(add_txt.clone());
                            Ok(state)
                        }
                    })
                    .is_err()
                {
                    return Err("Failed to persist recipe extension".to_owned());
                }

                activation_result
            }
            Err((old, e)) => {
                // need to restore the old recipe
                crit!(self.log, "failed to extend recipe: {:?}", e);
                self.recipe = old;
                Err("failed to extend recipe".to_owned())
            }
        }
    }

    fn install_recipe<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        r_txt: String,
    ) -> Result<ActivationResult, String> {
        match Recipe::from_str(&r_txt, Some(self.log.clone())) {
            Ok(r) => {
                let old = mem::replace(&mut self.recipe, Recipe::blank(None));
                let new = old.replace(r).unwrap();
                let activation_result = self.apply_recipe(new);
                if authority
                    .read_modify_write(STATE_KEY, |state: Option<ControllerState>| match state {
                        None => unreachable!(),
                        Some(ref state) if state.epoch > self.epoch => Err(()),
                        Some(mut state) => {
                            state.recipe_version = self.recipe.version();
                            state.recipes = vec![r_txt.clone()];
                            Ok(state)
                        }
                    })
                    .is_err()
                {
                    return Err("Failed to persist recipe installation".to_owned());
                }
                activation_result
            }
            Err(e) => {
                crit!(self.log, "failed to parse recipe: {:?}", e);
                Err("failed to parse recipe".to_owned())
            }
        }
    }

    fn graphviz(&self, detailed: bool) -> String {
        graphviz(&self.ingredients, detailed, &self.materializations)
    }

    pub fn unsubscribe(&mut self, ni: u32) -> Result<(), String> {
        let node = NodeIndex::from(ni);
        self.send_message(node, Default::default(), MessagePurpose::Unsubscribe, None);
        self.ingredients[node].remove();
        Ok(())
    }

    fn remove_leaf(&mut self, mut leaf: NodeIndex) -> Result<(), String> {
        let mut removals = vec![];
        let start = leaf;
        assert!(!self.ingredients[leaf].is_source());

        info!(
            self.log,
            "Computing removals for removing node {}",
            leaf.index()
        );

        if self
            .ingredients
            .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
            .count()
            > 0
        {
            // This query leaf node has children -- typically, these are readers, but they can also
            // include egress nodes or other, dependent queries.
            let mut has_non_reader_children = false;
            let mut non_readers = Vec::default();
            let readers: Vec<_> = self
                .ingredients
                .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
                .filter(|ni| {
                    if self.ingredients[*ni].is_reader() {
                        true
                    } else {
                        non_readers.push(*ni);
                        has_non_reader_children = true;
                        false
                    }
                })
                .collect();
            if has_non_reader_children {
                crit!(
                    self.log,
                    "not removing node {} yet, as it still has non-reader children",
                    leaf.index()
                );
                for nr in non_readers {
                    if !self.ingredients[nr].is_base() {
                        let mut children = Vec::default();
                        self.ingredients
                            .neighbors_directed(nr, petgraph::EdgeDirection::Outgoing)
                            .for_each(|ni| children.push(ni));
                        for child in children {
                            self.remove_leaf(child).expect("failed to remove a leaf");
                        }
                        self.remove_leaf(nr).expect("failed to remove a leaf");
                    }
                }
            }

            // nodes can have only one reader attached
            assert!(readers.len() <= 1);
            debug!(
                self.log,
                "Removing query leaf \"{}\"", self.ingredients[leaf].name();
                "node" => leaf.index(),
            );
            if !readers.is_empty() {
                removals.push(readers[0]);
                leaf = readers[0];
            } else {
                //unreachable!();
                //
            }
        }

        // `node` now does not have any children any more
        assert_eq!(
            self.ingredients
                .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
                .count(),
            0
        );

        let mut nodes = vec![leaf];
        while let Some(node) = nodes.pop() {
            let mut parents = self
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .detach();
            while let Some(parent) = parents.next_node(&self.ingredients) {
                let edge = self.ingredients.find_edge(parent, node).unwrap();
                self.ingredients.remove_edge(edge);

                if !self.ingredients[parent].is_source()
                    && !self.ingredients[parent].is_base()
                    // ok to remove original start leaf
                    && (parent == start || !self.recipe.sql_inc().is_leaf_address(parent))
                    && self
                        .ingredients
                        .neighbors_directed(parent, petgraph::EdgeDirection::Outgoing)
                        .count() == 0
                {
                    nodes.push(parent);
                }
            }

            removals.push(node);
        }

        self.remove_nodes(removals.as_slice())
    }

    fn remove_nodes(&mut self, removals: &[NodeIndex]) -> Result<(), String> {
        self.notify_domain(removals, HashMap::new())
    }

    fn notify_domain(
        &mut self,
        removals: &[NodeIndex],
        children: HashMap<LocalNodeIndex, Option<Emit>>,
    ) -> Result<(), String> {
        // Remove node from controller local state
        let mut domain_removals: HashMap<DomainIndex, Vec<(LocalNodeIndex, NodeIndex)>> =
            HashMap::default();
        for ni in removals {
            self.ingredients[*ni].remove();
            debug!(self.log, "Removed node {}", ni.index());
            domain_removals
                .entry(self.ingredients[*ni].domain())
                .or_insert_with(Vec::new)
                .push((self.ingredients[*ni].local_addr(), *ni))
        }

        let c = &children;

        // Send messages to domains
        for (domain, nodes) in domain_removals {
            trace!(
                self.log,
                "Notifying domain {} of node removals",
                domain.index(),
            );
            for (_, ni) in nodes.clone() {
                self.remap.get_mut(&domain).unwrap().remove(&ni);
                self.domain_nodes.get_mut(&domain).unwrap().remove_item(&ni);
            }


            match self.domains.get_mut(&domain).unwrap().send_to_healthy(
                box Packet::RemoveNodes {
                    nodes,
                    children: c.clone(),
                },
                &self.workers,
            ) {
                Ok(_) => (),
                Err(e) => match e {
                    SendError::IoError(ref ioe) => {
                        if ioe.kind() == io::ErrorKind::BrokenPipe
                            && ioe.get_ref().unwrap().description() == "worker failed"
                        {
                            // message would have gone to a failed worker, so ignore error
                        } else {
                            panic!("failed to remove nodes: {:?}", e);
                        }
                    }
                    _ => {
                        panic!("failed to remove nodes: {:?}", e);
                    }
                },
            }
        }

        Ok(())
    }

    fn get_failed_nodes(&self, lost_worker: &WorkerIdentifier) -> Vec<NodeIndex> {
        // Find nodes directly impacted by worker failure.
        let mut nodes: Vec<NodeIndex> = self.nodes_on_worker(Some(lost_worker));

        // Add any other downstream nodes.
        let mut failed_nodes = Vec::new();
        while let Some(node) = nodes.pop() {
            failed_nodes.push(node);
            for child in self
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
            {
                if !nodes.contains(&child) {
                    nodes.push(child);
                }
            }
        }
        failed_nodes
    }

    /// List data-flow nodes, on a specific worker if `worker` specified.
    fn nodes_on_worker(&self, worker: Option<&WorkerIdentifier>) -> Vec<NodeIndex> {
        // NOTE(malte): this traverses all graph vertices in order to find those assigned to a
        // domain. We do this to avoid keeping separate state that may get out of sync, but it
        // could become a performance bottleneck in the future (e.g., when recovering large
        // graphs).
        let domain_nodes = |i: DomainIndex| -> Vec<NodeIndex> {
            self.ingredients
                .node_indices()
                .filter(|&ni| ni != self.source)
                .filter(|&ni| !self.ingredients[ni].is_dropped())
                .filter(|&ni| self.ingredients[ni].domain() == i)
                .collect()
        };

        if worker.is_some() {
            self.domains
                .values()
                .filter(|dh| dh.assigned_to_worker(worker.unwrap()))
                .fold(Vec::new(), |mut acc, dh| {
                    acc.extend(domain_nodes(dh.index()));
                    acc
                })
        } else {
            self.domains.values().fold(Vec::new(), |mut acc, dh| {
                acc.extend(domain_nodes(dh.index()));
                acc
            })
        }
    }
    fn remove_query<A: Authority + 'static>(
        &mut self,
        authority: &Arc<A>,
        qname: &str,
    ) -> Result<(), String> {
        let old = self.recipe.clone();
        self.recipe.remove_query(qname);
        let updated = self.recipe.clone();
        let replaced = old.replace(updated).unwrap();

        self.apply_recipe(replaced)
            .expect("failed to apply recipe in remove_query");
        if authority
            .read_modify_write(STATE_KEY, |state: Option<ControllerState>| match state {
                None => unreachable!(),
                Some(ref state) if state.epoch > self.epoch => Err(()),
                Some(mut state) => {
                    state.recipe_version = self.recipe.version();
                    // state.recipes.push(updated_recipe.clone());
                    Ok(state)
                }
            })
            .is_err()
        {
            return Err("Failed to persist recipe extension".to_owned());
        }
        Ok(())
    }

    fn export_data(&mut self, ts: Vec<u32>) -> Result<String, String> {
        let tables: Vec<NodeIndex> = ts.into_iter().map(|i| NodeIndex::from(i as u32)).collect();
        let mut metadata: Vec<Table> = Vec::new();
        for ni in tables.into_iter() {
            let node = &self.ingredients[ni];
            if !node.is_base() {
                continue;
            }

            let primary_key = match node.get_base().unwrap().key() {
                Some(key) => key.to_vec(),
                None => vec![],
            };

            let resub_key = match node.get_base().unwrap().resub_key() {
                Some(k) => k.to_vec(),
                None => vec![],
            };

            metadata.push(Table {
                name: node.name.clone(),
                ni,
                fields: node.fields.clone(),
                rows: None,
                primary_key: Some(primary_key),
                resub_key: Some(resub_key),
                permissions: node.get_permissions(),
            });
        }

        let view_nis = self.migrate(|mig| {
            let mut nis = Vec::new();
            for table in metadata.clone().into_iter() {
                let view_name = format!("{}_lookup", table.name);
                let fields_len = table.fields.len();
                let mut fields = table.fields;
                fields.push("bogokey".to_string());
                let f: Vec<usize> = (0..fields_len.clone()).collect();
                let new_view = mig.add_ingredient_with_permissions(
                    &view_name,
                    &fields,
                    Project::new(table.ni, &f, Some(vec![0.into()]), None),
                    Some(table.permissions),
                );
                mig.maintain_anonymous(new_view, &[fields_len]);
                nis.push(new_view);
            }
            nis
        });
        // collect indices to be removed
        let mut nodes_to_remove = Vec::new();
        for ni in view_nis {
            // we have added a projection and a reader, so let's delete them
            self.ingredients
                .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
                .for_each(|c| nodes_to_remove.push(c));
            nodes_to_remove.push(ni);
        }

        let mut ts = Vec::new();
        let mut json_string = "".to_string();
        for mut table in metadata.into_iter() {
            let view = format!("{}_lookup", table.name);
            let x = Arc::new(Mutex::new(HashMap::new()));
            let vb = self.view_builder(&view);
            let view = vb.map(|vb| vb.build(x.clone()).wait().unwrap()).unwrap();
            let res = view.lookup(&[0.into()], true).wait().unwrap().1;
            let rows: Vec<Vec<DataType>> = res
                .into_iter()
                .map(|mut r| {
                    // eliminate bogokeys from each response
                    r.pop(); // safe to pop here, since we always insert an additional column
                    r
                })
                .collect();
            table.rows = Some(rows);
            ts.push(table);
        }

        // add to result
        let tables = Tables { t: ts };
        let json = serde_json::to_string_pretty(&tables).unwrap();
        json_string.push_str(&json);
        nodes_to_remove.sort_by(|a, b| b.cmp(a));
        let mut i = 0;
        while i < nodes_to_remove.len() {
            let ni = nodes_to_remove[i];
            let domain = self.ingredients[ni].domain();
            let laddr = self.ingredients[ni].local_addr();
            if self.ingredients[ni].is_reader() {
                if self.materializations.initialized_readers.contains(&ni) {
                    self.materializations.initialized_readers.remove(&ni);
                }
                if self.materializations.paths_ending_at.contains_key(&ni) {
                    self.materializations.paths_ending_at.remove(&ni);
                }
                self.remove_leaf(ni).expect("failed to remove the view");
            }
            self.ingredients.remove_node(ni);
            // remove the parent
            self.ingredients.remove_node(nodes_to_remove[i + 1]);
            i += 2;
        }
        Ok(json_string)
    }

    fn remove_base(&mut self, base: NodeIndex) -> Result<HashMap<NodeIndex, Vec<usize>>, String> {
        let index = self.ingredients[base].index().clone();
        let parent_fields: Vec<_> = (0..self.ingredients[base].fields().len()).collect();
        let mut children_with_fields = HashMap::new();

        let mut ch: Vec<NodeIndex> = self
            .ingredients
            .neighbors_directed(base, petgraph::EdgeDirection::Outgoing)
            .collect();

        let unions: HashSet<NodeIndex> = ch
            .clone()
            .into_iter()
            .filter(|&ni| self.ingredients[ni].is_union())
            .collect();
        for i in 0..ch.len() {
            let child = &mut self.ingredients[ch[i]];
            if child.is_egress() {
                let mut stack = Vec::new();
                stack.push(ch[i]);
                let mut prev = base;
                let mut chi = ch[i];
                while !stack.is_empty() {
                    chi = stack.pop().unwrap();
                    let edge = self.ingredients.find_edge(prev, chi).unwrap();
                    self.ingredients.remove_edge(edge);
                    if !self.ingredients[chi].is_egress() && !self.ingredients[chi].is_ingress() {
                        let e = self.ingredients[chi].get_emits();
                        let fields = e.get(&prev).expect("emits from such parent do not exist");
                        children_with_fields.insert(chi, fields.clone());

                        let mut ch_with_meta = HashMap::new();
                        ch_with_meta.insert(self.ingredients[chi].local_addr(), None);
                        self.notify_domain(&vec![prev], ch_with_meta)
                            .expect("failed to let domain know of removal");
                        continue;
                    }
                    let grands: Vec<_> = self
                        .ingredients
                        .neighbors_directed(chi, petgraph::EdgeDirection::Outgoing)
                        .collect();

                    for c in grands {
                        stack.push(c);
                    }
                    prev = chi;
                }
                ch[i] = chi;
            }
        }
        ch.clone().into_iter().for_each(|c| {
            let e = self.ingredients[c].get_emits();
            if e.is_empty() {
                children_with_fields
                    .entry(c)
                    .or_insert(parent_fields.clone());
            } else {
                match children_with_fields.entry(c) {
                    Vacant(entry) => {
                        let v = e.get(&base).expect("emits from such parent do not exist");
                        entry.insert(v.to_vec());
                    }
                    Occupied(_) => {}
                };
            };
        });

        let mut children_with_metadata: HashMap<LocalNodeIndex, Option<Emit>> = HashMap::new();

        for child in ch.into_iter() {
            let edge = match self.ingredients.find_edge(base, child) {
                Some(e) => e,
                None => continue,
            };
            let child_node = &mut self.ingredients[child];

            if !unions.contains(&child) {
                children_with_metadata
                    .entry(child_node.local_addr())
                    .or_insert(None);
                self.ingredients.remove_edge(edge);
                continue;
            }
            match index {
                Some(ip) => {
                    child_node.remove_parent_from_union(ip);
                    let meta = child_node.get_metadata();
                    match meta {
                        ParentInfo::Emit(e) => {
                            children_with_metadata
                                .entry(child_node.local_addr())
                                .or_insert(Some(e));
                        }
                        ParentInfo::IndexPair(_) => panic!("Wrong parent info"),
                    }
                }
                None => {}
            }
            self.ingredients.remove_edge(edge);
        }
        self.notify_domain(&vec![base], children_with_metadata)
            .expect("failed to let domain know of removal");

        Ok(children_with_fields)
    }

    fn import_data(&mut self, serialized: String) -> Result<Vec<u32>, String> {
        let mut new_bases: Vec<u32> = Vec::new();
        let tables: Tables = serde_json::from_str(&serialized).unwrap();
        for table in tables.t.into_iter() {
            let ni = table.ni;
            let name = table.name.clone();
            let fields = table.fields.clone();
            let resub_keys = table.resub_key.clone();
            let anonymized = match resub_keys.clone() {
                Some(resub_key) => {
                    if !resub_key.is_empty() {
                        // remove the contents of the anonymized base
                        self.send_message(ni, Default::default(), MessagePurpose::Clear, None);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            };

            let node = &self.ingredients[table.ni];

            // check if it is the right base
            if node.name != name || node.fields != fields {
                return Err("Could not find the matching node".to_owned());
            }

            let primary_key = match table.primary_key.clone() {
                Some(k) => k.to_vec(),
                None => vec![],
            };
            let permissions = table.permissions.clone();
            let data: Records = table.rows.unwrap().into();
            let children_with_fields = self.remove_base(ni).expect("failed to remove base");

            let new = self.migrate(|mig| {
                let mut base = Base::default().with_key(primary_key);
                if anonymized {
                    base = base.anonymize_with_resub_key(resub_keys.unwrap());
                }
                let new_base = mig.add_base_with_permissions(name, fields, base, Some(permissions));
                for (child, fields) in children_with_fields.into_iter() {
                    mig.add_parent(new_base, child, fields.to_vec());
                }
                new_base
            });
            self.send_message(new.clone(), data, MessagePurpose::Subscribe, None);
            assert!(self.ingredients[new].is_base());
            new_bases.push(new.index() as u32);
        }
        Ok(new_bases)
    }

    fn send_change_permissions(
        &mut self,
        permissions: u8,
        partial: HashSet<LocalNodeIndex>,
        step: ChPermStep,
        addr: LocalNodeIndex,
        domain: DomainIndex,
    ) {
        match self.domains.get_mut(&domain).unwrap().send_to_healthy(
            box Packet::ChangePermissions {
                step,
                link: Link::new(addr, addr),
                keys: Vec::default(),
                data: Default::default(),
                partial,
                permissions,
            },
            &self.workers,
        ) {
            Ok(_) => (),
            Err(e) => match e {
                SendError::IoError(ref ioe) => {
                    if ioe.kind() == io::ErrorKind::BrokenPipe
                        && ioe.get_ref().unwrap().description() == "worker failed"
                    {
                        // message would have gone to a failed worker, so ignore error
                    } else {
                        panic!("failed to send negative records: {:?}", e);
                    }
                }
                _ => {
                    panic!("failed to send negative records nodes: {:?}", e);
                }
            },
        }
    }

    fn change_permissions(&mut self, message: PermissionsChange) -> Result<(), String> {
        let base_ni = NodeIndex::from(message.ni);
        let new = message.to;
        if self.ingredients[base_ni].get_permissions() == new {
            return Ok(());
        }
        let base_domain = &self.ingredients[base_ni].domain();
        let my_addr = self.ingredients[base_ni].local_addr();

        let children: Vec<_> = self
            .ingredients
            .neighbors_directed(base_ni, petgraph::EdgeDirection::Outgoing)
            .collect();
        let partial: HashSet<_> = children
            .into_iter()
            .filter_map(|ni| {
                if self.materializations.is_partial(ni) {
                    Some(self.ingredients[ni].local_addr().clone())
                } else {
                    None
                }
            })
            .collect();

        let old = self.ingredients[base_ni].get_permissions();
        self.send_change_permissions(
            old,
            partial.clone(),
            ChPermStep::Cleanup,
            my_addr,
            *base_domain,
        );
        self.ingredients[base_ni].update_permissions(new);
        self.send_change_permissions(new, partial, ChPermStep::Update, my_addr, *base_domain);

        Ok(())
    }
}

impl Drop for ControllerInner {
    fn drop(&mut self) {
        for d in self.domains.values_mut() {
            // XXX: this is a terrible ugly hack to ensure that all workers exit
            for _ in 0..100 {
                // don't unwrap, because given domain may already have terminated
                drop(d.send_to_healthy(box Packet::Quit, &self.workers));
            }
        }
    }
}
