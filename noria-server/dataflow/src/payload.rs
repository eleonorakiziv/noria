use petgraph;
use serde::{Deserialize, Serialize};

#[cfg(debug_assertions)]
use backtrace::Backtrace;
use domain;
use node;
use noria;
use noria::channel;
use noria::internal::LocalOrNot;
use prelude::*;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::time;
use ops::union::Emit;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    pub node: LocalNodeIndex,
    pub partial_key: Option<Vec<usize>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum SourceSelection {
    /// Query only the shard of the source that matches the key.
    KeyShard {
        key_i_to_shard: usize,
        nshards: usize,
    },
    /// Query the same shard of the source as the destination.
    SameShard,
    /// Query all shards of the source.
    ///
    /// Value is the number of shards.
    AllShards(usize),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(SourceSelection, domain::Index),
    Local(Vec<usize>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum InitialState {
    PartialLocal(Vec<(Vec<usize>, Vec<Tag>)>),
    IndexedLocal(HashSet<Vec<usize>>),
    PartialGlobal {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: Vec<usize>,
        trigger_domain: (domain::Index, usize),
    },
    Global {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: Vec<usize>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ReplayPieceContext {
    Partial {
        for_keys: HashSet<Vec<DataType>>,
        unishard: bool,
        ignore: bool,
    },
    Regular {
        last: bool,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SourceChannelIdentifier {
    pub token: usize,
    pub tag: u32,
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum Packet {
    // Data messages
    //
    Input { //0
        inner: LocalOrNot<Input>,
        src: Option<SourceChannelIdentifier>,
        senders: Vec<SourceChannelIdentifier>,
    },

    /// Regular data-flow update.
    Message { //1
        link: Link,
        data: Records,
        tracer: Tracer,
    },

    /// Update that is part of a tagged data-flow replay path.
    ReplayPiece {//2
        link: Link,
        tag: Tag,
        data: Records,
        context: ReplayPieceContext,
    },

    /// Trigger an eviction from the target node.
    Evict { //3
        node: Option<LocalNodeIndex>,
        num_bytes: usize,
    },

    /// Evict the indicated keys from the materialization targed by the replay path `tag` (along
    /// with any other materializations below it).
    EvictKeys { //4
        link: Link,
        tag: Tag,
        keys: Vec<Vec<DataType>>,
    },

    //
    // Internal control
    //
    Finish(Tag, LocalNodeIndex), //5

    // Control messages
    //
    /// Add a new node to this domain below the given parents.
    AddNode { //6
        node: Node,
        parents: Vec<LocalNodeIndex>,
        union_children: HashMap<LocalNodeIndex, Emit>,
    },

    /// Direct domain to remove some nodes.
    RemoveNodes { //7
        nodes: Vec<LocalNodeIndex>,
    },

    /// Add a new column to an existing `Base` node.
    AddBaseColumn { // 8
        node: LocalNodeIndex,
        field: String,
        default: DataType,
    },

    /// Drops an existing column from a `Base` node.
    DropBaseColumn { //9
        node: LocalNodeIndex,
        column: usize,
    },

    /// Update Egress node.
    UpdateEgress { // 10
        node: LocalNodeIndex,
        new_tx: Option<(NodeIndex, LocalNodeIndex, ReplicaAddr)>,
        new_tag: Option<(Tag, NodeIndex)>,
    },

    /// Add a shard to a Sharder node.
    ///
    /// Note that this *must* be done *before* the sharder starts being used!
    UpdateSharder { //11
        node: LocalNodeIndex,
        new_txs: (LocalNodeIndex, Vec<ReplicaAddr>),
    },

    /// Add a streamer to an existing reader node.
    AddStreamer { //12
        node: LocalNodeIndex,
        new_streamer: channel::StreamSender<Vec<node::StreamUpdate>>,
    },

    /// Set up a fresh, empty state for a node, indexed by a particular column.
    ///
    /// This is done in preparation of a subsequent state replay.
    PrepareState {// 13
        node: LocalNodeIndex,
        state: InitialState,
    },

    /// Probe for the number of records in the given node's state
    StateSizeProbe { // 14
        node: LocalNodeIndex,
    },

    /// Inform domain about a new replay path.
    SetupReplayPath { //15
        tag: Tag,
        source: Option<LocalNodeIndex>,
        path: Vec<ReplayPathSegment>,
        notify_done: bool,
        trigger: TriggerEndpoint,
    },

    /// Ask domain (nicely) to replay a particular key.
    RequestPartialReplay { //16
        tag: Tag,
        key: Vec<DataType>,
        unishard: bool,
    },

    /// Ask domain (nicely) to replay a particular key into a Reader.
    RequestReaderReplay { //17
        node: LocalNodeIndex,
        cols: Vec<usize>,
        key: Vec<DataType>,
    },

    /// Instruct domain to replay the state of a particular node along an existing replay path.
    StartReplay { //18
        tag: Tag,
        from: LocalNodeIndex,
    },

    /// Sent to instruct a domain that a particular node should be considered ready to process
    /// updates.
    Ready { // 19
        node: LocalNodeIndex,
        purge: bool,
        index: HashSet<Vec<usize>>,
    },

    /// Notification from Blender for domain to terminate
    Quit, //20

    /// A packet used solely to drive the event loop forward.
    Spin, //21

    /// Request that a domain send usage statistics on the control reply channel.
    /// Argument specifies if we wish to get the full state size or just the partial nodes.
    GetStatistics, //22

    /// Ask domain to log its state size
    UpdateStateSize, //23
}

impl Packet {
    crate fn src(&self) -> LocalNodeIndex {
        match *self {
            Packet::Input { ref inner, .. } => {
                // inputs come "from" the base table too
                unsafe { inner.deref() }.dst
            }
            Packet::Message { ref link, .. } => link.src,
            Packet::ReplayPiece { ref link, .. } => link.src,
            _ => unreachable!(),
        }
    }

    crate fn dst(&self) -> LocalNodeIndex {
        match *self {
            Packet::Input { ref inner, .. } => unsafe { inner.deref() }.dst,
            Packet::Message { ref link, .. } => link.dst,
            Packet::ReplayPiece { ref link, .. } => link.dst,
            _ => unreachable!(),
        }
    }

    crate fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::ReplayPiece { ref mut link, .. } => link,
            Packet::EvictKeys { ref mut link, .. } => link,
            _ => unreachable!(),
        }
    }

    crate fn is_empty(&self) -> bool {
        match *self {
            Packet::Message { ref data, .. } => data.is_empty(),
            Packet::ReplayPiece { ref data, .. } => data.is_empty(),
            _ => unreachable!(),
        }
    }

    crate fn map_data<F>(&mut self, map: F)
    where
        F: FnOnce(&mut Records),
    {
        match *self {
            Packet::Message { ref mut data, .. } | Packet::ReplayPiece { ref mut data, .. } => {
                map(data);
            }
            _ => {
                unreachable!();
            }
        }
    }

    crate fn is_regular(&self) -> bool {
        match *self {
            Packet::Message { .. } => true,
            _ => false,
        }
    }

    crate fn tag(&self) -> Option<Tag> {
        match *self {
            Packet::ReplayPiece { tag, .. } => Some(tag),
            Packet::EvictKeys { tag, .. } => Some(tag),
            _ => None,
        }
    }

    crate fn data(&self) -> &Records {
        match *self {
            Packet::Message { ref data, .. } => data,
            Packet::ReplayPiece { ref data, .. } => data,
            _ => unreachable!(),
        }
    }

    crate fn take_data(&mut self) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, Records::default())
    }

    crate fn clone_data(&self) -> Self {
        match *self {
            Packet::Message {
                link,
                ref data,
                ref tracer,
            } => Packet::Message {
                link,
                data: data.clone(),
                tracer: tracer.clone(),
            },
            Packet::ReplayPiece {
                link,
                tag,
                ref data,
                ref context,
            } => Packet::ReplayPiece {
                link,
                tag,
                data: data.clone(),
                context: context.clone(),
            },
            _ => unreachable!(),
        }
    }

    crate fn trace(&self, event: PacketEvent) {
        if let Packet::Message {
            tracer: Some((tag, Some(ref sender))),
            ..
        } = *self
        {
            use noria::debug::trace::{Event, EventType};
            sender
                .send(Event {
                    instant: time::Instant::now(),
                    event: EventType::PacketEvent(event, tag),
                })
                .unwrap();
        }
    }

    crate fn tracer(&mut self) -> Option<&mut Tracer> {
        match *self {
            Packet::Message { ref mut tracer, .. } => Some(tracer),
            _ => None,
        }
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Packet::Input { .. } => write!(f, "Packet::Input"),
            Packet::Message { ref link, .. } => write!(f, "Packet::Message({:?})", link),
            Packet::RequestReaderReplay { ref key, .. } => {
                write!(f, "Packet::RequestReaderReplay({:?})", key)
            }
            Packet::RequestPartialReplay { ref tag, .. } => {
                write!(f, "Packet::RequestPartialReplay({:?})", tag)
            },
            Packet::AddNode {ref node, ref parents, ref union_children } => {
                write!(f, "Packet::AddNode({:?})(parents:{:?}) children {:?}", node, parents, union_children)
            },
            Packet::Ready {ref node, ref purge, ref index} => {
                write!(f, "Packet::Ready node {:?}, purge: {:?}, index {:?}", node, purge, index)
            }
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ..
            } => write!(
                f,
                "Packet::ReplayPiece({:?}, tag {}, {} records)",
                link,
                tag.id(),
                data.len()
            ),
            ref p => {
                use std::mem;
                write!(f, "Packet::Control({:?})", mem::discriminant(p))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlReplyPacket {
    #[cfg(debug_assertions)]
    Ack(Backtrace),
    #[cfg(not(debug_assertions))]
    Ack(()),
    /// (number of rows, size in bytes)
    StateSize(usize, u64),
    Statistics(
        noria::debug::stats::DomainStats,
        HashMap<petgraph::graph::NodeIndex, noria::debug::stats::NodeStats>,
    ),
    Booted(usize, SocketAddr),
}

impl ControlReplyPacket {
    #[cfg(debug_assertions)]
    crate fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(Backtrace::new())
    }

    #[cfg(not(debug_assertions))]
    crate fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(())
    }
}
