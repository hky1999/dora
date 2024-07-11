use crate::{daemon_connection::DaemonChannel, EventStream};

use self::{
    arrow_utils::{copy_array_into_sample, required_data_size},
    control_channel::ControlChannel,
};
use aligned_vec::{AVec, ConstAlign};
use arrow::array::Array;
use dora_core::{
    config::{DataId, NodeId, NodeRunConfig},
    daemon_messages::{
        DaemonCommunication, DaemonRequest, DataMessage, DataflowId, NodeConfig, Timestamped,
    },
    descriptor::Descriptor,
    message::{uhlc, ArrowTypeInfo, Metadata, MetadataParameters},
    topics::{DORA_DAEMON_LOCAL_LISTEN_PORT_DEFAULT, LOCALHOST},
};

use eyre::{bail, WrapErr};

#[cfg(feature = "shmem")]
use {
    self::drop_stream::DropStream,
    dora_core::daemon_messages::DropToken,
    shared_memory_extended::{Shmem, ShmemConf},
    std::{
        collections::{HashMap, VecDeque},
        time::Duration,
    },
};

use std::{
    net::IpAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tracing::info;

#[cfg(feature = "tracing")]
use dora_tracing::set_up_tracing;

pub mod arrow_utils;
mod control_channel;
#[cfg(feature = "shmem")]
mod drop_stream;

#[cfg(feature = "shmem")]
pub const ZERO_COPY_THRESHOLD: usize = 4096;

// To run without shared memory support.
#[cfg(not(feature = "shmem"))]
pub const ZERO_COPY_THRESHOLD: usize = usize::MAX;

pub struct DoraNode {
    id: NodeId,
    dataflow_id: DataflowId,
    node_config: NodeRunConfig,
    control_channel: ControlChannel,
    clock: Arc<uhlc::HLC>,

    #[cfg(feature = "shmem")]
    sent_out_shared_memory: HashMap<DropToken, ShmemHandle>,
    #[cfg(feature = "shmem")]
    drop_stream: DropStream,
    #[cfg(feature = "shmem")]
    cache: VecDeque<ShmemHandle>,

    dataflow_descriptor: Descriptor,
}

impl DoraNode {
    /// Initiate a node from environment variables set by `dora-coordinator`
    ///
    /// ```no_run
    /// use dora_node_api::DoraNode;
    ///
    /// let (mut node, mut events) = DoraNode::init_from_env().expect("Could not init node.");
    /// ```
    ///
    pub fn init_from_env() -> eyre::Result<(Self, EventStream)> {
        let node_config: NodeConfig = {
            let raw = std::env::var("DORA_NODE_CONFIG").wrap_err(
                "env variable DORA_NODE_CONFIG must be set. Are you sure your using `dora start`?",
            )?;
            serde_yaml::from_str(&raw).context("failed to deserialize operator config")?
        };
        #[cfg(feature = "tracing")]
        set_up_tracing(&node_config.node_id.to_string())
            .context("failed to set up tracing subscriber")?;
        Self::init(node_config)
    }

    /// Initiate a node from a specified file.
    ///
    /// ```no_run
    /// use dora_node_api::DoraNode;
    ///
    ///
    /// let (mut node, mut events) = DoraNode::init_from_file(file_path).expect("Could not init node.");
    /// ```
    ///
    pub fn init_from_file(file_path: &str) -> eyre::Result<(Self, EventStream)> {
        println!("DoraNode init from file {}", file_path);
        let node_config: NodeConfig = {
            let raw = std::fs::read_to_string(file_path).expect("failed to read from file");
            serde_yaml::from_str(&raw).context("failed to deserialize operator config")?
        };
        #[cfg(feature = "tracing")]
        set_up_tracing(&node_config.node_id.to_string())
            .context("failed to set up tracing subscriber")?;
        Self::init(node_config)
    }

    /// Initiate a node from a dataflow id and a node id.
    ///
    /// ```no_run
    /// use dora_node_api::DoraNode;
    /// use dora_node_api::dora_core::config::NodeId;
    ///
    ///     let (_node, mut events) = DoraNode::init_from_node_id(
    ///         NodeId::from("arceos-node-dynamic".to_string()),
    ///         Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
    ///     ).expect("Could not init node plot");
    /// ```
    ///
    pub fn init_from_node_id(
        node_id: NodeId,
        remote_addr: Option<IpAddr>,
    ) -> eyre::Result<(Self, EventStream)> {
        // Make sure that the node is initialized outside of dora start.
        let remote_ip = if let Some(ip) = remote_addr {
            ip
        } else {
            LOCALHOST
        };
        let daemon_address = (remote_ip, DORA_DAEMON_LOCAL_LISTEN_PORT_DEFAULT).into();

        let mut channel =
            DaemonChannel::new_tcp(daemon_address).context("Could not connect to the daemon")?;
        let clock = Arc::new(uhlc::HLC::default());

        let reply = channel
            .request(&Timestamped {
                inner: DaemonRequest::NodeConfig { node_id },
                timestamp: clock.new_timestamp(),
            })
            .wrap_err("failed to request node config from daemon")?;
        match reply {
            dora_core::daemon_messages::DaemonReply::NodeConfig {
                result: Ok(mut node_config),
            } => {
                println!(
                    "[TRACE] get node_config from daemon, DaemonCommunication {:#?}",
                    node_config.daemon_communication
                );

                // Replace the `socket_addr` in the `node_config` returned by daemon
                // with the `remote_ip` of the machine where daemon is located.
                // Todo: make it more elgant.
                #[allow(unreachable_patterns)]
                match node_config.daemon_communication {
                    DaemonCommunication::Tcp { socket_addr } => {
                        println!(
                            "[TRACE] DaemonCommunication::Tcp socket_addr {:?}",
                            socket_addr,
                        );

                        node_config.daemon_communication = DaemonCommunication::Tcp {
                            socket_addr: (remote_ip, socket_addr.port()).into(),
                        };

                        println!(
                            "[TRACE] DaemonCommunication modify to {:#?}",
                            node_config.daemon_communication
                        );
                    }
                    _ => {} // Do nothing if it's `Shmem`.
                };
                Self::init(node_config)
            }
            dora_core::daemon_messages::DaemonReply::NodeConfig { result: Err(error) } => {
                bail!("failed to get node config from daemon: {error}")
            }
            _ => bail!("unexpected reply from daemon"),
        }
    }

    pub fn init_flexible(node_id: NodeId) -> eyre::Result<(Self, EventStream)> {
        if std::env::var("DORA_NODE_CONFIG").is_ok() {
            info!("Skipping {node_id} specified within the node initialization in favor of `DORA_NODE_CONFIG` specified by `dora start`");
            Self::init_from_env()
        } else {
            Self::init_from_node_id(node_id, None)
        }
    }

    #[tracing::instrument]
    pub fn init(node_config: NodeConfig) -> eyre::Result<(Self, EventStream)> {
        let NodeConfig {
            dataflow_id,
            node_id,
            run_config,
            daemon_communication,
            dataflow_descriptor,
            dynamic: _,
        } = node_config;
        let clock = Arc::new(uhlc::HLC::default());

        let event_stream =
            EventStream::init(dataflow_id, &node_id, &daemon_communication, clock.clone())
                .wrap_err("failed to init event stream")?;
        #[cfg(feature = "shmem")]
        let drop_stream =
            DropStream::init(dataflow_id, &node_id, &daemon_communication, clock.clone())
                .wrap_err("failed to init drop stream")?;
        let control_channel =
            ControlChannel::init(dataflow_id, &node_id, &daemon_communication, clock.clone())
                .wrap_err("failed to init control channel")?;

        println!("[TRACE] all streams init success");

        let node = Self {
            id: node_id,
            dataflow_id,
            node_config: run_config.clone(),
            control_channel,
            clock,
            #[cfg(feature = "shmem")]
            sent_out_shared_memory: HashMap::new(),
            #[cfg(feature = "shmem")]
            drop_stream,
            #[cfg(feature = "shmem")]
            cache: VecDeque::new(),
            dataflow_descriptor,
        };
        Ok((node, event_stream))
    }

    /// Send data from the node to the other nodes.
    /// We take a closure as an input to enable zero copy on send.
    ///
    /// ```no_run
    /// use dora_node_api::{DoraNode, MetadataParameters};
    /// use dora_core::config::DataId;
    ///
    /// let (mut node, mut events) = DoraNode::init_from_env().expect("Could not init node.");
    ///
    /// let output = DataId::from("output_id".to_owned());
    ///
    /// let data: &[u8] = &[0, 1, 2, 3];
    /// let parameters = MetadataParameters::default();
    ///
    /// node.send_output_raw(
    ///    output,
    ///    parameters,
    ///    data.len(),
    ///    |out| {
    ///         out.copy_from_slice(data);
    ///     }).expect("Could not send output");
    /// ```
    ///
    pub fn send_output_raw<F>(
        &mut self,
        output_id: DataId,
        parameters: MetadataParameters,
        data_len: usize,
        data: F,
    ) -> eyre::Result<()>
    where
        F: FnOnce(&mut [u8]),
    {
        let mut sample = self.allocate_data_sample(data_len)?;
        data(&mut sample);

        let type_info = ArrowTypeInfo::byte_array(data_len);

        self.send_output_sample(output_id, type_info, parameters, Some(sample))
    }

    pub fn send_output(
        &mut self,
        output_id: DataId,
        parameters: MetadataParameters,
        data: impl Array,
    ) -> eyre::Result<()> {
        let arrow_array = data.to_data();

        let total_len = required_data_size(&arrow_array);

        let mut sample = self.allocate_data_sample(total_len)?;
        let type_info = copy_array_into_sample(&mut sample, &arrow_array);

        self.send_output_sample(output_id, type_info, parameters, Some(sample))
            .wrap_err("failed to send output")?;

        Ok(())
    }

    pub fn send_output_bytes(
        &mut self,
        output_id: DataId,
        parameters: MetadataParameters,
        data_len: usize,
        data: &[u8],
    ) -> eyre::Result<()> {
        self.send_output_raw(output_id, parameters, data_len, |sample| {
            sample.copy_from_slice(data)
        })
    }

    pub fn send_typed_output<F>(
        &mut self,
        output_id: DataId,
        type_info: ArrowTypeInfo,
        parameters: MetadataParameters,
        data_len: usize,
        data: F,
    ) -> eyre::Result<()>
    where
        F: FnOnce(&mut [u8]),
    {
        let mut sample = self.allocate_data_sample(data_len)?;
        data(&mut sample);

        self.send_output_sample(output_id, type_info, parameters, Some(sample))
    }

    pub fn send_output_sample(
        &mut self,
        output_id: DataId,
        type_info: ArrowTypeInfo,
        parameters: MetadataParameters,
        sample: Option<DataSample>,
    ) -> eyre::Result<()> {
        #[cfg(feature = "shmem")]
        self.handle_finished_drop_tokens()?;

        if !self.node_config.outputs.contains(&output_id) {
            eyre::bail!("unknown output");
        }
        let metadata = Metadata::from_parameters(
            self.clock.new_timestamp(),
            type_info,
            parameters.into_owned(),
        );

        let data = match sample {
            Some(sample) => match sample.finalize() {
                DataSampleType::Vec(data) => Some(data),
                #[cfg(feature = "shmem")]
                DataSampleType::Shmem(data, shared_memory, drop_token) => {
                    self.sent_out_shared_memory
                        .insert(drop_token, shared_memory);
                    Some(data)
                }
            },
            None => None,
        };

        self.control_channel
            .send_message(output_id.clone(), metadata, data)
            .wrap_err_with(|| format!("failed to send output {output_id}"))?;

        Ok(())
    }

    pub fn close_outputs(&mut self, outputs: Vec<DataId>) -> eyre::Result<()> {
        for output_id in &outputs {
            if !self.node_config.outputs.remove(output_id) {
                eyre::bail!("unknown output {output_id}");
            }
        }

        self.control_channel
            .report_closed_outputs(outputs)
            .wrap_err("failed to report closed outputs to daemon")?;

        Ok(())
    }

    pub fn id(&self) -> &NodeId {
        &self.id
    }

    pub fn dataflow_id(&self) -> &DataflowId {
        &self.dataflow_id
    }

    pub fn node_config(&self) -> &NodeRunConfig {
        &self.node_config
    }

    pub fn allocate_data_sample(&mut self, data_len: usize) -> eyre::Result<DataSample> {
        #[cfg(feature = "shmem")]
        let data = if data_len >= ZERO_COPY_THRESHOLD {
            // create shared memory region
            let shared_memory = self.allocate_shared_memory(data_len)?;

            DataSample {
                inner: DataSampleInner::Shmem(shared_memory),
                len: data_len,
            }
        } else {
            let avec: AVec<u8, ConstAlign<128>> = AVec::__from_elem(128, 0, data_len);

            avec.into()
        };

        #[cfg(not(feature = "shmem"))]
        let data = {
            let avec: AVec<u8, ConstAlign<128>> = AVec::__from_elem(128, 0, data_len);
            avec.into()
        };

        Ok(data)
    }

    #[cfg(feature = "shmem")]
    fn allocate_shared_memory(&mut self, data_len: usize) -> eyre::Result<ShmemHandle> {
        let cache_index = self
            .cache
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, s)| s.len() >= data_len)
            .min_by_key(|(_, s)| s.len())
            .map(|(i, _)| i);
        let memory = match cache_index {
            Some(i) => {
                // we know that this index exists, so we can safely unwrap here
                self.cache.remove(i).unwrap()
            }
            None => ShmemHandle(Box::new(
                ShmemConf::new()
                    .size(data_len)
                    .writable(true)
                    .create()
                    .wrap_err("failed to allocate shared memory")?,
            )),
        };
        assert!(memory.len() >= data_len);

        Ok(memory)
    }

    #[cfg(feature = "shmem")]
    fn handle_finished_drop_tokens(&mut self) -> eyre::Result<()> {
        loop {
            match self.drop_stream.try_recv() {
                Ok(token) => match self.sent_out_shared_memory.remove(&token) {
                    Some(region) => self.add_to_cache(region),
                    None => tracing::warn!("received unknown finished drop token `{token:?}`"),
                },
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    bail!("event stream was closed before sending all expected drop tokens")
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "shmem")]
    fn add_to_cache(&mut self, memory: ShmemHandle) {
        const MAX_CACHE_SIZE: usize = 20;

        self.cache.push_back(memory);
        while self.cache.len() > MAX_CACHE_SIZE {
            self.cache.pop_front();
        }
    }

    /// Returns the full dataflow descriptor that this node is part of.
    ///
    /// This method returns the parsed dataflow YAML file.
    pub fn dataflow_descriptor(&self) -> &Descriptor {
        &self.dataflow_descriptor
    }
}

impl Drop for DoraNode {
    #[tracing::instrument(skip(self), fields(self.id = %self.id), level = "trace")]
    fn drop(&mut self) {
        // close all outputs first to notify subscribers as early as possible
        if let Err(err) = self
            .control_channel
            .report_closed_outputs(
                std::mem::take(&mut self.node_config.outputs)
                    .into_iter()
                    .collect(),
            )
            .context("failed to close outputs on drop")
        {
            tracing::warn!("{err:?}")
        }

        #[cfg(feature = "shmem")]
        while !self.sent_out_shared_memory.is_empty() {
            if self.drop_stream.len() == 0 {
                tracing::trace!(
                    "waiting for {} remaining drop tokens",
                    self.sent_out_shared_memory.len()
                );
            }

            match self.drop_stream.recv_timeout(Duration::from_millis(500)) {
                Ok(token) => {
                    self.sent_out_shared_memory.remove(&token);
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    tracing::warn!(
                        "finished_drop_tokens channel closed while still waiting for drop tokens; \
                        closing {} shared memory regions that might still be used",
                        self.sent_out_shared_memory.len()
                    );
                    break;
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    tracing::warn!(
                        "timeout while waiting for drop tokens; \
                        closing {} shared memory regions that might still be used",
                        self.sent_out_shared_memory.len()
                    );
                    break;
                }
            }
        }

        if let Err(err) = self.control_channel.report_outputs_done() {
            tracing::warn!("{err:?}")
        }
    }
}

pub struct DataSample {
    inner: DataSampleInner,
    len: usize,
}

enum DataSampleType {
    Vec(DataMessage),
    #[cfg(feature = "shmem")]
    Shmem(DataMessage, ShmemHandle, DropToken),
}

impl DataSample {
    fn finalize(self) -> DataSampleType {
        match self.inner {
            #[cfg(feature = "shmem")]
            DataSampleInner::Shmem(shared_memory) => {
                let drop_token = DropToken::generate();
                let data = DataMessage::SharedMemory {
                    shared_memory_id: shared_memory.get_os_id().to_owned(),
                    len: self.len,
                    drop_token,
                };
                DataSampleType::Shmem(data, shared_memory, drop_token)
            }
            DataSampleInner::Vec(buffer) => DataSampleType::Vec(DataMessage::Vec(buffer)),
        }
    }
}

impl Deref for DataSample {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let slice = match &self.inner {
            #[cfg(feature = "shmem")]
            DataSampleInner::Shmem(handle) => unsafe { handle.as_slice() },
            DataSampleInner::Vec(data) => data,
        };
        &slice[..self.len]
    }
}

impl DerefMut for DataSample {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let slice = match &mut self.inner {
            #[cfg(feature = "shmem")]
            DataSampleInner::Shmem(handle) => unsafe { handle.as_slice_mut() },
            DataSampleInner::Vec(data) => data,
        };
        &mut slice[..self.len]
    }
}

impl From<AVec<u8, ConstAlign<128>>> for DataSample {
    fn from(value: AVec<u8, ConstAlign<128>>) -> Self {
        Self {
            len: value.len(),
            inner: DataSampleInner::Vec(value),
        }
    }
}

impl std::fmt::Debug for DataSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match &self.inner {
            #[cfg(feature = "shmem")]
            DataSampleInner::Shmem(_) => "SharedMemory",
            DataSampleInner::Vec(_) => "Vec",
        };
        f.debug_struct("DataSample")
            .field("len", &self.len)
            .field("kind", &kind)
            .finish_non_exhaustive()
    }
}

enum DataSampleInner {
    #[cfg(feature = "shmem")]
    Shmem(ShmemHandle),
    Vec(AVec<u8, ConstAlign<128>>),
}

cfg_if::cfg_if! {
    if #[cfg(feature = "shmem")] {
        struct ShmemHandle(Box<Shmem>);

        impl Deref for ShmemHandle {
            type Target = Shmem;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for ShmemHandle {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        unsafe impl Send for ShmemHandle {}
        unsafe impl Sync for ShmemHandle {}
    }
}
