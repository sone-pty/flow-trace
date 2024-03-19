#![feature(impl_trait_in_assoc_type)]
#![feature(lazy_cell)]
#![feature(async_closure)]
#![feature(new_uninit)]

use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    mem::ManuallyDrop,
    sync::{atomic::AtomicU32, Arc, Mutex, RwLock},
};

use handler::ServeHandler;
use slog::{error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;
use vnpkt::{
    tokio_ext::{io::AsyncReadExt, registry::Registry},
    vector::VectorU8,
};
use vnsvrbase::tokio_ext::tcp_link::{send_pkt, TcpLink};

use crate::proto::{FlowType, PacketInstanceClose, PacketNodeEvent, PacketNodeStatus};

mod handler;
mod map;
mod proto;

pub struct TraceServer<T: ExecMsg> {
    pub(crate) logger: slog::Logger,
    closer: tokio::sync::watch::Sender<bool>,
    templates: Arc<RwLock<HashMap<UUIDHashKey, TracerTemplate<T>>>>,
    tracers: Arc<Mutex<HashMap<TracerId, tokio::task::JoinHandle<std::io::Result<()>>>>>,
}

#[derive(Default)]
pub struct Builder {
    path: String,
    max_files: usize,
    limit_lines: usize,
    log_chan_size: usize,
}

impl Builder {
    pub fn with_log_path(mut self, path: &str) -> Self {
        self.path = path.into();
        self
    }

    pub fn with_max_files(mut self, size: usize) -> Self {
        self.max_files = size;
        self
    }

    pub fn with_limit_lines(mut self, lines: usize) -> Self {
        self.limit_lines = lines;
        self
    }

    pub fn with_log_chan_size(mut self, size: usize) -> Self {
        self.log_chan_size = size;
        self
    }

    pub fn build<T: ExecMsg + 'static>(
        self,
    ) -> (
        Arc<TraceServer<T>>,
        Arc<Registry<ServeHandler<T>>>,
        tokio::sync::watch::Receiver<bool>,
    ) {
        let logfile = file_rotate::FileRotate::new(
            self.path.as_str(),
            file_rotate::suffix::AppendTimestamp::default(
                file_rotate::suffix::FileLimit::MaxFiles(self.max_files),
            ),
            file_rotate::ContentLimit::Lines(self.limit_lines),
            file_rotate::compression::Compression::None,
            #[cfg(unix)]
            None,
        );
        let decorator = slog_term::PlainDecorator::new(logfile);
        let drain = slog::Drain::fuse(slog_term::FullFormat::new(decorator).build());
        let drain = slog::Drain::fuse(
            slog_async::Async::new(drain)
                .chan_size(self.log_chan_size)
                .overflow_strategy(slog_async::OverflowStrategy::Block)
                .build(),
        );
        let logger = slog::Logger::root(drain, slog::o!());
        let (quit_tx, quit_rx) = tokio::sync::watch::channel(false);
        (
            Arc::new(TraceServer::new(logger, quit_tx)),
            Arc::new(Registry::new()),
            quit_rx,
        )
    }
}

pub fn build<F>(f: F, quit_rx: tokio::sync::watch::Receiver<bool>)
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    std::thread::spawn({
        let mut quit_rx = quit_rx;
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()?;
            rt.block_on(async {
                tokio::spawn(f);
                let _ = quit_rx.changed().await;
                Ok::<(), std::io::Error>(())
            })
        }
    });
}

pub async fn receiving<T: ExecMsg + 'static>(
    link: &mut TcpLink,
    tracer: Arc<TraceServer<T>>,
    registry: Arc<Registry<ServeHandler<T>>>,
) -> std::io::Result<()> {
    loop {
        let pid = link.read.read_compressed_u64().await?;

        if pid <= u32::MAX as _ {
            let mut client = ServeHandler::new(link.handle().clone(), tracer.clone());
            if let Some(item) = registry.query(pid as u32) {
                let r = item.recv(&mut link.read).await?;
                r.proc(&mut client).await?;
            } else {
                warn!(tracer.logger, "recv invalid pid: {}", pid);
                return Err(std::io::ErrorKind::InvalidData.into());
            }
        } else {
            warn!(tracer.logger, "recv invalid pid: {}", pid);
            return Err(std::io::ErrorKind::InvalidData.into());
        }
    }
}

pub async fn main_loop<T: ExecMsg + 'static>(
    host: &str,
    tracer: Arc<TraceServer<T>>,
    registry: Arc<Registry<ServeHandler<T>>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(host).await?;
    let handle = tokio::runtime::Handle::current();
    let (sender, mut recv) = tokio::sync::mpsc::channel::<TcpStream>(4096);
    let sender_clone = sender.clone();
    let logger = tracer.logger.clone();

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let _ = sender_clone.send(stream).await;
                }
                Err(e) => {
                    error!(logger, "main listener accept failed: {}", e);
                }
            }
        }
    });

    while let Some(stream) = recv.recv().await {
        let _ = stream.set_nodelay(true);
        let _ = stream.set_linger(None);
        let registry_clone = registry.clone();
        let tracer_clone = tracer.clone();
        TcpLink::attach(stream, &handle, &handle, async move |link| {
            receiving(link, tracer_clone, registry_clone).await
        });
    }

    Ok(())
}

impl<T: ExecMsg> TraceServer<T> {
    pub(crate) fn new(logger: slog::Logger, quit_tx: tokio::sync::watch::Sender<bool>) -> Self {
        Self {
            logger,
            closer: quit_tx,
            templates: Arc::new(RwLock::new(HashMap::new())),
            tracers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn start(&self) {
        info!(self.logger, "trace server start");
    }

    pub fn stop(&self) {
        let _ = self.closer.send(true);
    }

    pub fn add_tracer_template(&self, tracer: TracerTemplate<T>) -> Option<TracerTemplate<T>> {
        let mut write = self.templates.write().unwrap();
        write.insert(UUIDHashKey(tracer.id), tracer)
    }

    pub fn del_tracer_template(&self, id: &Uuid) -> Option<TracerTemplate<T>> {
        let mut write = self.templates.write().unwrap();
        write.remove(&UUIDHashKey(id.clone()))
    }

    pub fn set_tracer_template_executor<E: ExecutorInfo>(&self, id: &Uuid, ptr: &E) -> bool {
        let mut write = self.templates.write().unwrap();
        let Some(tem) = write.get_mut(&UUIDHashKey(id.clone())) else {
            return false;
        };
        tem.executor = ptr as *const E as _;
        true
    }

    pub fn is_listen(&self, id: &Uuid) -> bool {
        let read = self.templates.read().unwrap();
        read.get(&UUIDHashKey(id.clone())).is_some_and(|v| v.listen)
    }

    pub(crate) fn check_listen(&self, id: &Uuid) {
        let mut write = self.templates.write().unwrap();
        let Some(tem) = write.get_mut(&UUIDHashKey(id.clone())) else {
            return;
        };
        if tem.publisher.receiver_count() == 0 {
            tem.listen = false;
        }
    }

    pub(crate) fn make_tracer(
        self: Arc<Self>,
        id: &Uuid,
        handle: vnsvrbase::tokio_ext::tcp_link::Handle,
    ) -> Option<Tracer<T>> {
        let mut write = self.templates.write().unwrap();
        write.get_mut(&UUIDHashKey(id.clone())).map(|v| {
            v.listen = true;
            v.make_tracer(handle, self.clone())
        })
    }

    pub(crate) fn add_tracer(
        &self,
        id: TracerId,
        handle: tokio::task::JoinHandle<std::io::Result<()>>,
    ) -> Option<tokio::task::JoinHandle<std::io::Result<()>>> {
        let mut guard = self.tracers.lock().unwrap();
        guard.insert(id, handle)
    }

    pub(crate) fn del_tracer(
        &self,
        id: &TracerId,
    ) -> Option<tokio::task::JoinHandle<std::io::Result<()>>> {
        let mut guard = self.tracers.lock().unwrap();
        guard.remove(id)
    }
}

pub struct TracerTemplate<T: ExecMsg> {
    pub template: Uuid,
    pub id: Uuid,
    pub executor: usize,
    publisher: tokio::sync::broadcast::Sender<T>,
    listen: bool,
    seed: AtomicU32,
}

impl<T: ExecMsg> TracerTemplate<T> {
    pub fn make_tracer(
        &self,
        handle: vnsvrbase::tokio_ext::tcp_link::Handle,
        tracer: Arc<TraceServer<T>>,
    ) -> Tracer<T> {
        Tracer {
            id: TracerId(
                self.id.clone(),
                self.seed.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ),
            chan_rx: ManuallyDrop::new(self.publisher.subscribe()),
            handle,
            tracer,
        }
    }

    pub fn build(template: Uuid, tracer: &TraceServer<T>) -> (Uuid, Sender<T>) {
        let id = Uuid::new_v4();
        let (sx, _) = tokio::sync::broadcast::channel(1024);
        let tracer_template = Self {
            template,
            id: id.clone(),
            executor: 0,
            publisher: sx.clone(),
            listen: false,
            seed: AtomicU32::new(0),
        };
        tracer.add_tracer_template(tracer_template);
        (id, Sender(sx))
    }
}

pub struct Tracer<T: ExecMsg> {
    id: TracerId,
    chan_rx: ManuallyDrop<tokio::sync::broadcast::Receiver<T>>,
    handle: vnsvrbase::tokio_ext::tcp_link::Handle,
    tracer: Arc<TraceServer<T>>,
}

impl<T: ExecMsg> Drop for Tracer<T> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.chan_rx);
        }
        self.tracer.check_listen(&self.id.0);
    }
}

impl<T: ExecMsg> Tracer<T> {
    pub async fn proc(&mut self) -> std::io::Result<()> {
        let Ok(ty) = FlowType::convert_from(<T as ExecMsg>::TY) else {
            error!(
                self.tracer.logger,
                "invalid TY for ExecStatus: {}",
                <T as ExecMsg>::TY
            );
            return Ok(());
        };

        loop {
            match self.chan_rx.recv().await {
                Ok(ref v) => {
                    if v.is_event() {
                        let (uuid, index) = v
                            .map_event()
                            .ok_or(std::io::Error::from(std::io::ErrorKind::Other))?;
                        let mut meta = Vec::new();
                        let ret = v.build_event_meta(&mut meta)?;
                        let _ = send_pkt!(
                            self.handle,
                            PacketNodeEvent {
                                ty,
                                nid: uuid.clone().into(),
                                index,
                                meta: if ret {
                                    Some(unsafe { VectorU8::from_unchecked(meta) })
                                } else {
                                    None
                                },
                            }
                        );
                    } else {
                        let (uuid, index) = v
                            .map_status()
                            .ok_or(std::io::Error::from(std::io::ErrorKind::Other))?;
                        let _ = send_pkt!(
                            self.handle,
                            PacketNodeStatus {
                                ty,
                                nid: uuid.clone().into(),
                                index,
                            }
                        );
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(self.tracer.logger, "recv from chan lagged {} items", n);
                    continue;
                }
                _ => {
                    info!(self.tracer.logger, "recv from closed chan");
                    self.tracer.del_tracer(&self.id);
                    let _ = send_pkt!(
                        self.handle,
                        PacketInstanceClose {
                            ty,
                            id: self.id.0.clone().into(),
                        }
                    );
                    break;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
#[repr(transparent)]
struct UUIDHashKey(Uuid);

impl PartialEq for UUIDHashKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes()[0..8].eq(&other.0.as_bytes()[0..8])
    }
}

impl Eq for UUIDHashKey {}

impl Hash for UUIDHashKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_bytes()[0..8].hash(state)
    }
}

impl From<Uuid> for UUIDHashKey {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<&[u8; 16]> for UUIDHashKey {
    fn from(value: &[u8; 16]) -> Self {
        Self(Uuid::from_bytes(*value))
    }
}

#[derive(Clone)]
pub struct TracerId(Uuid, u32);

impl PartialEq for TracerId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for TracerId {}

impl Hash for TracerId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
        self.1.hash(state);
    }
}

pub trait ExecMsg: Clone + Send + Debug {
    const TY: u8;
    fn map_status(&self) -> Option<(&uuid::Uuid, u32)>;
    fn map_event(&self) -> Option<(&uuid::Uuid, u32)>;
    fn is_event(&self) -> bool;
    fn build_event_meta<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<bool>;
}

#[repr(transparent)]
pub struct Sender<T: ExecMsg>(tokio::sync::broadcast::Sender<T>);

impl<T: ExecMsg + 'static> Sender<T> {
    pub fn send(&self, info: T) -> Result<usize, Box<dyn std::error::Error>> {
        self.0.send(info).map_err(|e| Box::new(e) as _)
    }
}

pub trait ExecutorInfo {
    fn current_info<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()>;
}

#[cfg(test)]
mod tests {
    use std::{io::Write, net::TcpStream};

    use uuid::Uuid;
    use vnpkt::packet_id::{PacketId, PacketIdExt};
    use vnutil::io::{ReadExt, ReadFrom, WriteExt, WriteTo};

    use super::proto::{self, ErrCode, PacketHB, ReqListenNode, RspListenNode};
    use crate::proto::{FlowType, PacketNodeEvent, PacketNodeStatus, ReqCancelListen};

    #[test]
    fn test_listen_node() {
        let mut conn = TcpStream::connect("127.0.0.1:9054").unwrap();
        let id: proto::Uuid = Uuid::parse_str("cfcca7fb-4bdd-4174-a1f2-9a3ff2500458")
            .unwrap()
            .into();
        let pkt = ReqListenNode { id: id.clone() };
        let mut bytes = Vec::<u8>::with_capacity(1024);
        let _ = bytes.write_compressed_u64(pkt.pid() as _);
        let _ = pkt.write_to(&mut bytes);
        let _ = conn.write_all(&bytes);
        let mut tid = 0;
        let mut cnt = 0;

        loop {
            if cnt == 20 {
                break;
            } else {
                cnt += 1;
            }

            match conn.read_compressed_u64() {
                Ok(pid) => {
                    if pid == <RspListenNode as PacketId>::PID as _ {
                        let rsp = RspListenNode::read_from(&mut conn).unwrap();
                        if rsp.code == ErrCode::Ok {
                            println!("listen success");
                            tid = rsp.tid.unwrap();
                        } else {
                            println!("can't find node for `{}`", Into::<Uuid>::into(rsp.id));
                        }
                    } else if pid == <PacketNodeStatus as PacketId>::PID as _ {
                        let rsp = PacketNodeStatus::read_from(&mut conn).unwrap();
                        if rsp.ty == FlowType::BevTree {
                            if rsp.index == 1 {
                                println!(
                                    "[status] node {} is pending",
                                    Into::<Uuid>::into(rsp.nid)
                                );
                            }
                        }
                    } else if pid == <PacketNodeEvent as PacketId>::PID as _ {
                        let rsp = PacketNodeEvent::read_from(&mut conn).unwrap();
                        if rsp.ty == FlowType::BevTree {
                            match rsp.index {
                                1 => {
                                    println!("[event] node {} abort", Into::<Uuid>::into(rsp.nid));
                                }
                                2 => {
                                    println!(
                                        "[event] node {} done, result = true",
                                        Into::<Uuid>::into(rsp.nid)
                                    );
                                }
                                3 => {
                                    println!(
                                        "[event] node {} done, result = false",
                                        Into::<Uuid>::into(rsp.nid)
                                    );
                                }
                                _ => {}
                            }
                        }
                    } else if pid == <PacketHB as PacketId>::PID as _ {
                        let _ = PacketHB::read_from(&mut conn).unwrap();
                    }
                }
                _ => {
                    break;
                }
            }
        }

        let close = ReqCancelListen {
            id: id.clone(),
            tid,
        };
        println!("tid = {}", tid);
        bytes.clear();
        let _ = bytes.write_compressed_u64(close.pid() as _);
        let _ = close.write_to(&mut bytes);
        let _ = conn.write_all(&bytes);
    }
}
