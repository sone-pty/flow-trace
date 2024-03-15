#![feature(impl_trait_in_assoc_type)]
#![feature(lazy_cell)]
#![feature(async_closure)]

use std::{collections::HashMap, hash::Hash, mem::ManuallyDrop, sync::{atomic::AtomicU32, Arc, Mutex, RwLock}};
use handler::ServeHandler;
use slog::{error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;
use vnpkt::{tokio_ext::{io::AsyncReadExt, registry::Registry}, vector::VectorU8};
use vnsvrbase::tokio_ext::tcp_link::{send_pkt, TcpLink};

use crate::proto::{FlowType, PacketNodeStatus};

mod handler;
mod proto;

pub struct TraceServer<T: ExecStatus> {
    pub(crate) logger: slog::Logger,
    closer: tokio::sync::watch::Sender<bool>,
    templates: Arc<RwLock<HashMap<UUIDHashKey, TracerTemplate<T>>>>,
    tracers: Arc<Mutex<HashMap<TracerId, tokio::task::JoinHandle<()>>>>,
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

    pub fn build<T: ExecStatus + 'static>(self) -> (Arc<TraceServer<T>>, Arc<Registry<ServeHandler<T>>>, tokio::sync::watch::Receiver<bool>) {
        let logfile = file_rotate::FileRotate::new(
            self.path.as_str(),
            file_rotate::suffix::AppendTimestamp::default(file_rotate::suffix::FileLimit::MaxFiles(self.max_files)),
            file_rotate::ContentLimit::Lines(self.limit_lines),
            file_rotate::compression::Compression::None,
            #[cfg(unix)]
            None,
        );
        let decorator = slog_term::PlainDecorator::new(logfile);
        let drain = slog::Drain::fuse(slog_term::FullFormat::new(decorator).build());
        let drain = slog::Drain::fuse(slog_async::Async::new(drain)
            .chan_size(self.log_chan_size)
            .overflow_strategy(slog_async::OverflowStrategy::Block)
            .build());
        let logger = slog::Logger::root(drain, slog::o!());
        let (quit_tx, quit_rx) = tokio::sync::watch::channel(false);
        (Arc::new(TraceServer::new(logger, quit_tx)), Arc::new(Registry::new()), quit_rx)
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
            let rt = tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build()?;
            rt.block_on(async {
                tokio::spawn(f);
                let _ = quit_rx.changed().await;
                Ok::<(), std::io::Error>(())
            })
        }
    });
}

pub async fn receiving<T: ExecStatus + 'static>(link: &mut TcpLink, tracer: Arc<TraceServer<T>>, registry: Arc<Registry<ServeHandler<T>>>) -> std::io::Result<()> {
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

pub async fn main_loop<T: ExecStatus + 'static>(host: &str, tracer: Arc<TraceServer<T>>, registry: Arc<Registry<ServeHandler<T>>>) -> std::io::Result<()> {
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

impl<T: ExecStatus> TraceServer<T> {
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

    pub fn set_tracer_template_executor(&self, id: &Uuid, ptr: usize) -> bool {
        let mut write = self.templates.write().unwrap();
        let Some(tem) = write.get_mut(&UUIDHashKey(id.clone())) else { return false; };
        tem.executor = ptr;
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

    pub(crate) fn make_tracer(self: Arc<Self>, id: &Uuid, handle: vnsvrbase::tokio_ext::tcp_link::Handle) -> Option<Tracer<T>> {
        let mut write = self.templates.write().unwrap();
        write.get_mut(&UUIDHashKey(id.clone())).map(|v| {
            v.listen = true; 
            v.make_tracer(handle, self.clone())
        })
    }
    
    pub(crate) fn add_tracer(&self, id: TracerId, handle: tokio::task::JoinHandle<()>) -> Option<tokio::task::JoinHandle<()>> {
        let mut guard = self.tracers.lock().unwrap();
        guard.insert(id, handle)
    }

    pub(crate) fn del_tracer(&self, id: &TracerId) -> Option<tokio::task::JoinHandle<()>> {
        let mut guard = self.tracers.lock().unwrap();
        guard.remove(id)
    }
}

pub struct TracerTemplate<T: ExecStatus> {
    pub template: Uuid,
    pub id: Uuid,
    pub executor: usize,
    publisher: tokio::sync::broadcast::Sender<T>,
    listen: bool,
    seed: AtomicU32,
}

impl<T: ExecStatus> TracerTemplate<T> {
    pub fn make_tracer<'a>(&self, handle: vnsvrbase::tokio_ext::tcp_link::Handle, tracer: Arc<TraceServer<T>>) -> Tracer<T> {
        Tracer {
            id: TracerId(self.id.clone(), self.seed.fetch_add(1, std::sync::atomic::Ordering::Relaxed)),
            recv: ManuallyDrop::new(self.publisher.subscribe()),
            handle,
            tracer,
        }
    }

    pub fn new(template: Uuid, publisher: tokio::sync::broadcast::Sender<T>) -> Self {
        Self {
            template,
            id: Uuid::new_v4(),
            executor: 0,
            publisher,
            listen: false,
            seed: AtomicU32::new(0),
        }
    }
}

pub struct Tracer<T: ExecStatus> {
    id: TracerId,
    recv: ManuallyDrop<tokio::sync::broadcast::Receiver<T>>,
    handle: vnsvrbase::tokio_ext::tcp_link::Handle,
    tracer: Arc<TraceServer<T>>,
}

impl<T: ExecStatus> Drop for Tracer<T> {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.recv);
        }
        self.tracer.check_listen(&self.id.0);
    }
}

impl<T: ExecStatus> Tracer<T> {
    pub async fn proc(&mut self) {
        loop {
            match self.recv.recv().await {
                Ok(v) => {
                    let mut data = Vec::new();

                    if let Err(_) = v.serialize(&mut data) {
                        warn!(self.tracer.logger, "serialize ExecStatus data failed");
                    } else {
                        let Ok(ty) = FlowType::convert_from(<T as ExecStatus>::TY) else {
                            error!(self.tracer.logger, "invalid TY for ExecStatus: {}", <T as ExecStatus>::TY);
                            break;
                        };
                        
                        if data.len() > 0xFFFF {
                            error!(self.tracer.logger, "ExecStatus data out of 0xFFFF");
                            break;
                        }

                        let _ = send_pkt!(self.handle, PacketNodeStatus {
                            ty,
                            index: v.map(),
                            data: unsafe { VectorU8::from_unchecked(data) },
                        });
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(self.tracer.logger, "recv from chan lagged {} items", n);
                    continue;
                }
                _ => {
                    warn!(self.tracer.logger, "recv from closed chan");
                    break;
                }
            }
        }

        info!(self.tracer.logger, "tracer-{}-{} proc end", self.id.0, self.id.1);
        self.tracer.del_tracer(&self.id);
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

pub trait ExecStatus: Clone + Send {
    const TY: u8;
    fn map(&self) -> u32;
    fn serialize<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()>;
}

#[cfg(test)]
mod tests {
    use std::{io::Write, net::TcpStream};
    use uuid::Uuid;
    use vnpkt::packet_id::{PacketId, PacketIdExt};
    use vnutil::io::{ReadExt, WriteExt, WriteTo, ReadFrom};
    use crate::proto::{FlowType, PacketNodeStatus, ReqCancelListen};

    use super::proto::{self, ErrCode, PacketHB, ReqListenNode, RspListenNode};

    #[test]
    fn test_listen_node() {
        let mut conn = TcpStream::connect("127.0.0.1:9054").unwrap();
        let id: proto::Uuid = Uuid::parse_str("9034f2b7-9691-4628-8f84-e6caf7a8b00a").unwrap().into();
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
                            println!("can't find node for `9034f2b7-9691-4628-8f84-e6caf7a8b00a`");
                        }
                    } else if pid == <PacketNodeStatus as PacketId>::PID as _ {
                        let rsp = PacketNodeStatus::read_from(&mut conn).unwrap();
                        if rsp.ty == FlowType::BevTree {
                            let ptr = rsp.data.as_ptr();
                            let len = rsp.data.len();
                            let span = unsafe { *(std::slice::from_raw_parts(ptr, 16) as *const [u8] as *const [u8; 16]) };
                            let node = uuid::Uuid::from_bytes(span);
                            let index = rsp.index.to_be_bytes();
                            let index = u32::from_le_bytes(index);

                            match index {
                                0 => {
                                    println!("node {} pending", node);
                                }
                                1 => {
                                    println!("node {} abort", node);
                                }
                                2 => {
                                    if len > 16 {
                                        let res = unsafe { std::ptr::read(ptr.add(16)) };
                                        println!("node {} complete, result = {}", node, res);
                                    }
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