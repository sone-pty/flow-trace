use std::{future::Future, sync::Arc};

use slog::info;
use tokio::{io::BufReader, net::tcp::OwnedReadHalf};
use vnpkt::tokio_ext::registry::{PacketProc, RegistryInit};
use vnsvrbase::tokio_ext::tcp_link::send_pkt;

use super::{
    proto::{ErrCode, PacketHB, ReqCancelListen, ReqListenNode, RspCancelListen, RspListenNode},
    TracerId,
};
use crate::{ExecMsg, TraceServer};

fn conv_u8_to_uuid(data: &vnpkt::vector::VectorU8<16>) -> uuid::Uuid {
    let mut input = [0u8; 16];
    input.iter_mut().enumerate().for_each(|(i, v)| *v = data[i]);
    uuid::Uuid::from_bytes(input)
}

fn conv_uuid_to_u8(data: &uuid::Uuid) -> vnpkt::vector::VectorU8<16> {
    let mut vec = Vec::with_capacity(16);
    data.as_bytes().iter().for_each(|b| vec.push(*b));
    unsafe { vnpkt::vector::VectorU8::from_unchecked(vec) }
}

impl From<super::proto::Uuid> for uuid::Uuid {
    fn from(value: super::proto::Uuid) -> Self {
        conv_u8_to_uuid(&value.id)
    }
}

impl From<uuid::Uuid> for super::proto::Uuid {
    fn from(value: uuid::Uuid) -> Self {
        Self {
            id: conv_uuid_to_u8(&value),
        }
    }
}

pub struct ServeHandler<T: ExecMsg> {
    handle: vnsvrbase::tokio_ext::tcp_link::Handle,
    tracer: Arc<TraceServer<T>>,
}

impl<T: ExecMsg> ServeHandler<T> {
    pub fn new(
        handle: vnsvrbase::tokio_ext::tcp_link::Handle,
        tracer: Arc<TraceServer<T>>,
    ) -> Self {
        Self { handle, tracer }
    }
}

impl<T: ExecMsg + 'static> RegistryInit for ServeHandler<T> {
    type AsyncRead = BufReader<OwnedReadHalf>;

    fn init(register: &mut vnpkt::tokio_ext::registry::Registry<Self>) {
        register.insert::<PacketHB>();
        register.insert::<ReqListenNode>();
        register.insert::<ReqCancelListen>();
    }
}

impl<T: ExecMsg> PacketProc<PacketHB> for ServeHandler<T> {
    type Output<'a> = impl Future<Output = std::io::Result<()>> + 'a where Self: 'a;

    fn proc(&mut self, pkt: Box<PacketHB>) -> Self::Output<'_> {
        async {
            let _ = send_pkt!(self.handle, pkt);
            Ok(())
        }
    }
}

impl<T: ExecMsg + 'static> PacketProc<ReqListenNode> for ServeHandler<T> {
    type Output<'a> = impl Future<Output = std::io::Result<()>> + 'a where Self: 'a;

    fn proc(&mut self, pkt: Box<ReqListenNode>) -> Self::Output<'_> {
        async move {
            let mut rsp = RspListenNode {
                id: pkt.id.clone(),
                code: ErrCode::Ok,
                tid: None,
            };

            if let Some(mut tracer) = self
                .tracer
                .clone()
                .make_tracer(&conv_u8_to_uuid(&pkt.id.id), self.handle.clone())
            {
                let id = tracer.id.clone();
                let handle = tokio::spawn(async move { tracer.proc().await });
                rsp.tid = Some(id.1);

                if let Some(prev) = self.tracer.add_tracer(id, handle) {
                    prev.abort();
                }
            } else {
                rsp.code = ErrCode::NodeNotFound;
            }

            let _ = send_pkt!(self.handle, rsp);
            Ok(())
        }
    }
}

impl<T: ExecMsg> PacketProc<ReqCancelListen> for ServeHandler<T> {
    type Output<'a> = impl Future<Output = std::io::Result<()>> + 'a where Self: 'a;

    fn proc(&mut self, pkt: Box<ReqCancelListen>) -> Self::Output<'_> {
        async move {
            let mut rsp = RspCancelListen {
                id: pkt.id.clone(),
                tid: pkt.tid,
                code: ErrCode::Ok,
            };

            if let Some(v) = self
                .tracer
                .del_tracer(&TracerId(conv_u8_to_uuid(&pkt.id.id), pkt.tid))
            {
                info!(self.tracer.logger, "tracer.{} end", pkt.tid);
                v.abort();
            } else {
                rsp.code = ErrCode::NodeNotFound;
            }

            let _ = send_pkt!(self.handle, rsp);
            Ok(())
        }
    }
}
