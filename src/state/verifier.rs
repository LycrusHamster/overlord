use crate::error::ConsensusError;
use crate::record::{RRConfig, RRData, TaskLock, VectorClock};
use crate::types::{Hash, VerifyResp};
use crate::{Codec, Consensus, ConsensusResult};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{Stream, StreamExt};
use lazy_static::lazy_static;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use creep::Context as cContext;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub struct Verifier<T: Codec, F: Consensus<T>> {
    req_rx:   UnboundedReceiver<VerifyReq<T>>,
    resp_tx:  UnboundedSender<VerifyResp>,
    function: Arc<F>,

    rr: TaskLock,
}

lazy_static! {
    static ref SOURCE_CHECK_BLOCK: usize = 1usize;
    static ref SOURCE_VERIFY_REQ: usize = 0usize;
}

impl<
        T: Codec + Send + Sync + Serialize + DeserializeOwned + 'static,
        F: Consensus<T> + 'static,
    > Verifier<T, F>
{
    pub(crate) fn new(
        req_rx: UnboundedReceiver<VerifyReq<T>>,
        resp_tx: UnboundedSender<VerifyResp>,
        function: Arc<F>,
        rr_config: RRConfig,
    ) -> Self {
        let mut rr_config = rr_config.clone();
        rr_config.trace_path.push("verifier.rr");
        Verifier {
            req_rx,
            resp_tx,
            function,
            rr: TaskLock::new(2usize, "Verify".to_string(), rr_config),
        }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            while let Some(req) = self.next().await {
                if self.rr.on_record() || self.rr.on_inactivated() {
                    self.run_inactivated_on_record(req).await;
                } else {
                    self.run_on_replay(req).await;
                }
            }

            log::error!("Overlord: SMR drop");
        });
    }

    pub async fn run_inactivated_on_record(&mut self, req: VerifyReq<T>) {
        self.handle_req(req).await;
    }

    pub async fn run_on_replay(&mut self, req: VerifyReq<T>) {
        let req = req.clone();
        let rr_data = RRData::from_vector(0usize, req.rr_vc.clone().unwrap());
        self.rr.supply(rr_data.clone());

        self.rr
            .save_vc_data(rr_data, serde_json::to_string(&req).unwrap());

        while let Some(rr_data) = self.rr.next() {
            let source = rr_data.get_source();

            if let RRData::VECT {
                source,
                received_vc,
            } = rr_data.clone()
            {
                let msg = self.rr.extract_vc_data(rr_data);
                let req = serde_json::from_str(msg.as_str()).unwrap();

                self.handle_req(req).await;
            }
        }
    }

    // here we assume check block wont happen error due to lack of json stringfy errors
    async fn handle_req(&mut self, req: VerifyReq<T>) {
        if self.rr.on_record() || self.rr.on_replay() {
            self.rr.receive(
                RRData::from_vector(0usize, req.clone().rr_vc.unwrap()),
                "verify get req".to_string(),
            );
        }

        let res = if self.rr.on_inactivated() {
            let res = self
                .function
                .check_block(
                    cContext::new(),
                    req.height,
                    req.hash.clone(),
                    req.block.clone(),
                )
                .await;
            if res.is_err() {
                log::error!("verify_resp {} block error", req.height);
            }

            res.unwrap()
        } else if self.rr.on_record() {
            let res = self
                .function
                .check_block(
                    cContext::new(),
                    req.height,
                    req.hash.clone(),
                    req.block.clone(),
                )
                .await;
            if res.is_err() {
                log::error!("verify_resp {} block error", req.height);
            }

            let res = res.unwrap();
            self.rr.receive(
                RRData::from_sycn_call_json(0usize, serde_json::to_string(&res).unwrap()),
                "verify, function.check_block".to_string(),
            );
            res
        } else {
            // on replay
            if let Some(RRData::SyncCallJson { source, json }) = self.rr.next_sync_json(0usize) {
                let res: () = serde_json::from_str(json.as_str()).unwrap();

                self.rr.receive(
                    RRData::from_sycn_call_json(0usize, serde_json::to_string(&res).unwrap()),
                    "verify, function.check_block".to_string(),
                );

                res
            } else {
                log::error!("verify function.check_block error");
            }
        };

        let vc = self
            .rr
            .send("verifier send verify_resp to state".to_string());

        if let Err(e) = self.resp_tx.unbounded_send(VerifyResp {
            height:     req.height,
            round:      req.round,
            block_hash: req.hash,
            is_pass:    true,
            rr_vc:      Some(vc),
        }) {
            log::error!(
                "Overlord: verify_resp sends VerifyResp to state process error: {:?}",
                e
            );
        };
    }
}

impl<T: Codec, F: Consensus<T>> Stream for Verifier<T, F> {
    type Item = VerifyReq<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match Stream::poll_next(Pin::new(&mut self.req_rx), cx) {
            Poll::Pending => Poll::Pending,

            Poll::Ready(msg) => {
                if msg.is_none() {
                    log::error!("Channel dropped");
                    return Poll::Ready(None);
                }

                let msg = msg.unwrap();

                return Poll::Ready(Some(msg));
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifyReq<T: Codec> {
    // pub ctx:    cContext,
    pub height: u64,
    pub round:  u64,
    pub hash:   Hash,
    pub block:  T,
    pub rr_vc:  Option<VectorClock>,
}

impl<T: Codec> VerifyReq<T> {
    pub(crate) fn new(
        // ctx: cContext,
        height: u64,
        round: u64,
        hash: Hash,
        block: T,
        rr_vc: Option<VectorClock>,
    ) -> Self {
        VerifyReq {
            // ctx,
            height,
            round,
            hash,
            block,
            rr_vc,
        }
    }
}
