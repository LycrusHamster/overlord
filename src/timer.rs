use std::task::{Context, Poll};
use std::time::Duration;
use std::{future::Future, pin::Pin};

use derive_more::Display;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::{Stream, StreamExt};
use futures::{FutureExt, SinkExt};
use futures_timer::Delay;
use log::{debug, error, info};

use crate::record::RRData::VECT;
use crate::record::{RRConfig, RRData, TaskLock, VectorClock};
use crate::smr::smr_types::{SMREvent, SMRTrigger, TriggerSource, TriggerType};
use crate::smr::{Event, SMRHandler};
use crate::DurationConfig;
use crate::{error::ConsensusError, ConsensusResult, INIT_HEIGHT, INIT_ROUND};
use crate::{types::Hash, utils::timer_config::TimerConfig};

const MAX_TIMEOUT_COEF: u32 = 5;

/// Overlord timer used futures timer which is powered by a timer heap. When monitor a SMR event,
/// timer will get timeout interval from timer config, then set a delay. When the timeout expires,
#[derive(Debug)]
pub struct Timer {
    config:        TimerConfig,
    event:         Event,
    sender:        UnboundedSender<SMREvent>,
    notify:        UnboundedReceiver<SMREvent>,
    state_machine: SMRHandler,
    height:        u64,
    round:         u64,
    rr:            TaskLock,
}

pub enum ReturnType {
    EventFromSMR(SMREvent),
    EventFromTimeout(SMREvent),
    Error(),
}

///
impl Stream for Timer {
    type Item = ReturnType;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        const SOURCE_EVENT: usize = 0usize;
        const SOURCE_TIMEOUT: usize = 1usize;

        let mut event_ready = true;
        let mut timer_ready = true;

        match self.event.poll_next_unpin(cx) {
            Poll::Pending => {}

            Poll::Ready(event) => {
                // log::info!("timer1 polls, event : {:?}", event);

                if event.is_none() {
                    log::error!("timer.event drops");
                    return Poll::Ready(Some(ReturnType::Error()));
                }

                let event = event.unwrap();
                if event == SMREvent::Stop {
                    return Poll::Ready(None);
                }

                return Poll::Ready(Some(ReturnType::EventFromSMR(event)));
            }
        };

        match self.notify.poll_next_unpin(cx) {
            Poll::Pending => {}

            Poll::Ready(event) => {
                // log::info!("timer2 polls, event : {:?}", event);

                if event.is_none() {
                    log::error!("timer.notify drops");
                    return Poll::Ready(Some(ReturnType::Error()));
                }

                let event = event.unwrap();

                return Poll::Ready(Some(ReturnType::EventFromTimeout(event)));
            }
        }

        return Poll::Pending;
    }
}

impl Timer {
    const SOURCE_EVENT: usize = 0usize;
    const SOURCE_TIMEOUT: usize = 1usize;

    pub fn new(
        event: Event,
        state_machine: SMRHandler,
        interval: u64,
        config: Option<DurationConfig>,
        rr_config: RRConfig,
    ) -> Self {
        let (tx, rx) = unbounded();
        let mut timer_config = TimerConfig::new(interval);
        if let Some(tmp) = config {
            timer_config.update(tmp);
        }
        let mut rr_config = rr_config.clone();
        rr_config.trace_path.push("timer.rr");
        Timer {
            config: timer_config,
            height: INIT_HEIGHT,
            round: INIT_ROUND,
            sender: tx,
            notify: rx,
            event,
            state_machine,
            rr: TaskLock::new(3usize, "Timer".to_string(), rr_config),
        }
    }

    pub fn run(mut self) {
        tokio::spawn(async move {
            loop {
                let res = self.next().await;

                if res.is_none() {
                    log::error!("timer channel ends");
                    break;
                }

                if let Some(return_data) = res {
                    if self.rr.on_record() || self.rr.on_inactivated() {
                        self.run_inactivated_on_record(return_data);
                    } else {
                        self.run_on_replay(return_data);
                    }
                }
            }
        });
    }

    pub fn run_inactivated_on_record(&mut self, return_data: ReturnType) {
        match return_data {
            ReturnType::EventFromSMR(event) => {
                self.set_timer(event);
            }
            ReturnType::EventFromTimeout(event) => {
                self.trigger(event);
            }
            ReturnType::Error() => {
                log::error!("timer polling get error");
            }
        };
    }

    pub fn run_on_replay(&mut self, return_data: ReturnType) {
        match return_data {
            ReturnType::EventFromSMR(event) => {
                let rr_data =
                    RRData::from_vector(Self::SOURCE_EVENT, event.clone().get_vc().unwrap());
                self.rr.supply(rr_data.clone());
                self.rr
                    .save_vc_data(rr_data, serde_json::to_string(&event).unwrap());
            }
            ReturnType::EventFromTimeout(event) => {
                let rr_data =
                    RRData::from_vector(Self::SOURCE_TIMEOUT, event.clone().get_vc().unwrap());
                self.rr.supply(rr_data.clone());
                self.rr
                    .save_vc_data(rr_data, serde_json::to_string(&event).unwrap());
            }
            ReturnType::Error() => log::error!("timer polling get error"),
        };

        while let Some(rr_data) = self.rr.next() {
            let source = rr_data.get_source();
            if source == Self::SOURCE_EVENT {
                if let RRData::VECT {
                    source,
                    received_vc,
                } = rr_data.clone()
                {
                    let data: SMREvent =
                        serde_json::from_str(self.rr.extract_vc_data(rr_data).as_str()).unwrap();

                    self.set_timer(data);
                }
            } else if source == Self::SOURCE_TIMEOUT {
                if let RRData::VECT {
                    source,
                    received_vc,
                } = rr_data.clone()
                {
                    let data: SMREvent =
                        serde_json::from_str(self.rr.extract_vc_data(rr_data).as_str()).unwrap();

                    self.trigger(data);
                }
            }
        }
    }

    fn set_timer(&mut self, mut event: SMREvent) -> ConsensusResult<()> {
        let mut is_brake_timer = false;
        log::warn!("timer.set_timer SMREvent: {:?}", event);

        if self.rr.on_record() || self.rr.on_replay() {
            let vc = event.clone().get_vc();

            let vc = vc.unwrap();

            self.rr.receive(
                RRData::from_vector(Self::SOURCE_EVENT, vc),
                "timer, get smr event from smr".to_string(),
            );
        }

        match event.clone() {
            SMREvent::NewRoundInfo {
                height,
                round,
                new_interval,
                new_config,
                ..
            } => {
                if height > self.height {
                    self.height = height;
                }
                self.round = round;

                if let Some(interval) = new_interval {
                    self.config.set_interval(interval);
                }
                if let Some(config) = new_config {
                    self.config.update(config);
                }
            }
            SMREvent::Brake { .. } => is_brake_timer = true,
            SMREvent::Commit(_, _) => return Ok(()),
            _ => (),
        };

        let mut interval = self.config.get_timeout(event.clone())?;
        if !is_brake_timer {
            let mut coef = self.round as u32;
            if coef > MAX_TIMEOUT_COEF {
                coef = MAX_TIMEOUT_COEF;
            }
            interval *= 2u32.pow(coef);
        }

        if self.rr.on_record() || self.rr.on_replay() {
            let vc = self
                .rr
                .send("timer polls, send timeout to self".to_string());
            event.set_vc(vc);
        }

        info!("Overlord: timer set timer, {:?}", event);
        let smr_timer = TimeoutInfo::new(interval, event, self.sender.clone());

        tokio::spawn(async move {
            smr_timer.await;
        });
        Ok(())
    }

    #[rustfmt::skip]
    fn trigger(&mut self, event: SMREvent) -> ConsensusResult<()> {
        log::warn!("timer.trigger SMREvent: {:?}",event);

        if self.rr.on_record() || self.rr.on_replay() {
            let vc = event.clone().get_vc();

            let vc = vc.unwrap();

            self.rr.receive(
                RRData::from_vector(Self::SOURCE_TIMEOUT, vc),
                "timer, get timeout".to_string(),
            );
        }

        let (trigger_type, round, height) = match event {
            SMREvent::NewRoundInfo { height, round, .. } => {
                if height < self.height || round < self.round {
                    return Ok(());
                }
                (TriggerType::Proposal, None, height)
            }

            SMREvent::PrevoteVote {
                height, round, ..
            } => {
                if height < self.height {
                    return Ok(());
                }
                (TriggerType::PrevoteQC, Some(round), height)
            }

            SMREvent::PrecommitVote {
                height, round, ..
            } => {
                if height < self.height {
                    return Ok(());
                }
                (TriggerType::PrecommitQC, Some(round), height)
            }

            SMREvent::Brake { height, round, .. } => {
                if height < self.height {
                    return Ok(());
                }
                (TriggerType::BrakeTimeout, Some(round), height)
            }

            _ => return Err(ConsensusError::TimerErr("No commit timer".to_string())),
        };

        debug!("Overlord: timer {:?} time out", event);
        let mut vc = None;

        if self.rr.on_record() || self.rr.on_replay(){
            vc = Some(self.rr.send("timer polls, send trigger to state".to_string()));

        }

        log::info!("timer send trigger to smr : {}", trigger_type.clone());
        self.state_machine.trigger(SMRTrigger {
            source: TriggerSource::Timer,
            hash: Hash::new(),
            trigger_type,
            round,
            height,
            wal_info: None,
            rr_vc: vc,
        })

    }
}

/// Timeout info which is a future consists of a `futures-timer Delay`, timeout info and a sender.
/// When the timeout expires, future will send timeout info by sender.
#[derive(Debug, Display)]
#[display(fmt = "{:?}", info)]
struct TimeoutInfo {
    timeout: Delay,
    info:    SMREvent,
    sender:  UnboundedSender<SMREvent>,
}

impl Future for TimeoutInfo {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let msg = self.info.clone();
        let mut tx = self.sender.clone();

        match self.timeout.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                // tokio::spawn(async move {
                // tx.send(msg).await;
                // });
                // log::info!("TimeoutInfo poll: {:?}", msg);
                log::info!("TimeoutInfo sends event to Timer: {:?}", msg);
                tx.unbounded_send(msg);

                // tokio::spawn(async move {
                //     let _ = tx.send(msg).await;
                // });

                Poll::Ready(())
            }
        }
    }
}

impl TimeoutInfo {
    fn new(interval: Duration, event: SMREvent, tx: UnboundedSender<SMREvent>) -> Self {
        TimeoutInfo {
            timeout: Delay::new(interval),
            info:    event,
            sender:  tx,
        }
    }
}

#[cfg(test)]
mod test {
    use futures::channel::mpsc::unbounded;
    use futures::stream::StreamExt;

    use crate::record::RRConfig;
    use crate::smr::smr_types::{FromWhere, SMREvent, SMRTrigger, TriggerSource, TriggerType};
    use crate::smr::{Event, SMRHandler};
    use crate::{timer::Timer, types::Hash};

    async fn test_timer_trigger(input: SMREvent, output: SMRTrigger) {
        let (trigger_tx, mut trigger_rx) = unbounded();
        let (event_tx, event_rx) = unbounded();
        let mut timer = Timer::new(
            Event::new(event_rx),
            SMRHandler::new(trigger_tx),
            3000,
            None,
            RRConfig::default(),
        );
        event_tx.unbounded_send(input).unwrap();

        tokio::spawn(async move {
            loop {
                match timer.next().await {
                    None => break,
                    Some(_) => panic!("Error"),
                }
            }
        });

        if let Some(res) = trigger_rx.next().await {
            assert_eq!(res, output);
            event_tx.unbounded_send(SMREvent::Stop).unwrap();
        }
    }

    fn gen_output(trigger_type: TriggerType, round: Option<u64>, height: u64) -> SMRTrigger {
        SMRTrigger {
            source: TriggerSource::Timer,
            hash: Hash::new(),
            trigger_type,
            round,
            height,
            wal_info: None,
            rr_vc: None,
        }
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_correctness() {
        // Test propose step timer.
        test_timer_trigger(
            SMREvent::NewRoundInfo {
                height:        0,
                round:         0,
                lock_round:    None,
                lock_proposal: None,
                new_interval:  None,
                new_config:    None,
                from_where:    FromWhere::PrecommitQC(0),
                rr_vc:         None,
            },
            gen_output(TriggerType::Proposal, None, 0),
        )
        .await;

        // Test prevote step timer.
        test_timer_trigger(
            SMREvent::PrevoteVote {
                height:     0u64,
                round:      0u64,
                block_hash: Hash::new(),
                lock_round: None,
                rr_vc:      None,
            },
            gen_output(TriggerType::PrevoteQC, Some(0), 0),
        )
        .await;

        // Test precommit step timer.
        test_timer_trigger(
            SMREvent::PrecommitVote {
                height:     0u64,
                round:      0u64,
                block_hash: Hash::new(),
                lock_round: None,
                rr_vc:      None,
            },
            gen_output(TriggerType::PrecommitQC, Some(0), 0),
        )
        .await;
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_order() {
        let (trigger_tx, mut trigger_rx) = unbounded();
        let (event_tx, event_rx) = unbounded();
        let mut timer = Timer::new(
            Event::new(event_rx),
            SMRHandler::new(trigger_tx),
            3000,
            None,
            RRConfig::default(),
        );

        let new_round_event = SMREvent::NewRoundInfo {
            height:        0,
            round:         0,
            lock_round:    None,
            lock_proposal: None,
            new_interval:  None,
            new_config:    None,
            from_where:    FromWhere::PrecommitQC(0),
            rr_vc:         None,
        };

        let prevote_event = SMREvent::PrevoteVote {
            height:     0u64,
            round:      0u64,
            block_hash: Hash::new(),
            lock_round: None,
            rr_vc:      None,
        };

        let precommit_event = SMREvent::PrecommitVote {
            height:     0u64,
            round:      0u64,
            block_hash: Hash::new(),
            lock_round: None,
            rr_vc:      None,
        };

        tokio::spawn(async move {
            loop {
                match timer.next().await {
                    None => break,
                    Some(_) => panic!("Error"),
                }
            }
        });

        event_tx.unbounded_send(new_round_event).unwrap();
        event_tx.unbounded_send(prevote_event).unwrap();
        event_tx.unbounded_send(precommit_event).unwrap();

        let mut count = 1u32;
        let mut output = Vec::new();
        let predict = vec![
            gen_output(TriggerType::PrecommitQC, Some(0), 0),
            gen_output(TriggerType::PrevoteQC, Some(0), 0),
            gen_output(TriggerType::Proposal, None, 0),
        ];

        while let Some(res) = trigger_rx.next().await {
            output.push(res);
            if count != 3 {
                count += 1;
            } else {
                assert_eq!(predict, output);
                event_tx.unbounded_send(SMREvent::Stop).unwrap();
                return;
            }
        }
    }
}
