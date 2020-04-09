use std::fs::File;
use std::sync::Arc;

use async_trait::async_trait;
use core::fmt;
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Result};
use std::collections::{HashMap, HashSet, VecDeque};
use std::format;
use std::hash::Hasher;
use std::io::{BufRead, BufReader, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;

/// Vector Clock
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct VectorClock {
    pub vc:    [u64; 10],
    pub index: usize,
    pub name:  String,
}

impl VectorClock {
    pub fn new(index: usize, name: String) -> Self {
        VectorClock {
            vc: [0; 10],
            index,
            name,
        }
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        VectorClock {
            vc:    [0; 10],
            index: 11,
            name:  "null".to_string(),
        }
    }
}

impl VectorClock {
    pub fn receive(&mut self, coming: VectorClock) {
        for (s, incoming) in self.vc.iter_mut().zip(coming.vc.iter()) {
            *s = (*s).max(*incoming);
        }
        self.vc[self.index] += 1;
    }

    pub fn receive_anonymous(&mut self) {
        self.vc[self.index] += 1;
    }

    pub fn send(&mut self) -> VectorClock {
        self.vc[self.index] += 1;
        self.clone()
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn from_json_string(input: String) -> Self {
        serde_json::from_str(&input).unwrap()
    }

    pub fn to_clone(&self) -> Self {
        self.clone()
    }
}

lazy_static! {
    static ref FORM: Regex =
        Regex::new(r#"(RCV|SND|CAL)\|(\d|-)\|(V|S|J|C)\|(.*)->(.*)\|\|(.*)"#).unwrap();
}

#[derive(Debug)]
pub struct TaskLock {
    vc_lock:    Arc<RwLock<VectorClock>>,
    record_log: Option<File>,
    mode:       RRMode,

    // for replay
    container: HashSet<RRData>,
    saver:     HashMap<RRData, String>,
    queue:     VecDeque<RRRecord>,
}

impl TaskLock {
    pub fn new(index: usize, name: String, config: RRConfig) -> Self {
        let mode = RRMode::from(config.mode);
        let mut queue = VecDeque::new();
        let mut file: Option<File> = None;
        if mode == RRMode::Record {
            // let path: Path = config.trace_path.into();

            if !config.trace_path.exists() {
                let mut p = config.trace_path.clone();
                p.pop();
                std::fs::create_dir_all(p).expect("Failed to create rr directory");
            }

            file = Some(
                std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    //.append(true)
                    .create(true)
                    .open(config.trace_path)
                    .map_err(|e| {
                        log::error!("{}", e);
                        e
                    })
                    .unwrap(),
            );
        } else if mode == RRMode::Replay {
            let file2 = std::fs::OpenOptions::new()
                .read(true)
                //.truncate(true)
                //.append(true)
                //.create(true)
                .open(config.trace_path.clone())
                .map_err(|e| {
                    //log::error!("{}", e);
                    println!("{}", e);
                    e
                }).unwrap();

            let mut reader = BufReader::new(file2);
            for line in reader.lines() {
                if let Err(e) = line {
                    log::error!("task_lock read error: {}", e)
                } else {
                    let record_string = line.unwrap();
                    let rr_record = RRData::parse(record_string);
                    if let Some(r) = rr_record {
                        queue.push_back(r);
                    } else {
                        println!("parse record error")
                    }
                }
            }

            // prepare rrlog

            if !config.trace_path.exists() {
                let mut p = config.trace_path.clone();
                p.pop();
                std::fs::create_dir_all(p).expect("Failed to create rr directory");
            }
            let mut path = config.trace_path.clone();
            path.pop();
            path.push(format!("{}.rrlog", name.clone()));
            file = Some(
                std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    //.append(true)
                    .create(true)
                    .open(path)
                    .map_err(|e| {
                        log::error!("{}", e);
                        e
                    })
                    .unwrap(),
            );
        } else {
            // do nothing here
        }

        TaskLock {
            vc_lock: Arc::new(RwLock::new(VectorClock::new(index, name))),
            record_log: file,
            mode,
            container: HashSet::new(),
            saver: HashMap::new(),
            queue,
        }
    }

    pub fn receive(&mut self, coming: RRData, comment: String) -> VectorClock {
        if self.mode == RRMode::Inactivated {
            return self.vc_lock.read().to_clone();
        }

        match coming.clone() {
            RRData::VECT {
                received_vc: vc, ..
            } => self.vc_lock.write().receive(vc.clone()),
            RRData::JSON { json, .. } => self.vc_lock.write().receive_anonymous(),
            RRData::SEND { current: vc } => return self.vc_lock.read().to_clone(),
            RRData::SyncCallJson { json, .. } => self.vc_lock.write().receive_anonymous(),
        }

        if self.mode == RRMode::Record || self.mode == RRMode::Replay {
            let record = RRRecord::new(coming, self.vc_lock.write().to_clone(), comment);

            self.record_log
                .as_ref()
                .unwrap()
                .write_all(record.to_formatted_string().as_bytes())
                .unwrap();

            // log::warn!(
            //     "{} receive, {}",
            //     self.vc_lock.read().name,
            //     record.to_formatted_string(),
            // );
        };

        self.vc_lock.read().to_clone()
    }

    pub fn send(&mut self, comment: String) -> VectorClock {
        if self.mode == RRMode::Inactivated {
            return VectorClock::default();
        }
        let before = self.vc_lock.write().to_clone();

        let vc = self.vc_lock.write().send();

        if self.mode == RRMode::Record || self.mode == RRMode::Replay {
            let rr_data = RRData::from_send(before);

            let record = RRRecord::new(rr_data, vc.clone(), comment.clone());

            self.record_log
                .as_ref()
                .unwrap()
                .write(record.to_formatted_string().as_bytes())
                .unwrap();

            // log::warn!(
            //     "{} send, {}",
            //     self.vc_lock.read().name,
            //     record.to_formatted_string()
            // );
        }

        vc
    }

    pub fn supply(&mut self, data: RRData) {
        self.container.insert(data);
    }

    pub fn save_vc_data(&mut self, data: RRData, json_string: String) {
        self.saver.insert(data, json_string);
    }

    pub fn extract_vc_data(&mut self, data: RRData) -> String {
        self.saver.remove(&data).unwrap()
    }

    // get next RRData if ready
    pub fn next(&mut self) -> Option<RRData> {
        loop {
            let record = self.queue.pop_front();
            if record.is_none() {
                return None;
            }

            let record = record.unwrap();
            let expect_data = record.data.clone();

            // log::info!(
            //     "{} scanned : {}",
            //     self.vc_lock.read().name,
            //     record.to_formatted_string(),
            // );

            match &expect_data {
                RRData::JSON { .. } => break Some(expect_data),
                RRData::SEND { .. } => continue,
                RRData::VECT { .. } => {
                    if self.container.contains(&expect_data) {
                        self.container.remove(&expect_data);

                        // let current_vc = self.receive(expect_data.clone(), "replay".to_string());
                        // let expected_vc_translated = record.vc_translated.clone();
                        //
                        // if current_vc != expected_vc_translated {
                        //     log::error!("RR replay get expected vc/data {} but move onto
                        // unexpected vc {}, expect is {} ",
                        //     expect_data.to_string(),
                        //     expected_vc_translated.to_json_string(),
                        //     current_vc.to_json_string()
                        //     );
                        // }

                        break Some(expect_data);
                    } else {
                        log::warn!(
                            "{} has not found, waiting {}",
                            self.vc_lock.read().name,
                            expect_data.to_string(),
                        );
                        self.queue.push_front(record);
                        break None;
                    }
                }
                RRData::SyncCallJson { .. } => {
                    // log::error!(
                    // "{} next, the next is SyncCallJson!",
                    // self.vc_lock.read().name
                    // );
                    break None;
                }
            }
        }
    }

    pub fn next_sync_json(&mut self, source: usize) -> Option<RRData> {
        let record = self.queue.pop_front();
        if record.is_none() {
            return None;
        }

        let record = record.unwrap();
        let expect_data = record.data.clone();

        if let RRData::SyncCallJson {
            source: source_1,
            json,
        } = expect_data.clone()
        {
            if source == source_1 {
                Some(expect_data)
            } else {
                self.queue.push_front(record);
                None
            }
        } else {
            self.queue.push_front(record);
            None
        }
    }

    pub fn on_record(&self) -> bool {
        self.mode == RRMode::Record
    }

    pub fn on_replay(&self) -> bool {
        self.mode == RRMode::Replay
    }

    pub fn on_inactivated(&self) -> bool {
        self.mode == RRMode::Inactivated
    }
}

#[derive(Debug, PartialEq)]
pub enum RRMode {
    Inactivated,
    Record,
    Replay,
}

impl From<String> for RRMode {
    fn from(input: String) -> Self {
        match input.to_lowercase().as_str() {
            "inactivated" => RRMode::Inactivated,
            "record" => RRMode::Record,
            "replay" => RRMode::Replay,
            _ => RRMode::Inactivated,
        }
    }
}

#[derive(PartialEq, Clone, Debug, Hash, Eq)]
pub enum RRData {
    VECT {
        source:      usize,
        received_vc: VectorClock,
    },
    JSON {
        source: usize,
        json:   String,
    },
    SEND {
        current: VectorClock,
    },

    SyncCallJson {
        source: usize,
        json:   String,
    },
}

impl Default for RRData {
    fn default() -> Self {
        RRData::SEND {
            current: VectorClock::default(),
        }
    }
}

impl RRData {
    pub fn from_vector(source: usize, vc: VectorClock) -> Self {
        RRData::VECT {
            source,
            received_vc: vc,
        }
    }

    pub fn from_feed(source: usize, json: String) -> Self {
        RRData::JSON { source, json }
    }

    pub fn from_send(vc: VectorClock) -> Self {
        RRData::SEND { current: vc }
    }

    pub fn from_sycn_call_json(source: usize, json: String) -> Self {
        RRData::SyncCallJson { source, json }
    }

    pub fn empty_vector() -> Self {
        RRData::VECT {
            source:      0usize,
            received_vc: VectorClock::default(),
        }
    }

    pub fn empty_feed() -> Self {
        RRData::JSON {
            source: 0usize,
            json:   "".to_string(),
        }
    }

    pub fn empty_send() -> Self {
        RRData::SEND {
            current: VectorClock::default(),
        }
    }

    pub fn to_direction(&self) -> String {
        match self {
            RRData::VECT { .. } => "RCV".to_string(),
            RRData::JSON { .. } => "RCV".to_string(),
            RRData::SEND { .. } => "SND".to_string(),
            RRData::SyncCallJson { .. } => "CAL".to_string(),
        }
    }

    pub fn to_data_json_string(&self) -> String {
        match self {
            RRData::VECT {
                received_vc: vc, ..
            } => vc.to_json_string(),
            RRData::JSON { json, .. } => json.clone(),
            RRData::SEND { current, .. } => current.to_json_string(),
            RRData::SyncCallJson { json, .. } => json.clone(),
        }
    }

    pub fn to_tag(&self) -> String {
        match self {
            RRData::VECT { .. } => "V".to_string(),
            RRData::JSON { .. } => "J".to_string(),
            RRData::SEND { .. } => "S".to_string(),
            RRData::SyncCallJson { .. } => "C".to_string(),
        }
    }

    pub fn to_source(&self) -> String {
        match self {
            RRData::VECT { source, .. } => source.to_string(),
            RRData::JSON { source, .. } => source.to_string(),
            RRData::SEND { .. } => "-".to_string(),
            RRData::SyncCallJson { source, .. } => source.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "[{},{},{},{}]",
            self.to_direction(),
            self.to_source(),
            self.to_tag(),
            self.to_data_json_string()
        )
    }

    pub fn parse(record: String) -> Option<RRRecord> {
        let mut matches = FORM.captures_iter(record.as_str());
        let captures = matches.next();
        if captures.is_none() {
            return None;
        }
        //  Regex::new(r#"(RCV|SND)\|(\d|-)\|(V|S|J|C)\|(.*)->(.*)\|\|(.*)"#).unwrap();
        let cap = captures.unwrap();

        let ret = match cap[1].to_string().as_str() {
            "RCV" => {
                let source: String = cap[2].parse().unwrap();
                let data_type = cap[3].to_string();
                let rr_data = match data_type.as_str() {
                    "V" => {
                        let data = cap[4].to_string();
                        let vc = VectorClock::from_json_string(data);
                        Some(RRData::from_vector(source.parse().unwrap(), vc))
                    }
                    "J" => {
                        let data = cap[4].to_string();
                        Some(RRData::from_feed(source.parse().unwrap(), data))
                    }
                    _ => None,
                };
                rr_data
            }
            "SND" => {
                let data_type = cap[3].to_string();
                let rr_data = match data_type.as_str() {
                    "S" => {
                        let data = cap[4].to_string();
                        let vc = VectorClock::from_json_string(data);
                        Some(RRData::from_send(vc))
                    }

                    _ => None,
                };
                rr_data
            }

            "CAL" => {
                let source: String = cap[2].parse().unwrap();
                let data_type = cap[3].to_string();
                let rr_data = match data_type.as_str() {
                    "C" => {
                        let data = cap[4].to_string();
                        Some(RRData::from_sycn_call_json(source.parse().unwrap(), data))
                    }

                    _ => None,
                };
                rr_data
            }
            _ => None,
        };

        if ret.is_none() {
            return None;
        }

        let status_after_received = VectorClock::from_json_string(cap[5].to_string());
        let comment = cap[6].to_string();

        Some(RRRecord::new(ret.unwrap(), status_after_received, comment))
    }

    pub fn get_source(&self) -> usize {
        match self {
            RRData::VECT { source, .. } => *source,
            RRData::JSON { source, .. } => *source,
            RRData::SEND { .. } => usize::max_value(),
            RRData::SyncCallJson { source, .. } => *source,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RRRecord {
    pub data:          RRData,
    pub vc_translated: VectorClock,
    pub comment:       String,
}

impl RRRecord {
    pub fn new(data: RRData, vc_translated: VectorClock, comment: String) -> Self {
        RRRecord {
            data,
            vc_translated,
            comment,
        }
    }

    pub fn to_formatted_string(&self) -> String {
        let direction = self.data.to_direction();
        let source = self.data.to_source();
        let tag = self.data.to_tag();
        let data = self.data.to_data_json_string();
        let vc_translated = self.vc_translated.to_json_string();
        let comment = self.comment.clone();

        let s = std::fmt::format(format_args!(
            "{}|{}|{}|{}->{}||{}\n",
            direction, source, tag, data, vc_translated, comment,
        ));
        s
    }
}

#[derive(Clone, Debug)]
pub struct RRConfig {
    pub mode:       String,
    pub trace_path: PathBuf,
}

impl Default for RRConfig {
    fn default() -> Self {
        Self {
            mode:       "Inactivated".to_string(),
            trace_path: PathBuf::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::record::{RRConfig, TaskLock};
    use std::path::PathBuf;
    // use crate::record::FORM;

    #[test]
    fn t_read() {
        let rr_config = RRConfig {
            mode:       "replay".to_string(),
            trace_path: PathBuf::from("/home/lycrus/Desktop/state.rr"),
        };

        let t_l = TaskLock::new(0, "test_t_l".to_string(), rr_config);
    }
}
