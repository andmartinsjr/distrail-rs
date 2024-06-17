#![warn(clippy::pedantic)]

pub mod log;

pub mod log_v1 {
    tonic::include_proto!("log.v1");
}