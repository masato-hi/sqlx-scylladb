use std::net::IpAddr;

use crate::{ScyllaDBTypeInfo, arguments::ScyllaDBArgument};

impl_type!(IpAddr, ScyllaDBTypeInfo::Inet, ScyllaDBArgument::IpAddr);

impl_array_type!(
    IpAddr,
    ScyllaDBTypeInfo::InetArray,
    ScyllaDBArgument::IpAddrArray
);
