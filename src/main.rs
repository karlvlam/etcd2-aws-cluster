#[macro_use]
extern crate serde_derive;

extern crate hyper;
extern crate serde_json;
extern crate rusoto;
use std::process::exit;

use std::time::Duration;
use std::io::prelude::*;
use std::fs::File;
use std::borrow::ToOwned;
use std::thread::{sleep};

use std::io::Read;
use std::ops::Add;
use hyper::client::Client;
use hyper::header::{Header, HeaderFormat, Headers};

use std::default::Default;
use rusoto::{DefaultCredentialsProvider, Region, default_tls_client};
use rusoto::autoscaling::{AutoscalingClient, AutoScalingGroupNamesType, AutoScalingGroup};
use rusoto::ec2::{Ec2Client, DescribeInstancesRequest,Filter};


const min_etcd_node:usize = 5;

// ETCD API https://coreos.com/etcd/docs/latest/v2/members_api.html
const api_add_ok: u16 = 201;
const api_already_added: u16 = 409;
const api_delete_ok: u16 = 204;
const api_delete_gone: u16 = 410;

/*
Reading: http://engineering.monsanto.com/2015/06/12/etcd-clustering/
*/

fn main() {

    let INSTANCE_ADDR = match std::env::var("DEBUG") {
        Ok(_) => "127.0.0.1:8000", // debug, port forward use
        _ => "169.254.169.254" // real EC2 api IP
    };

    let etcd_peers_file_path = env("ETCD_PEERS_FILE_PATH", "/etc/sysconfig/etcd-peers");
    let CLIENT_PORT = env("ETCD_CLIENT_PORT", "2379");
    let SERVER_PORT = env("ETCD_SERVER_PORT", "2380");
    let CLIENT_SCHEME = env("ETCD_CLIENT_SCHEME", "http");
    let PEER_SCHEME = env("ETCD_PEER_SCHEME", "http");


    // TODO: Check script has already run? if so just exit
    /*
    Get REGION, instancd id, private ip
    */


    let aws_id_url = &format!("http://{}/latest/dynamic/instance-identity/document", INSTANCE_ADDR);
    println!("Getting EC2 instance information... {}", aws_id_url);
    let r:String = get_url(aws_id_url);
    let awsid: AwsId = serde_json::from_str(&r).unwrap();
    println!("- success -> {:?}", awsid);
    let AWS_REGION: Region = get_region_from_string(&awsid.region);

    println!("AWS_REGION : {:#?}", AWS_REGION);

    // TODO: check proxy mode
    /*
    Confirm existence of ASG , or Get auto-scaling-groups by node instance ID
    */
    let provider = DefaultCredentialsProvider::new().unwrap();
    let client = AutoscalingClient::new(default_tls_client().unwrap(), provider, AWS_REGION);
    let asg_result = client.describe_auto_scaling_groups(&Default::default());
    let (PROXY_ASG, ASG_NAME) = match std::env::var("PROXY_ASG") {
        Ok(asg_string) => {
            match asg_result {
                Ok(r) => {
                    if !asg_name_exists(r.auto_scaling_groups, &asg_string){
                        println!("ASG not found!");
                        exit(9);
                    }
                    ("on", asg_string)
                }

                Err(e) => {
                    println!("{:?}", e);
                    exit(9);

                }
            }
        },
        Err(e) => {
            (
                "off",
                match asg_result {
                    Ok(r) => {
                        match find_asg_name_from_instance_id(r.auto_scaling_groups, &awsid.instanceId) {
                            Some(asg_name) => {
                                println!("ASG found : {:#?}", asg_name);
                                asg_name
                            }
                            None => {
                                println!("ASG not found!");
                                exit(9);
                            }
                        }
                    },
                    _ => {
                        println!("ASG not found!");
                        exit(9);
                    }
                }
            )
        }
    };
    println!("PROXY_ASG = {}, ASG_NAME = {}", PROXY_ASG, ASG_NAME);

    /*
    get EC2 instances by auto-scaling-group
    */
    let mut etcd_ec2_ips:Vec<String> = vec![];
    loop {
        println!("Search for ASG node ...");
        let etcd_ec2_ips = match extract_ec2_ip_from_asg_name(&ASG_NAME, AWS_REGION) {
            Some(ec2ips) => {
                if ec2ips.len() >= min_etcd_node {
                    etcd_ec2_ips = ec2ips;
                    break;
                }
                println!("Number of ETCD node does not meet the min {}/{}, retry in 10 seconds...",
                         ec2ips.len(), min_etcd_node);
                sleep(Duration::from_secs(10));
            }
            None => {
                println!("Number of ETCD node does not meet the min {}/{}, retry in 10 seconds...",
                         0, min_etcd_node);
                sleep(Duration::from_secs(10));
            }

        };
    }


    /*
    check etcd members from the list one-by-one
    */
    println!("Looking for active ETCD node...");
    let mut etcd_active_node:String = String::new();
    let mut etcd_current_members:Vec<EtcdMember> = vec![];
    let mut etcd_active_node_client_url = String::new();
    for _item in &etcd_ec2_ips{
        match get_etcd_members(&_item, &CLIENT_SCHEME, &CLIENT_PORT) {
            Some(list) => {
                etcd_active_node = _item.to_string();
                etcd_current_members = list;
                etcd_active_node_client_url = format!("{}://{}:{}",
                                                      &CLIENT_SCHEME,&etcd_active_node,&CLIENT_PORT);
                break;

            }
            None => {
            }
        }
    }
    println!("{:#?}, {:#?}", etcd_active_node, etcd_current_members);

    println!("Looking for good/bad members from ETCD member list...");
    let mut etcd_good_members:Vec<EtcdMember> = vec![];
    let mut etcd_bad_members:Vec<EtcdMember> = vec![];
    for _m in etcd_current_members{
        if _m.clientURLs.len() == 0 {
            etcd_bad_members.push(_m);
            continue;
        }
        match check_etcd_members(&_m.clientURLs[0]) {
            Some(_) => {
                println!("OK: {:?}", _m);
                etcd_good_members.push(_m);
            }
            None => {
                etcd_bad_members.push(_m);
            }
        }
    }
    println!("Good members: {:#?}", etcd_good_members);
    println!("Bad members: {:#?}", etcd_bad_members);

    // remove bad etcd members
    println!("Delete bad members...");
    delete_etcd_members(&etcd_active_node_client_url, &etcd_bad_members);
    // TODO: need to clean etcd data in ETCD_DATA_DIR=/var/lib/etcd2

    /*
     make a etcd-peer-list string, and write to the file
    */
    println!("Write file to {} ...", &etcd_peers_file_path);
    match PROXY_ASG {
        "on" => {
            // Case for proxy mode only
            println!("Get the refreshed ETCD members...");
            match get_etcd_members(&etcd_active_node, &CLIENT_SCHEME, &CLIENT_PORT) {
                Some(list) => {
                    etcd_current_members = list;
                    println!("{:#?}", &etcd_current_members);

                }
                None => {
                    println!("Get member failed!");
                    exit(9);
                }
            }

            let file_content = gen_etcd_config_file_string(false, &awsid.instanceId, PROXY_ASG,
                                                           &etcd_current_members, &awsid.privateIp);
            println!("{}", &file_content);
            write_string_to_file(&etcd_peers_file_path, &file_content);
            exit(0);
        }
        "off" => {
            // Case for etcd cluster member
            println!("PROXY_ASG: {}", PROXY_ASG);
            // TODO: case 1 - new cluster
            // TODO: case 2 - existing cluster
            println!("Adding current node as new ETCD member...");
            add_etcd_members(&etcd_active_node_client_url, &awsid, &PEER_SCHEME, &SERVER_PORT);

            println!("Get the refreshed ETCD members...");
            match get_etcd_members(&etcd_active_node, &CLIENT_SCHEME, &CLIENT_PORT) {
                Some(list) => {
                    etcd_current_members = list;
                    println!("{:#?}", &etcd_current_members);

                }
                None => {
                    println!("Get member failed!");
                    exit(9);
                }
            }

            let file_content = gen_etcd_config_file_string(false, &awsid.instanceId, PROXY_ASG,
                                                           &etcd_current_members, &awsid.privateIp);
            println!("{}", &file_content);
            write_string_to_file(&etcd_peers_file_path, &file_content);
            exit(0);
            // TODO: case 2b - add current node to existing cluster
        }
        _ => {
            println!("Something went wrong: unknown PROXY_ASG");
            exit(9);

        }
    }




}

fn env(key:&str, d:&str) -> String {
    match std::env::var(key){
        Ok(v) => v,
        _ => d.to_string()
    }
}

fn get_url(url: &str) -> String {
    let mut result = String::new();
    let mut client = Client::new();
    // TODO: make timeout works!!!
    client.set_read_timeout(Some(Duration::from_millis(1000)));
    client.set_write_timeout(Some(Duration::from_millis(1000)));
    match client.get(url).send() {
        Ok(mut res) => {
            res.read_to_string(&mut result);
        }
        Err(e) => {
           println!("HTTP_GET_ERROR : {} : {:#?}", url, e);
        }
    }
    result
}
fn add_etcd_members(client_url:&str, awsid: &AwsId, peer_schema: &str, server_port: &str){
    let mut result = String::new();
    let mut client = Client::new();
    // TODO: make timeout works!!!
    client.set_read_timeout(Some(Duration::from_millis(1000)));
    client.set_write_timeout(Some(Duration::from_millis(1000)));
    let url = &format!("{}/v2/members",client_url);
    let body = &format!("{{\"name\":\"{}\", \"peerURLs\": [\"{}://{}:{}\"]}}",
                        awsid.instanceId, peer_schema, awsid.privateIp, server_port);
    println!("{}", body);

    let mut headers = Headers::new();
    headers.set_raw("Content-Type",vec![b"application/json".to_vec()]);
    match client.post(url).headers(headers).body(body).send() {
        Ok(mut res) => {
            println!("ETCD_ADD : {} - {}", res.url, res.status);
        }
        Err(e) => {
            println!("ETCD_ADD_ERROR : {} : {:#?}", url, e);
        }
    }
    sleep(Duration::from_secs(3));
}
fn delete_etcd_members(client_url:&str, list:&Vec<EtcdMember>){
    let mut result = String::new();
    let mut client = Client::new();
    // TODO: make timeout works!!!
    client.set_read_timeout(Some(Duration::from_millis(1000)));
    client.set_write_timeout(Some(Duration::from_millis(1000)));
    for _m in list {
        let url = &format!("{}/v2/members/{}",client_url,_m.id);
        match client.delete(url).send() {
            Ok(mut res) => {
                println!("ETCD_DEL : {} - {}", res.url, res.status);
            }
            Err(e) => {
                println!("ETCD_DEL_ERROR : {} : {:#?}", url, e);
            }
        }
    }
    sleep(Duration::from_secs(5));
}

#[derive(Serialize, Deserialize, Debug)]
struct AwsId {
    region: String,
    instanceId: String,
    privateIp: String,
}

#[derive(Debug)]
struct Ec2Info {
    id: String,
    private_ip: String,
    state: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct EtcdMemberList{
    members: Vec<EtcdMember>
}

#[derive(Serialize, Deserialize, Debug)]
struct EtcdMember{
    id: String,
    name: String,
    peerURLs: Vec<String>,
    clientURLs: Vec<String>,
}

fn get_region_from_string(region: &String) -> Region {
    match region.as_str() {
        "us-east-1" => Region::UsEast1,
        "us-east-2" => Region::UsEast2,
        "us-west-1" => Region::UsWest1,
        "us-west-2" => Region::UsWest2,
        "ca-central-1" => Region::CaCentral1,
        "eu-west-1" => Region::EuWest1,
        "eu-west-2" => Region::EuWest2,
        "eu-central-1" => Region::EuCentral1,
        "ap-northeast-1" => Region::ApNortheast1,
        "ap-northeast-2" => Region::ApNortheast2,
        "ap-south-1" => Region::ApSouth1,
        "ap-southeast-1" => Region::ApSoutheast1,
        "ap-southeast-2" => Region::ApSoutheast2,
        "sa-east-1" => Region::SaEast1,
        "cn-north-1" => Region::CnNorth1,
        _ => {
            println!("AWS region not found!");
            exit(9)
        }

    }
}



fn asg_name_exists(asgs: Vec<AutoScalingGroup>, asg_name: &String) -> bool {
    for asg in asgs {
        if &asg.auto_scaling_group_name == asg_name {
            return true;
        }

    }
    false
}
fn find_asg_name_from_instance_id(asgs: Vec<AutoScalingGroup>, instance_id: &String) -> Option<String> {
    for asg in asgs {

        match asg.instances {
            Some(instances) => {
                for instance in instances {
                    if &instance.instance_id == instance_id {
                        return Some(asg.auto_scaling_group_name);
                    }
                }
            }
            None => {
            }
        }
    }
    None
}

fn extract_ec2_ip_from_asg_name(asg_name: &String, region: Region) -> Option<Vec<String>> {
    let provider = DefaultCredentialsProvider::new().unwrap();
    let client = Ec2Client::new(default_tls_client().unwrap(), provider, region);
    let opt = DescribeInstancesRequest {
        dry_run: Some(false),
        instance_ids: None,
        //max_results: Some(1000),
        max_results: None,
        filters: Some(vec![Filter{
            name: Some("tag:aws:autoscaling:groupName".to_string()),
            values: Some(vec![asg_name.clone()])
        }]),
        next_token: None

    };
    match client.describe_instances(&opt) {
        Ok(result) => {
            let r = result.reservations.unwrap();
            let mut v:Vec<String> = vec![];
            for _r in r {

                for __r in _r.instances.unwrap(){
                    match __r.private_ip_address {
                        Some(ip) => v.push(ip),
                        None => continue
                    }
                }
            }
            match v.len() {
                0 => None,
                _ => Some(v)
            }

        }
        Err(e) => {
            println!("{:#?}", e);
            None

        }
    }
}


fn get_etcd_members(ip: &str, schema: &str, port: &str) -> Option<Vec<EtcdMember>>{

    let r:Result<EtcdMemberList, serde_json::Error> = serde_json::from_str(
        &get_url(&format!("{}://{}:{}/v2/members", schema, ip, port)));
    match r {
        Ok(r) => Some(r.members),
        Err(e) => None
    }

}
fn check_etcd_members(client_url: &str) -> Option<Vec<EtcdMember>>{

    let r:Result<EtcdMemberList, serde_json::Error> = serde_json::from_str(
        &get_url(&format!("{}/v2/members", client_url)));
    match r {
        Ok(r) => Some(r.members),
        Err(e) => None
    }

}
fn gen_etcd_config_file_string(cluster_new:bool, etcd_name: &str, proxy_asg: &str,
                               cluster_list:&Vec<EtcdMember>, etcd_self_ip: &str) -> String {
    /* SAMPLE
    ETCD_INITIAL_CLUSTER_STATE=existing
    ETCD_NAME=i-09b721fa2cf2d1f0a
    ETCD_INITIAL_CLUSTER="i-0b34530cfd3a80737=http://10.0.24.230:2380,i-002a0e71d60984532=http://10.0.24.245:2380"
    PROXY_ASG=off
    */

    // new or existing cluster?
    let mut r:String = match cluster_new {
        true => format!("ETCD_INITIAL_CLUSTER_STATE=new\n"),
        false => format!("ETCD_INITIAL_CLUSTER_STATE=existing\n")
    };

    // ETCD_NAME, as is
    r += &format!("ETCD_NAME={}\n",etcd_name);

    // CLUSTER url list, play with strings...
    r += "ETCD_INITIAL_CLUSTER=\"";
    for m in cluster_list {
        let current_node_not_joinned_etcd_cluster =
            &m.name == "" && *&m.peerURLs[0].contains(&format!("//{}:", etcd_self_ip));
        match current_node_not_joinned_etcd_cluster {
            true => {
                r += &format!("{}={},",etcd_name, &m.peerURLs[0]);
            }
            false => {

                r += &format!("{}={},",&m.name, &m.peerURLs[0]);
            }
        }

    }
    if cluster_list.len() > 1 {
        let l = r.len();
        r.truncate(l - 1);
    }
    r += "\"\n";

    // PROXY_ASG
    r += &format!("ETCD_PROXY={}\n",proxy_asg);

    r

}

fn write_string_to_file(path:&str, s:&str) {
    match File::create(path) {
        Ok(mut f) => {
            match f.write_all(s.as_bytes()){
                Ok(_) => {
                    println!("Write to file : {} -> successful!", path);
                }
                Err(e) => {
                    println!("{}", e);
                    println!("Write to file : {} -> failed!", path);
                    exit(9);
                }

            }
        }
        Err(e) => {
            println!("Create file {} failed -> {}", path, e);
            exit(9);
        }

    }
}
