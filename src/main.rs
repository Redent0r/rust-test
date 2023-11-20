use k8s_cri::v1::image_service_client::ImageServiceClient;
use containerd_client::{services::v1::ReadContentRequest, tonic::Request, with_namespace, Client};
use log::{error, info, warn};
use tokio::runtime::{Runtime};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tonic::Status;
use std::{env, path::Path, process, sync::Arc};
use std::{collections::HashMap, fs, fs::OpenOptions, io, io::Seek};
// use oci_distribution::{manifest, secrets::RegistryAuth, Reference};
use serde::{Deserialize, Serialize};
use serde::Deserializer;
use containerd_client::services::v1::GetImageRequest;

use std::convert::TryFrom;
use tokio::main;

use k8s_cri::v1alpha2::runtime_service_client::RuntimeServiceClient;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");   
    my_async().await?;
    Ok(())
}

async fn my_async() ->  Result<(), Box<dyn std::error::Error>>{
    let path = "/var/run/containerd/containerd.sock";
    let channel = Endpoint::try_from("http://[::]")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| UnixStream::connect(path)))
        .await
        .expect("Could not create client.");

    let mut client = ImageServiceClient::new(channel);

    // let req =   k8s_cri::v1::PullImageRequest {
    //     image: Some(k8s_cri::v1::ImageSpec {
    //         image: "docker.io/library/nginx:latest".to_string(),
    //         annotations: HashMap::new(),
    //     }),
    //     auth: None,
    //     sandbox_config: None,
    // };

    // let resp = client.pull_image(req).await?;

    // println!("{:?}", resp);

    let req = tonic::Request::new(k8s_cri::v1::ListImagesRequest {
        filter: None,
    });
    let resp = client.list_images(req).await?;
    println!("{:?}", resp);


    // let req = tonic::Request::new(k8s_cri::v1::ImageStatusRequest {
    //     image: Some(k8s_cri::v1::ImageSpec {
    //         image: "docker.io/library/nginx:latest".to_string(),
    //         annotations: HashMap::new(),
    //     }),
    //     verbose: false,
    // });
    // let resp = client.image_status(req).await?;
    // println!("{:?}", resp);


    // let req = tonic::Request::new(k8s_cri::v1::ImageFsInfoRequest {
        
    // });
    // let resp = client.image_fs_info(req).await?;
    // println!("{:?}", resp);

    let containerd_socket = "/var/run/containerd/containerd.sock";
    info!("Connecting to containerd at {containerd_socket}");
    let client = match Client::from_path(containerd_socket).await {
        Ok(c) => {
            c
        },
        Err(e) => {
            println!("Failed to connect to containerd: {e:?}");
            process::exit(1);
        }
    };

    let mut imageChannel = client.images();

    let req = GetImageRequest{
        name: "docker.io/library/nginx:latest".to_string()
    };
    let req = with_namespace!(req, "default");
    let resp = imageChannel.get(req).await?;
    // let image = imageChannel.pull(image_ref, None)?;
    println!("{:?}", resp);

    // let req = ReadContentRequest {
    //     digest: "sha256:343e6546f35877801de0b8580274a5e3a8e8464cabe545a2dd9f3c78df77542a".to_string(),
    //     offset: 0,
    //     size: 0,
    // };
    // let req = with_namespace!(req, "default");
    // let mut c = client.content();
    // let resp = c.read(req).await?;
    // let mut stream = resp.into_inner();

    // let mut file = tokio::fs::File::create("myfile").await?;
    
    // while let Some(chunk) = stream.message().await? {
    //     if chunk.offset < 0 {
    //         // debug!("Containerd reported a negative offset: {}", chunk.offset);
    //         // return Err(Status::invalid_argument("negative offset"));
    //         print!("oop")
    //     }
    //     else {
    //         // file.seek(io::SeekFrom::Start(chunk.offset as u64)).await?;
    //         // file.write_all(&chunk.data).await?;
    //         // print!("{:?}", chunk);
    //         // file.seek(io::SeekFrom::Start(chunk.offset as u64)).await?;
    //         // file.write_all(&chunk.data).await?;
    //         let manifest: serde_json::Value = serde_json::from_slice(&chunk.data)?;
    //         // println!("{:?}", manifest);
    //         let manifests = manifest["manifests"].as_array().unwrap();
    //         // println!("{:?}", manifests);
    //         for m in manifests {
    //             println!("{}", &m["digest"].as_str().unwrap());
    //         }
    //     }
    // }
    Ok(())
}