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

    let image = "bprashanth/nginxhttps:1.0".to_string();

    let req =   k8s_cri::v1::PullImageRequest {
        image: Some(k8s_cri::v1::ImageSpec {
            image: image.clone(),
            annotations: HashMap::new(),
        }),
        auth: None,
        sandbox_config: None,
    };

    let resp = client.pull_image(req).await?;

    println!("pull image response: {:?}\n", resp);

    // let req = k8s_cri::v1::ListImagesRequest {
    //     filter: Some(k8s_cri::v1::ImageFilter {
    //         image: Some(k8s_cri::v1::ImageSpec {
    //             image: image.clone(),
    //             annotations: HashMap::new(),
    //         }),

    //     }),
       
    //  };

    // let resp = client.list_images(req).await?;

    // println!("list image response: {:#?}\n", resp);

    let req = k8s_cri::v1::ImageStatusRequest {
        image: Some(k8s_cri::v1::ImageSpec{
            image: image.clone(),
            annotations: HashMap::new(),
        }),
        verbose: true
    };
    
    let image_info = client.image_status(req).await?.into_inner();
    // println!("image_status: {:?}\n", image_info);

    // image_info.image.unwrap().repo_digests.iter().for_each(|digest| {
    //     let x = digest.split('@').collect::<Vec<&str>>();
    //     println!("digest: {}", x[1]);
    // });
    let x = image_info.image.unwrap();

    println!("image: {:#?}\n", x);
    
    // let status_info = image_info.info.get("info").unwrap();
    // println!("rootfs: {:?}\n", status_info);

    // let json_value: serde_json::Value = serde_json::from_str(image_info)?;
    // println!("JSON value: {}\n", json_value);

    // let layers = &json_value["imageSpec"]["rootfs"]["diff_ids"];
    // let layerVec: Vec<String> = serde_json::from_value(json_value["imageSpec"]["rootfs"]["diff_ids"].clone()).unwrap();
    
    // println!("layers: {}\n", layers);
    // println!("layerVec: {:?}\n", layerVec);

    // for layer in layerVec {
    //     println!("this is a layer: {}", layer);
    // }

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
    // let resp: tonic::Response<k8s_cri::v1::ImageFsInfoResponse> = client.image_fs_info(req).await?;
    // println!("fs info: {:?}\n", resp);

    let client = match Client::from_path(path).await {
        Ok(c) => {
            c
        },
        Err(e) => {
            println!("Failed to connect to containerd: {e:?}");
            process::exit(1);
        }
    };

    let mut imageChannel = client.images();


    let req = containerd_client::services::v1::ListImagesRequest {
        filters: vec![
            //"name!=docker.io/bprashanth/nginxhttps:1.0".to_string() //todo: get this filter working
            ]
    };

    let req = with_namespace!(req, "k8s.io");
    let resp = imageChannel.list(req).await?.into_inner();
    println!("list image response: {:?}\n", resp);

    let mut imageDigest = String::from("");
    resp.images.iter().for_each(|image| {
        println!("image name: {}", image.name);
        if image.name == "docker.io/library/nginx:latest" {
            imageDigest = image.target.clone().unwrap().digest.to_string();
        }
    });

    println!("the image digest is: {:?}\n", imageDigest);

    // let req = GetImageRequest{
    //     name: "docker.io/library/nginx:latest".to_string()
    // };
    // let req = with_namespace!(req, "default");
    // let resp = imageChannel.get(req).await?;
    
    // println!("get image response: {:?}", resp);

    // let image = imageChannel.pull(image_ref, None)?;

    let req = ReadContentRequest {
        digest: imageDigest.to_string(),
        offset: 0,
        size: 0,
    };
    let req = with_namespace!(req, "k8s.io");
    let mut c = client.content();
    let resp = c.read(req).await?;
    let mut stream = resp.into_inner();

    while let Some(chunk) = stream.message().await? {
        if chunk.offset < 0 {
            // debug!("Containerd reported a negative offset: {}", chunk.offset);
            // return Err(Status::invalid_argument("negative offset"));
            print!("oop")
        }
        else {
            // file.seek(io::SeekFrom::Start(chunk.offset as u64)).await?;
            // file.write_all(&chunk.data).await?;
            // print!("{:?}", chunk);
            // file.seek(io::SeekFrom::Start(chunk.offset as u64)).await?;
            // file.write_all(&chunk.data).await?;
            let manifest: serde_json::Value = serde_json::from_slice(&chunk.data)?;
            println!("manifet: {:#?}", manifest);
            let isv2Manifest = manifest.get("manifests") != None; // has manifest["manifests"]
            if isv2Manifest {
                let manifests = manifest["manifests"].as_array().unwrap();
                // println!("manifest: {:?}", manifests);
                for m in manifests {
                    println!("da layer: {}", &m["digest"].as_str().unwrap());
                }
            }
            else {
                println!("v1 detected")
            }
            
        }
    }
    Ok(())
}

