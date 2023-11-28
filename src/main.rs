use k8s_cri::v1::image_service_client::ImageServiceClient;
use containerd_client::{services::v1::ReadContentRequest, Client, with_namespace};
use std::process;
use std::collections::HashMap;
use containerd_client::services::v1::GetImageRequest;
use std::convert::TryFrom;
use tokio::net::UnixStream;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");   
    my_async().await?;
    Ok(())
}

async fn pull_image(image_ref: String, socket_path: String) ->  Result<(), Box<dyn std::error::Error>>{
    let socket = socket_path.clone(); // todo: figure out how not to clone everything to get it working
    let channel = Endpoint::try_from("http://[::]")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| UnixStream::connect(socket.clone())))
        .await
        .expect("Could not create client.");

    let mut client = ImageServiceClient::new(channel);

    let req =   k8s_cri::v1::PullImageRequest {
        image: Some(k8s_cri::v1::ImageSpec {
            image: image_ref.clone(),
            annotations: HashMap::new(),
        }),
        auth: None,
        sandbox_config: None,
    };

    let resp = client.pull_image(req).await?;

    println!("pull image response: {:?}\n", resp);
    Ok(())
}

async fn get_image_manifest (image_ref: String, socket_path: String) ->  Result<serde_json::Value, Box<dyn std::error::Error>>{
    let client = match Client::from_path(socket_path).await {
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
        name: image_ref.clone()
    };
    let req = with_namespace!(req, "k8s.io");
    let resp = imageChannel.get(req).await?;

    let image_digest = resp.into_inner().image.unwrap().target.clone().unwrap().digest.to_string();
    println!("image digest used to query layers: {:?}\n", image_digest);

    let req = ReadContentRequest {
        digest: image_digest.to_string(),
        offset: 0,
        size: 0,
    };
    let req = with_namespace!(req, "k8s.io");
    let mut c = client.content();
    let resp = c.read(req).await?;
    let mut stream = resp.into_inner();

    while let Some(chunk) = stream.message().await? {
        if chunk.offset < 0 {
            print!("oop")
        }
        else {
            let manifest: serde_json::Value = serde_json::from_slice(&chunk.data)?;
            return Ok(manifest);
        }
    }
    Ok(serde_json::Value::Null)
}

async fn my_async() ->  Result<(), Box<dyn std::error::Error>>{
    let containerd_socket_path = "/var/run/containerd/containerd.sock";

    let image_ref = "docker.io/bprashanth/nginxhttps:1.0".to_string();

    // let image_ref = "docker.io/library/nginx:latest".to_string();

    pull_image(image_ref.clone(), containerd_socket_path.to_string()).await?;

    let manifest = get_image_manifest(image_ref.clone(), containerd_socket_path.to_string()).await?;
    println!("manifest: {:#?}", manifest);

    let isv2_manifest = manifest.get("manifests") != None; // v2 has manifest["manifests"]

    let manifests = if isv2_manifest {
        println!("v2 layers for {}:", image_ref);
        manifest["manifests"].as_array().unwrap()
    }
    else {
        println!("v1 layers for {}: ", image_ref);
        manifest["layers"].as_array().unwrap()
    };

    for entry in manifests {
        println!("{}", &entry["digest"].as_str().unwrap());
    }
    Ok(())
}

