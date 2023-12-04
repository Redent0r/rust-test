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
use serde::{Deserialize, Serialize};
use std::{ fs, fs::OpenOptions, io, io::Seek};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use docker_credential::{CredentialRetrievalError, DockerCredential};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");   
    my_async().await?;
    Ok(())
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct DockerConfigLayer {
    architecture: String,
    config: DockerImageConfig,
    rootfs: DockerRootfs,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct DockerImageConfig {
    User: Option<String>,
    Tty: Option<bool>,
    Env: Vec<String>,
    Cmd: Option<Vec<String>>,
    WorkingDir: Option<String>,
    Entrypoint: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct DockerRootfs {
    r#type: String,
    diff_ids: Vec<String>,
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

async fn save_layer_to_file (digest: String) ->  Result<(), Box<dyn std::error::Error>>{
    let mut file = tokio::fs::File::create("my_file")
            .await
            .map_err(|e| println!("{e}")).unwrap();
    
    let client = match Client::from_path("/var/run/containerd/containerd.sock").await {
        Ok(c) => {
            c
        },
        Err(e) => {
            println!("Failed to connect to containerd: {e:?}");
            process::exit(1);
        }
    };

    let req = ReadContentRequest {
        digest,
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
        file.seek(io::SeekFrom::Start(chunk.offset as u64)).await?;
        file.write_all(&chunk.data).await?;
        // println!("chunk.data: {:?}", chunk.data)
    }
    file.flush().await.map_err(|e| println!("{e}")).unwrap();
    Ok(())
}

async fn save_layer_to_file_oci (digest: String) ->  Result<(), Box<dyn std::error::Error>>{
    let image_ref:oci_distribution::Reference = "mcr.microsoft.com/aks/e2e/library-busybox:master.220314.1-linux-amd64".to_string().parse().unwrap();
    let auth = build_auth(&image_ref);

    let mut client = oci_distribution::Client::new(oci_distribution::client::ClientConfig {
        platform_resolver: Some(Box::new(oci_distribution::client::linux_amd64_resolver)),
        ..Default::default()
    });

    client.auth(&image_ref, &auth, oci_distribution::RegistryOperation::Pull).await?;

    let mut file = tokio::fs::File::create("my_file_oci")
            .await
            .map_err(|e| println!("{e}")).unwrap();

    
    client
        .pull_blob(&image_ref, &digest, &mut file)
        .await
        .map_err(|e| println!("{e}")).unwrap();

    file.flush().await.map_err(|e| println!("{e}")).unwrap();

    Ok(())
}

fn build_auth(reference: &oci_distribution::Reference) -> oci_distribution::secrets::RegistryAuth {
    println!("build_auth: {:?}", reference);

    let server = reference
        .resolve_registry()
        .strip_suffix("/")
        .unwrap_or_else(|| reference.resolve_registry());

    match docker_credential::get_credential(server) {
        Ok(DockerCredential::UsernamePassword(username, password)) => {
            println!("build_auth: Found docker credentials");
            return oci_distribution::secrets::RegistryAuth::Basic(username, password);
        }
        Ok(DockerCredential::IdentityToken(_)) => {
            println!("build_auth: Cannot use contents of docker config, identity token not supported. Using anonymous access.");
        }
        Err(CredentialRetrievalError::ConfigNotFound) => {
            println!("build_auth: Docker config not found - using anonymous access.");
        }
        Err(CredentialRetrievalError::NoCredentialConfigured) => {
            println!("build_auth: Docker credentials not configured - using anonymous access.");
        }
        Err(CredentialRetrievalError::ConfigReadError) => {
            println!("build_auth: Cannot read docker credentials - using anonymous access.");
        }
        Err(CredentialRetrievalError::HelperFailure { stdout, stderr }) => {
            if stdout == "credentials not found in native keychain\n" {
                // On WSL, this error is generated when credentials are not
                // available in ~/.docker/config.json.
                println!("build_auth: Docker credentials not found - using anonymous access.");
            } else {
                println!("build_auth: Docker credentials not found - using anonymous access. stderr = {}, stdout = {}",
                    &stderr, &stdout);
            }
        }
        Err(e) => panic!("Error handling docker configuration file: {}", e),
    }

    oci_distribution::secrets::RegistryAuth::Anonymous
}

async fn get_config_layer(image_ref: String, socket_path: String) ->  Result<DockerConfigLayer, Box<dyn std::error::Error>>{
    
    let socket = socket_path.clone(); // todo: figure out how not to clone everything to get it working
    let channel = Endpoint::try_from("http://[::]")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| UnixStream::connect(socket.clone())))
        .await
        .expect("Could not create client.");

    let mut client = ImageServiceClient::new(channel);

    let req =   k8s_cri::v1::ImageStatusRequest {
        image: Some(k8s_cri::v1::ImageSpec {
            image: image_ref,
            annotations: HashMap::new(),
        }),
        verbose: true
    };

    let resp = client.image_status(req).await?;
    let image_layers = resp.into_inner();

    let status_info: serde_json::Value = serde_json::from_str(image_layers.info.get("info").unwrap())?;
    let image_spec = status_info["imageSpec"].as_object().unwrap();
    let docker_config_layer: DockerConfigLayer = serde_json::from_value(serde_json::to_value(image_spec)?).unwrap();

    Ok(docker_config_layer)
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
    let mut manifest: serde_json::Value = Default::default();
    while let Some(chunk) = stream.message().await? {
        if chunk.offset < 0 {
            print!("oop")
        }
        else {
            
            manifest = serde_json::from_slice(&chunk.data)?;
            let isv1_manifest = manifest.get("layers") != None;
            if isv1_manifest {
                println!("v1 layers for {}: ", image_ref);
                return Ok(manifest);
            }
        }
    }

    println!("v2 manifest for {:#?}\n: ", manifest);

    // manifest is v2
    let manifest = manifest["manifests"].as_array().unwrap();

    if manifest.len() < 1 {
        println!("No manifests found for image: {}", image_ref);
        return Ok(serde_json::Value::Null);
    }

    // assume amd64 manifest is the first one
    let mut manifest_amd64 = &manifest[0];

    // iterate manifest to find the amd64 manifest:
    for entry in manifest {
        let platform = entry["platform"].as_object().unwrap();
        let architecture = platform["architecture"].as_str().unwrap();
        let os = platform["os"].as_str().unwrap();
        if architecture == "amd64" && os == "linux" {
            println!("found amd64 linux manifest: {:#?}", entry);
            manifest_amd64 = entry;
            break;
        }
    }

    let image_digest = manifest_amd64["digest"].as_str().unwrap().to_string();
    println!("image digest2 used to query layers: {:?}\n", image_digest);

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
            return Ok(manifest)
        }
    }

    Ok(serde_json::Value::Null)
}

async fn my_async() ->  Result<(), Box<dyn std::error::Error>>{
    let containerd_socket_path = "/var/run/containerd/containerd.sock";


    // let image_ref = "docker.io/library/nginx:latest".to_string();
    // let image_ref = "mcr.microsoft.com/aks/e2e/library-busybox:master.220314.1-linux-amd64".to_string();

    // let image_ref = "docker.io/bprashanth/nginxhttps:1.0".to_string();

    let image_ref = "mcr.microsoft.com/oss/kubernetes/pause:3.6".to_string();

    // let image_ref = "docker.io/alpine/socat:1.7.4.3-r0".to_string();
    pull_image(image_ref.clone(), containerd_socket_path.to_string()).await?;

    let config_layer: DockerConfigLayer = get_config_layer(image_ref.clone(), containerd_socket_path.to_string()).await?;
    
    println!("config_layer: {:#?}", config_layer);
    
    let manifest = get_image_manifest(image_ref.clone(), containerd_socket_path.to_string()).await?;
    println!("manifest: {:#?}", manifest);

    let manifests = manifest["layers"].as_array().unwrap();

    for entry in manifests {
        println!("{}", &entry["digest"].as_str().unwrap());
    }

    // save_layer_to_file("sha256:3c2cba919283a210665e480bcbf943eaaf4ed87a83f02e81bb286b8bdead0e75".to_string()).await?;
    // save_layer_to_file_oci("sha256:3c2cba919283a210665e480bcbf943eaaf4ed87a83f02e81bb286b8bdead0e75".to_string()).await?;
    Ok(())
}

