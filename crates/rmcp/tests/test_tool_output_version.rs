#![cfg(all(feature = "client", feature = "macros", not(feature = "local")))]

use std::sync::Arc;

use rmcp::{
    ClientHandler, Json, RoleClient, RoleServer, ServerHandler,
    handler::server::router::{
        Router,
        tool::{ToolRoute, ToolRouter},
    },
    model::{
        CallToolRequestParams, CallToolResult, ClientConfig, InitializeResult, JsonObject,
        ProtocolVersion, Tool,
    },
    service::serve_directly,
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct Name {
    value: String,
}

#[derive(Debug, Clone)]
struct OutputServer {
    tool_router: ToolRouter<Self>,
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for OutputServer {}

#[tool_router(router = tool_router)]
impl OutputServer {
    fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool]
    fn names(&self) -> Json<Vec<String>> {
        Json(vec!["Ada".to_owned()])
    }

    #[tool]
    fn name(&self) -> Json<Name> {
        Json(Name {
            value: "Ada".to_owned(),
        })
    }
}

#[derive(Debug, Clone)]
struct VersionedClient {
    protocol_version: ProtocolVersion,
}

impl ClientHandler for VersionedClient {
    fn get_info(&self) -> ClientConfig {
        let mut info = ClientConfig::default();
        info.protocol_version = self.protocol_version.clone();
        info
    }
}

#[derive(Debug, PartialEq, Eq)]
struct OutputShape {
    array_schema: bool,
    object_schema: bool,
    array_structured_content: bool,
    object_structured_content: bool,
    array_text_content: bool,
}

async fn output_shape_for(version: ProtocolVersion) -> OutputShape {
    let (server_transport, client_transport) = tokio::io::duplex(4096);
    let client_handler = VersionedClient {
        protocol_version: version.clone(),
    };
    let mut server_peer_info = InitializeResult::default();
    server_peer_info.protocol_version = version;

    let server = serve_directly::<RoleServer, _, _, _, _>(
        OutputServer::new(),
        server_transport,
        Some(client_handler.get_info()),
    );
    let server_handle = tokio::spawn(async move {
        server.waiting().await?;
        anyhow::Ok(())
    });
    let client = serve_directly::<RoleClient, _, _, _, _>(
        client_handler,
        client_transport,
        Some(server_peer_info.into()),
    );

    let tools = client
        .list_tools(None)
        .await
        .expect("list tools should succeed");
    let array_schema = tools
        .tools
        .iter()
        .find(|tool| tool.name == "names")
        .expect("array tool should be listed")
        .output_schema
        .is_some();
    let object_schema = tools
        .tools
        .iter()
        .find(|tool| tool.name == "name")
        .expect("object tool should be listed")
        .output_schema
        .is_some();
    let array_result = client
        .call_tool(CallToolRequestParams::new("names"))
        .await
        .expect("array tool should succeed");
    let object_result = client
        .call_tool(CallToolRequestParams::new("name"))
        .await
        .expect("object tool should succeed");
    let shape = OutputShape {
        array_schema,
        object_schema,
        array_structured_content: array_result.structured_content.is_some(),
        object_structured_content: object_result.structured_content.is_some(),
        array_text_content: !array_result.content.is_empty(),
    };

    client.cancel().await.expect("client should cancel");
    server_handle.await.expect("server task").expect("server");
    shape
}

async fn direct_router_output_shape_for(version: ProtocolVersion) -> (bool, bool, bool) {
    let (server_transport, client_transport) = tokio::io::duplex(4096);
    let client_handler = VersionedClient {
        protocol_version: version.clone(),
    };
    let mut server_peer_info = InitializeResult::default();
    server_peer_info.protocol_version = version;

    let mut output_schema = JsonObject::new();
    output_schema.insert("type".to_owned(), json!("array"));
    let tool = Tool::new("names", "Return names", Arc::new(JsonObject::new()))
        .with_raw_output_schema(Arc::new(output_schema));
    let service = Router::new(OutputServer::new())
        .with_tool(ToolRoute::new_dyn(tool, |_context| {
            Box::pin(async { Ok(CallToolResult::structured(json!(["Ada"])).into()) })
        }));
    let server = serve_directly::<RoleServer, _, _, _, _>(
        service,
        server_transport,
        Some(client_handler.get_info()),
    );
    let server_handle = tokio::spawn(async move {
        server.waiting().await?;
        anyhow::Ok(())
    });
    let client = serve_directly::<RoleClient, _, _, _, _>(
        client_handler,
        client_transport,
        Some(server_peer_info.into()),
    );

    let tools = client
        .list_tools(None)
        .await
        .expect("list tools should succeed");
    let array_schema = tools
        .tools
        .iter()
        .find(|tool| tool.name == "names")
        .expect("array tool should be listed")
        .output_schema
        .is_some();
    let result = client
        .call_tool(CallToolRequestParams::new("names"))
        .await
        .expect("array tool should succeed");
    let shape = (
        array_schema,
        result.structured_content.is_some(),
        !result.content.is_empty(),
    );

    client.cancel().await.expect("client should cancel");
    server_handle.await.expect("server task").expect("server");
    shape
}

#[tokio::test]
async fn legacy_protocols_omit_non_object_tool_output_fields() {
    for version in [ProtocolVersion::V_2025_06_18, ProtocolVersion::V_2025_11_25] {
        assert_eq!(
            output_shape_for(version).await,
            OutputShape {
                array_schema: false,
                object_schema: true,
                array_structured_content: false,
                object_structured_content: true,
                array_text_content: true,
            }
        );
    }
}

#[tokio::test]
async fn current_protocol_keeps_non_object_tool_output_fields() {
    assert_eq!(
        output_shape_for(ProtocolVersion::V_2026_07_28).await,
        OutputShape {
            array_schema: true,
            object_schema: true,
            array_structured_content: true,
            object_structured_content: true,
            array_text_content: true,
        }
    );
}

#[tokio::test]
async fn direct_router_strips_non_object_tool_output_for_legacy_peers() {
    assert_eq!(
        direct_router_output_shape_for(ProtocolVersion::V_2025_11_25).await,
        (false, false, true)
    );
    assert_eq!(
        direct_router_output_shape_for(ProtocolVersion::V_2026_07_28).await,
        (true, true, true)
    );
}
