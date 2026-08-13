use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, InfluxConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct InfluxConnection {
    client: reqwest::Client,
    config: InfluxConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InfluxConfig {
    base_url: String,
    database: String,
    token: Option<String>,
    query_type: String,
    tls: TlsConfig,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    schema: String,
    name: String,
    columns: Vec<Value>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, InfluxConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match InfluxConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let client = match config.tls.build_client() {
        Ok(client) => client,
        Err(err) => return abi::error("connector.invalidRequest", config.redact(&err)),
    };
    let connection = InfluxConnection { client, config };
    let version = match runtime().and_then(|runtime| runtime.block_on(ping_server(&connection))) {
        Ok(version) => version,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let mut response = connection.connect_response(&connection_id);
    if let Some(version) = version {
        response.insert("serverVersion".to_string(), Value::String(version));
    }
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime()
        .and_then(|runtime| runtime.block_on(run_query(&connection, sql, abi::max_rows(request))))
    {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl InfluxConnection {
    fn connect_response(&self, connection_id: &str) -> Map<String, Value> {
        Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            (
                "connectionId".to_string(),
                Value::String(connection_id.to_string()),
            ),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "endpoint".to_string(),
                Value::String(self.config.base_url.clone()),
            ),
            (
                "database".to_string(),
                Value::String(self.config.database.clone()),
            ),
        ])
    }

    fn auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = self.config.token.as_deref() {
            builder.header("Authorization", format!("Token {token}"))
        } else {
            builder
        }
    }
}

/// Transport security, as `connector.config.json` declares it under
/// `clientCertificate`.
///
/// Paths, never key material: connector options persist to the workspace in the
/// clear, so the profile carries a path and the driver reads the file at
/// connect time.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TlsConfig {
    root_cert_path: Option<String>,
    client_cert_path: Option<String>,
    client_key_path: Option<String>,
    accept_invalid_certs: bool,
}

impl TlsConfig {
    fn from_request(request: &Value) -> Self {
        Self {
            root_cert_path: option_string(
                request,
                &["sslRootCert", "sslrootcert", "ssl-ca", "caCert"],
            ),
            client_cert_path: option_string(
                request,
                &["sslCert", "sslcert", "ssl-cert", "clientCert"],
            ),
            client_key_path: option_string(request, &["sslKey", "sslkey", "ssl-key", "clientKey"]),
            accept_invalid_certs: option_string(request, &["sslInsecure", "tlsInsecure"])
                .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes")),
        }
    }

    /// A client honouring the profile's TLS material.
    ///
    /// Returns the default client untouched when nothing is configured, so a
    /// plain `http://` endpoint keeps working exactly as before.
    fn build_client(&self) -> Result<reqwest::Client, String> {
        if self.root_cert_path.is_none()
            && self.client_cert_path.is_none()
            && self.client_key_path.is_none()
            && !self.accept_invalid_certs
        {
            return reqwest::Client::builder()
                .build()
                .map_err(|err| format!("HTTP client setup failed: {err}"));
        }

        let mut builder = reqwest::Client::builder();

        if let Some(path) = &self.root_cert_path {
            let pem = read_pem(path, "SSL root certificate")?;
            let bundle = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|err| format!("SSL root certificate at {path} is not valid PEM: {err}"))?;
            // `from_pem_bundle` answers Ok(vec![]) for a file with no PEM
            // blocks in it. Adding nothing and carrying on would fall back to
            // the system roots — the connection would succeed while verifying
            // against something other than the CA the user named.
            if bundle.is_empty() {
                return Err(format!(
                    "SSL root certificate at {path} contains no PEM certificate."
                ));
            }
            for certificate in bundle {
                builder = builder.add_root_certificate(certificate);
            }
        }

        // reqwest wants one PEM carrying both halves. Accept them as separate
        // files, which is how every other tool spells it, and join them here.
        match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let mut pem = read_pem(cert_path, "SSL client certificate")?;
                if !pem.ends_with(b"\n") {
                    pem.push(b'\n');
                }
                pem.extend_from_slice(&read_pem(key_path, "SSL client key")?);
                builder = builder.identity(
                    reqwest::Identity::from_pem(&pem)
                        .map_err(|err| format!("SSL client identity is not usable: {err}"))?,
                );
            }
            (Some(_), None) => {
                return Err("SSL client certificate needs a matching client key.".to_string())
            }
            (None, Some(_)) => {
                return Err("SSL client key needs a matching client certificate.".to_string())
            }
            (None, None) => {}
        }

        if self.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        builder
            .build()
            .map_err(|err| format!("TLS client setup failed: {err}"))
    }
}

fn read_pem(path: &str, label: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|err| format!("{label} at {path} could not be read: {err}"))
}

impl InfluxConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let base_url = option_string(request, &["connectionString", "url", "dsn"])
            .map(|url| normalize_url(&url))
            .transpose()?
            .unwrap_or_else(|| build_url(request));
        let database = option_string(
            request,
            &[
                "database",
                "db",
                "bucket",
                "bucketName",
                "organization",
                "org",
            ],
        )
        .or_else(|| database_from_url(&base_url))
        .unwrap_or_else(|| "default".to_string());
        // An InfluxDB API token and an OAuth access token are both sent as a
        // bearer token; the difference is where they came from. Accepting the
        // OAuth name lets a profile say which it holds rather than requiring it
        // to know they share a transport.
        let token = secret_option(
            request,
            &[
                "token",
                "apiKey",
                "influxToken",
                "bearerToken",
                "oauthAccessToken",
                "password",
            ],
        );
        let query_type = option_string(request, &["queryType", "language"])
            .unwrap_or_else(|| "sql".to_string())
            .to_ascii_lowercase();
        if query_type != "sql" {
            return Err(
                "this native InfluxDB driver currently supports SQL queryType only.".to_string(),
            );
        }
        let tls = TlsConfig::from_request(request);
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, token.as_deref());
        collect_url_auth(&base_url, &mut redaction_values);
        Ok(Self {
            base_url: strip_query_and_path_database(&base_url),
            database,
            token,
            query_type,
            tls,
            redaction_values,
        })
    }

    fn query_url(&self) -> String {
        format!(
            "{}/api/v3/query?database={}",
            self.base_url.trim_end_matches('/'),
            url_component(&self.database)
        )
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.base_url, "<influxdb-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

async fn ping_server(connection: &InfluxConnection) -> Result<Option<String>, String> {
    let response = connection
        .auth(
            connection
                .client
                .get(format!("{}/ping", connection.config.base_url)),
        )
        .send()
        .await
        .map_err(|err| format!("InfluxDB ping failed: {err}"))?;
    if !response.status().is_success() && response.status().as_u16() != 204 {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(format!("InfluxDB ping returned HTTP {status}: {text}"));
    }
    let version = response
        .headers()
        .get("X-Influxdb-Version")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(|value| format!("InfluxDB {value}"));
    Ok(version)
}

async fn run_query(
    connection: &InfluxConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let payload = json!({
        "query": sql,
        "type": connection.config.query_type,
    });
    let response = connection
        .auth(
            connection
                .client
                .post(connection.config.query_url())
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .json(&payload),
        )
        .send()
        .await
        .map_err(|err| format!("InfluxDB query request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("InfluxDB response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "InfluxDB query returned HTTP {status}: {}",
            text.trim().chars().take(500).collect::<String>()
        ));
    }
    Ok(influx_response_to_output(&text, cap))
}

async fn load_metadata(connection: &InfluxConnection) -> Result<Value, String> {
    let sql = "SELECT table_name, column_name, data_type \
               FROM information_schema.columns \
               WHERE table_schema = 'public' \
               ORDER BY table_name, ordinal_position";
    match run_query(connection, sql, 10_000).await {
        Ok((columns, rows, _)) => Ok(metadata_from_columns(
            &connection.config.database,
            &columns,
            rows,
        )),
        Err(_) => Ok(json!({
            "schemas": [{
                "name": connection.config.database,
                "objects": []
            }]
        })),
    }
}

fn influx_response_to_output(text: &str, cap: usize) -> QueryOutput {
    let rows_json = parse_rows(text);
    rows_to_output(rows_json, cap)
}

fn parse_rows(text: &str) -> Vec<Value> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return match value {
            Value::Array(rows) => rows,
            Value::Object(object) if object.contains_key("data") => object
                .get("data")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_else(|| vec![Value::Object(object)]),
            other => vec![other],
        };
    }
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                None
            } else {
                serde_json::from_str::<Value>(line).ok()
            }
        })
        .collect()
}

fn rows_to_output(rows_json: Vec<Value>, cap: usize) -> QueryOutput {
    let mut columns = Vec::new();
    for row in &rows_json {
        if let Some(object) = row.as_object() {
            for key in object.keys() {
                if !columns.iter().any(|column| column == key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    let rows = rows_json
        .iter()
        .take(cap)
        .map(|row| {
            if let Some(object) = row.as_object() {
                columns
                    .iter()
                    .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
                vec![row.clone()]
            }
        })
        .collect::<Vec<_>>();
    let truncated = rows_json.len() > cap;
    if columns.is_empty() && !rows_json.is_empty() {
        (vec!["value".to_string()], rows, truncated)
    } else {
        (columns, rows, truncated)
    }
}

fn metadata_from_columns(database: &str, columns: &[String], rows: QueryRows) -> Value {
    let table_idx = columns.iter().position(|column| column == "table_name");
    let column_idx = columns.iter().position(|column| column == "column_name");
    let type_idx = columns.iter().position(|column| column == "data_type");
    let mut objects: BTreeMap<String, ObjectMeta> = BTreeMap::new();
    let (Some(table_idx), Some(column_idx), Some(type_idx)) = (table_idx, column_idx, type_idx)
    else {
        return json!({ "schemas": [{ "name": database, "objects": [] }] });
    };
    for row in rows {
        let table = string_cell(&row, table_idx);
        let column = string_cell(&row, column_idx);
        if table.is_empty() || column.is_empty() {
            continue;
        }
        let data_type = string_cell(&row, type_idx);
        let object = objects.entry(table.clone()).or_insert_with(|| ObjectMeta {
            schema: database.to_string(),
            name: table,
            columns: Vec::new(),
        });
        object.columns.push(Value::Object(Map::from_iter([
            ("name".to_string(), Value::String(column)),
            ("dataType".to_string(), Value::String(data_type)),
            ("nullable".to_string(), Value::Bool(true)),
            ("ordinal".to_string(), json!(object.columns.len() + 1)),
        ])));
    }
    json!({
        "schemas": [{
            "name": database,
            "objects": objects
                .into_values()
                .map(|object| {
                    json!({
                        "schema": object.schema,
                        "name": object.name,
                        "kind": "table",
                        "columns": object.columns,
                        "indexes": [],
                        "primaryKey": [],
                        "foreignKeys": []
                    })
                })
                .collect::<Vec<_>>()
        }]
    })
}

fn connection(connection_id: &str) -> Result<InfluxConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| nested_string(container.get(*field)?))
        })
        .map(str::to_string)
}

fn secret_option(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| secret_string(container.get(*field)?))
        })
        .map(str::to_string)
}

fn option_u16(request: &Value, fields: &[&str]) -> Option<u16> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .and_then(number_value)
                    .and_then(|value| u16::try_from(value).ok())
            })
        })
}

fn option_bool(request: &Value, fields: &[&str]) -> Option<bool> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields
                .iter()
                .find_map(|field| bool_value(container.get(*field)?))
        })
}

fn nested_string(value: &Value) -> Option<&str> {
    match value {
        Value::String(value) => Some(value.as_str()).filter(|value| !value.trim().is_empty()),
        Value::Object(object) => ["value", "text", "url", "uri"]
            .iter()
            .find_map(|field| object.get(*field).and_then(Value::as_str))
            .filter(|value| !value.trim().is_empty()),
        _ => None,
    }
}

fn secret_string(value: &Value) -> Option<&str> {
    match value {
        Value::String(value) => Some(value.as_str()).filter(|value| !value.trim().is_empty()),
        Value::Object(object) => [
            "value",
            "secret",
            "token",
            "password",
            "apiKey",
            "accessToken",
        ]
        .iter()
        .find_map(|field| object.get(*field).and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty()),
        _ => None,
    }
}

fn number_value(value: &Value) -> Option<u64> {
    match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.trim().parse().ok(),
        Value::Object(object) => object.get("value").and_then(number_value),
        _ => None,
    }
}

fn bool_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        },
        Value::Object(object) => object.get("value").and_then(bool_value),
        _ => None,
    }
}

fn build_url(request: &Value) -> String {
    let host = option_string(request, &["host", "hostname"]).unwrap_or_else(|| "127.0.0.1".into());
    let port = option_u16(request, &["port"]).unwrap_or(8086);
    let tls = option_bool(request, &["tls", "ssl", "useTls"]).unwrap_or(false);
    let scheme = if tls { "https" } else { "http" };
    format!("{scheme}://{host}:{port}")
}

fn normalize_url(value: &str) -> Result<String, String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("InfluxDB URL is empty.".to_string());
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else if let Some(rest) = trimmed.strip_prefix("influxdb://") {
        Ok(format!("http://{}", rest.trim_end_matches('/')))
    } else {
        Ok(format!("http://{trimmed}"))
    }
}

fn strip_query_and_path_database(url: &str) -> String {
    let without_query = url.split(['?', '#']).next().unwrap_or(url);
    if let Some((scheme, rest)) = without_query.split_once("://") {
        let host = rest.split('/').next().unwrap_or(rest);
        format!("{scheme}://{host}")
    } else {
        without_query.to_string()
    }
}

fn database_from_url(url: &str) -> Option<String> {
    let path = url
        .split(['?', '#'])
        .next()
        .and_then(|url| url.split_once("://").map(|(_, rest)| rest).or(Some(url)))?
        .split_once('/')
        .map(|(_, path)| path)?;
    let database = path.trim_matches('/');
    (!database.is_empty()).then(|| database.to_string())
}

fn collect_url_auth(url: &str, redaction_values: &mut Vec<String>) {
    let Some((_, rest)) = url.split_once("://") else {
        return;
    };
    let Some((auth, _)) = rest.split_once('@') else {
        return;
    };
    for value in auth.split(':') {
        push_sensitive(redaction_values, Some(value));
    }
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        values.push(value.to_string());
    }
}

fn url_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn string_cell(row: &[Value], index: usize) -> String {
    row.get(index)
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            row.get(index)
                .map(Value::to_string)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string()
        })
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;
    use crate::{
        irodori_connector_call_json, irodori_connector_free_buffer, IrodoriConnectorBuffer,
    };

    fn buffer_from_str(value: &'static str) -> IrodoriConnectorBuffer {
        IrodoriConnectorBuffer {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }

    fn buffer_to_json(buffer: IrodoriConnectorBuffer) -> Value {
        let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
        let value = serde_json::from_slice(bytes).unwrap();
        irodori_connector_free_buffer(buffer);
        value
    }

    fn call(request: &'static str) -> Value {
        buffer_to_json(irodori_connector_call_json(buffer_from_str(request)))
    }

    #[test]
    fn query_requires_open_connection() {
        let response = call(r#"{"method":"query","connectionId":"missing","sql":"select 1"}"#);
        assert_eq!(response["ok"], false);
        assert_eq!(response["error"]["code"], "connector.connectionNotFound");
    }

    #[test]
    fn config_builds_url_database_and_redacts_token() {
        let request = json!({
            "host": "influx.example.com",
            "port": 8086,
            "database": "sensors",
            "tls": true,
            "secrets": {
                "token": "secret-token"
            }
        });
        let config = InfluxConfig::from_request(&request).unwrap();
        assert_eq!(config.base_url, "https://influx.example.com:8086");
        assert_eq!(config.database, "sensors");
        assert_eq!(
            config.redact("failed with secret-token"),
            "failed with ****"
        );
    }

    #[test]
    fn json_rows_shape_query_output() {
        let text = r#"[{"time":"2026-01-01T00:00:00Z","value":1.5},{"time":"2026-01-01T00:01:00Z","value":2.0}]"#;
        let (columns, rows, truncated) = influx_response_to_output(text, 1);
        assert_eq!(columns, vec!["time", "value"]);
        assert_eq!(rows, vec![vec![json!("2026-01-01T00:00:00Z"), json!(1.5)]]);
        assert!(truncated);
    }

    #[test]
    fn ndjson_rows_shape_query_output() {
        let text = "{\"series\":\"cpu\",\"value\":1}\n{\"series\":\"mem\",\"value\":2}\n";
        let (columns, rows, truncated) = influx_response_to_output(text, 10);
        assert_eq!(columns, vec!["series", "value"]);
        assert_eq!(
            rows,
            vec![vec![json!("cpu"), json!(1)], vec![json!("mem"), json!(2)]]
        );
        assert!(!truncated);
    }

    #[test]
    fn metadata_groups_information_schema_columns() {
        let columns = vec![
            "table_name".to_string(),
            "column_name".to_string(),
            "data_type".to_string(),
        ];
        let rows = vec![
            vec![json!("cpu"), json!("time"), json!("timestamp")],
            vec![json!("cpu"), json!("usage"), json!("float64")],
        ];
        let metadata = metadata_from_columns("sensors", &columns, rows);
        let object = &metadata["schemas"][0]["objects"][0];
        assert_eq!(object["schema"], "sensors");
        assert_eq!(object["name"], "cpu");
        assert_eq!(object["columns"][1]["dataType"], "float64");
    }

    #[test]
    fn reads_tls_paths_from_the_connector_options() {
        let tls = TlsConfig::from_request(&json!({
            "profile": {
                "options": {
                    "sslRootCert": "/etc/ssl/ca.pem",
                    "sslCert": "/etc/ssl/client.pem",
                    "sslKey": "/etc/ssl/client.key"
                }
            }
        }));
        assert_eq!(tls.root_cert_path.as_deref(), Some("/etc/ssl/ca.pem"));
        assert_eq!(tls.client_cert_path.as_deref(), Some("/etc/ssl/client.pem"));
        assert_eq!(tls.client_key_path.as_deref(), Some("/etc/ssl/client.key"));
        assert!(!tls.accept_invalid_certs);
    }

    #[test]
    fn accepts_the_driver_spellings_of_the_tls_options() {
        let tls = TlsConfig::from_request(&json!({
            "profile": { "options": { "sslrootcert": "/ca.pem", "ssl-cert": "/c.pem" } }
        }));
        assert_eq!(tls.root_cert_path.as_deref(), Some("/ca.pem"));
        assert_eq!(tls.client_cert_path.as_deref(), Some("/c.pem"));
    }

    #[test]
    fn a_profile_without_tls_options_keeps_the_plain_client() {
        let tls = TlsConfig::from_request(&json!({ "profile": {} }));
        assert_eq!(tls, TlsConfig::default());
        assert!(tls.build_client().is_ok());
    }

    #[test]
    fn half_a_client_identity_is_rejected_with_a_usable_message() {
        // Silently ignoring the half that was supplied would connect without
        // the certificate the user asked for.
        let cert_only = TlsConfig {
            client_cert_path: Some("/etc/ssl/client.pem".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            cert_only.build_client().unwrap_err(),
            "SSL client certificate needs a matching client key."
        );

        let key_only = TlsConfig {
            client_key_path: Some("/etc/ssl/client.key".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            key_only.build_client().unwrap_err(),
            "SSL client key needs a matching client certificate."
        );
    }

    #[test]
    fn an_unreadable_certificate_names_the_file_and_the_field() {
        let tls = TlsConfig {
            root_cert_path: Some("/definitely/not/here.pem".into()),
            ..TlsConfig::default()
        };
        let err = tls.build_client().unwrap_err();
        assert!(
            err.starts_with("SSL root certificate at /definitely/not/here.pem"),
            "{err}"
        );
    }

    #[test]
    fn a_certificate_file_with_no_pem_block_is_rejected() {
        // reqwest answers Ok(vec![]) rather than an error for a file with no
        // PEM blocks, so without an explicit emptiness check this connection
        // would silently verify against the system roots instead of the named
        // CA.
        let dir = std::env::temp_dir().join("irodori-influxdb-tls-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("not-a-cert.pem");
        std::fs::write(&path, b"this is not a certificate").unwrap();

        let tls = TlsConfig {
            root_cert_path: Some(path.to_string_lossy().into_owned()),
            ..TlsConfig::default()
        };
        let err = tls.build_client().unwrap_err();
        assert!(err.contains("contains no PEM certificate"), "{err}");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn an_oauth_access_token_is_accepted_under_its_own_name() {
        // Same transport as an API token, different origin — a profile should
        // be able to say which it is holding.
        for field in ["token", "influxToken", "oauthAccessToken", "bearerToken"] {
            let config = InfluxConfig::from_request(&json!({
                "profile": { "host": "influx.local", "options": { field: "tok-value" } }
            }))
            .expect("config");
            assert_eq!(config.token.as_deref(), Some("tok-value"), "{field}");
        }
    }
}
