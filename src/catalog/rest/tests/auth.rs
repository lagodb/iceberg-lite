use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use http::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use http::{Method, StatusCode};

use super::super::auth::{AuthManager, AuthSession, OAuth2Manager};
use super::super::catalog::{
    REST_CATALOG_PROP_AUTH_TYPE, REST_CATALOG_PROP_URI, RestCatalogBuilder,
    RestCatalogConfig, RestSessionCatalog,
};
use super::super::client::HttpClient;
use super::super::request::HttpRequest;
use super::support::{
    CATALOG_URI, ExpectedExchange, MockHttpTransport, RestTestFixture,
};
use crate::catalog::rest::LoadTableResult;
use crate::table::Table;
use crate::transaction::PreparedTableCommit;
use crate::{
    CatalogBuilder, Error, ErrorKind, NamespaceIdent, Result, SessionCatalog,
    SessionContext, TableCommit, TableIdent,
};

const TOKEN_RESPONSE: &str = r#"{
    "access_token":"tok",
    "token_type":"Bearer",
    "issued_token_type":"urn:ietf:params:oauth:token-type:access_token",
    "expires_in":86400
}"#;

#[test]
fn credential_is_exchanged_once_and_the_handshake_is_authenticated() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::post(format!("{CATALOG_URI}/v1/oauth/tokens"))
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([("credential", "client1:secret1")]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn oauth_optional_parameters_and_explicit_endpoint_are_preserved() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::post("https://auth.test/token")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "custom_scope"),
                ("audience", "custom_audience"),
                ("resource", "custom_resource"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([
        ("credential", "client1:secret1"),
        ("oauth2-server-uri", "https://auth.test/token"),
        ("scope", "custom_scope"),
        ("audience", "custom_audience"),
        ("resource", "custom_resource"),
    ]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn configured_token_takes_precedence_over_credential() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Bearer seeded")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer seeded")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog =
        fixture.catalog([("token", "seeded"), ("credential", "client1:secret1")]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn explicit_none_disables_auth_even_when_a_token_is_present() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .missing_header(AUTHORIZATION)
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .missing_header(AUTHORIZATION)
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture
        .catalog([(REST_CATALOG_PROP_AUTH_TYPE, "none"), ("token", "ignored")]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn explicit_authorization_header_overrides_oauth_on_the_wire() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/config"))
            .header(AUTHORIZATION, "Basic xyz")
            .respond(StatusCode::OK, r#"{"defaults":{},"overrides":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Basic xyz")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );

    let catalog = fixture.catalog([
        ("token", "oauth-token"),
        ("header.authorization", "Basic xyz"),
    ]);
    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    fixture.assert_finished();
}

#[test]
fn concurrent_authentication_performs_one_token_exchange() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post("https://auth.test/token")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    let config =
        RestCatalogConfig::for_test(CATALOG_URI, HashMap::new(), transport.clone());
    let client = HttpClient::new(&config).unwrap();
    let manager = OAuth2Manager::new("https://auth.test/token")
        .with_credential(Some("client1".to_owned()), "secret1".to_owned());
    let session: Arc<dyn AuthSession> =
        Arc::from(manager.init_session(&client, &HashMap::new()).unwrap());

    let workers: Vec<_> = (0..8)
        .map(|_| {
            let session = session.clone();
            thread::spawn(move || {
                let mut request = HttpRequest::new(
                    Method::GET,
                    "https://rest.test/v1/config".parse().unwrap(),
                );
                session.authenticate(&mut request).unwrap();
                request
                    .headers()
                    .get(AUTHORIZATION)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
        })
        .collect();
    let bearers: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();

    assert!(bearers.iter().all(|bearer| bearer == "Bearer tok"));
    assert_eq!(transport.request_count(), 1);
    transport.assert_finished();
}

#[test]
fn oauth_exchange_uses_manager_headers_not_catalog_client_headers() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post("https://auth.test/token")
            .header("x-from", "manager")
            .missing_header("x-catalog-only")
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([(
            "header.x-catalog-only".to_owned(),
            "not-on-token-requests".to_owned(),
        )]),
        transport.clone(),
    );
    let client = HttpClient::new(&config).unwrap();
    let manager = OAuth2Manager::new("https://auth.test/token")
        .with_credential(Some("client1".to_owned()), "secret1".to_owned())
        .with_extra_headers(HeaderMap::from_iter([(
            HeaderName::from_static("x-from"),
            HeaderValue::from_static("manager"),
        )]));
    let session = manager.init_session(&client, &HashMap::new()).unwrap();
    let mut request = HttpRequest::new(
        Method::GET,
        "https://rest.test/v1/namespaces".parse().unwrap(),
    );

    session.authenticate(&mut request).unwrap();

    assert_eq!(request.headers().get(AUTHORIZATION).unwrap(), "Bearer tok");
    transport.assert_finished();
}

#[test]
fn default_oauth_endpoint_follows_the_merged_catalog_uri() {
    let transport = Arc::new(MockHttpTransport::default());
    transport.expect(
        ExpectedExchange::post("https://redirected.test/v1/oauth/tokens")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    let config = RestCatalogConfig::for_test(
        CATALOG_URI,
        HashMap::from([("credential".to_owned(), "client1:secret1".to_owned())]),
        transport.clone(),
    );
    let client = HttpClient::new(&config).unwrap();
    let manager = OAuth2Manager::from_config(&config).unwrap();
    let session = manager
        .catalog_session(
            &client,
            &HashMap::from([(
                REST_CATALOG_PROP_URI.to_owned(),
                "https://redirected.test".to_owned(),
            )]),
        )
        .unwrap();
    let mut request = HttpRequest::new(
        Method::GET,
        "https://redirected.test/v1/namespaces".parse().unwrap(),
    );

    session.authenticate(&mut request).unwrap();

    assert_eq!(request.headers().get(AUTHORIZATION).unwrap(), "Bearer tok");
    transport.assert_finished();
}

#[test]
fn injected_manager_keeps_its_endpoint_headers_and_parameters() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {"credential": "client1:secret1"},
        "overrides": {}
    }));
    fixture.expect(
        ExpectedExchange::post("https://auth.test/custom/token")
            .header("x-tenant", "t1")
            .form([
                ("grant_type", "client_credentials"),
                ("client_id", "client1"),
                ("client_secret", "secret1"),
                ("scope", "catalog"),
                ("audience", "aud-1"),
            ])
            .respond(StatusCode::OK, TOKEN_RESPONSE),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header(AUTHORIZATION, "Bearer tok")
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );
    let manager = OAuth2Manager::new("https://auth.test/custom/token")
        .with_extra_headers(HeaderMap::from_iter([(
            HeaderName::from_static("x-tenant"),
            HeaderValue::from_static("t1"),
        )]))
        .with_extra_oauth_params(HashMap::from([(
            "audience".to_owned(),
            "aud-1".to_owned(),
        )]));
    let catalog = fixture.catalog_with_auth_manager([], manager);

    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();
    fixture.assert_finished();
}

#[derive(Debug)]
struct PlainSession;

impl AuthSession for PlainSession {
    fn authenticate(&self, _request: &mut HttpRequest) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct CapturingManager {
    init_props: Arc<Mutex<Option<HashMap<String, String>>>>,
    catalog_props: Arc<Mutex<Option<HashMap<String, String>>>>,
}

impl AuthManager for CapturingManager {
    fn init_session(
        &self,
        _client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        *self.init_props.lock().unwrap() = Some(props.clone());
        Ok(Box::new(PlainSession))
    }

    fn catalog_session(
        &self,
        _client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        *self.catalog_props.lock().unwrap() = Some(props.clone());
        Ok(Arc::new(PlainSession))
    }
}

#[test]
fn auth_manager_receives_user_then_merged_properties() {
    let fixture = RestTestFixture::default();
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/config?warehouse=client-wh"
        ))
        .respond(
            StatusCode::OK,
            r#"{"defaults":{"warehouse":"default-wh"},"overrides":{"warehouse":"override-wh"}}"#,
        ),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .respond(StatusCode::OK, r#"{"namespaces":[]}"#),
    );
    let init_props = Arc::new(Mutex::new(None));
    let catalog_props = Arc::new(Mutex::new(None));
    let manager = CapturingManager {
        init_props: init_props.clone(),
        catalog_props: catalog_props.clone(),
    };
    let catalog = fixture.catalog_with_auth_manager(
        [("warehouse", "client-wh"), ("custom", "user")],
        manager,
    );

    catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap();

    let init = init_props.lock().unwrap().clone().unwrap();
    assert_eq!(
        init.get(REST_CATALOG_PROP_URI).map(String::as_str),
        Some(CATALOG_URI)
    );
    assert_eq!(init.get("warehouse").map(String::as_str), Some("client-wh"));
    assert_eq!(init.get("custom").map(String::as_str), Some("user"));
    let merged = catalog_props.lock().unwrap().clone().unwrap();
    assert_eq!(
        merged.get("warehouse").map(String::as_str),
        Some("override-wh")
    );
    fixture.assert_finished();
}

#[test]
fn injected_auth_manager_overrides_the_configured_auth_type() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    let manager = CapturingManager {
        init_props: Arc::new(Mutex::new(None)),
        catalog_props: Arc::new(Mutex::new(None)),
    };
    let catalog = fixture.catalog_with_auth_manager(
        [(REST_CATALOG_PROP_AUTH_TYPE, "kerberos")],
        manager,
    );
    let endpoint: super::super::Endpoint =
        "GET /v1/{prefix}/namespaces".parse().unwrap();

    assert!(catalog.supports_endpoint(&endpoint).unwrap());
    fixture.assert_finished();
}

#[derive(Debug)]
struct GuardSession(Arc<AtomicBool>);

impl Drop for GuardSession {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl AuthSession for GuardSession {
    fn authenticate(&self, _request: &mut HttpRequest) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct GuardManager(Arc<AtomicBool>);

impl AuthManager for GuardManager {
    fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(GuardSession(self.0.clone())))
    }

    fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        if !self.0.load(Ordering::SeqCst) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "init session must be dropped before catalog_session",
            ));
        }
        Ok(Arc::new(PlainSession))
    }
}

#[test]
fn init_session_is_dropped_before_catalog_session_is_created() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    let dropped = Arc::new(AtomicBool::new(false));
    let catalog =
        fixture.catalog_with_auth_manager([], GuardManager(dropped.clone()));

    let endpoint: super::super::Endpoint =
        "GET /v1/{prefix}/namespaces".parse().unwrap();
    assert!(catalog.supports_endpoint(&endpoint).unwrap());
    assert!(dropped.load(Ordering::SeqCst));
    fixture.assert_finished();
}

#[test]
fn unknown_auth_type_fails_before_any_request() {
    let fixture = RestTestFixture::default();
    let catalog = fixture.catalog([(REST_CATALOG_PROP_AUTH_TYPE, "kerberos")]);
    let error = catalog
        .list_namespaces(&SessionContext::empty(), None)
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(error.to_string().contains(REST_CATALOG_PROP_AUTH_TYPE));
    assert_eq!(fixture.transport().request_count(), 0);
}

#[derive(Debug)]
struct ContextSession(String);

impl AuthSession for ContextSession {
    fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
        request.headers_mut().insert(
            "x-session-id",
            HeaderValue::from_str(&self.0).expect("valid test session ID"),
        );
        Ok(())
    }
}

#[derive(Debug)]
struct ContextManager {
    catalog_session: Arc<dyn AuthSession>,
    seen_session_ids: Arc<Mutex<Vec<String>>>,
    fail_contextual_session: bool,
}

impl AuthManager for ContextManager {
    fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(PlainSession))
    }

    fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::clone(&self.catalog_session))
    }

    fn contextual_session(
        &self,
        context: &SessionContext,
        catalog_session: Arc<dyn AuthSession>,
    ) -> Result<Arc<dyn AuthSession>> {
        assert!(Arc::ptr_eq(&catalog_session, &self.catalog_session));
        self.seen_session_ids
            .lock()
            .unwrap()
            .push(context.session_id().to_owned());
        if self.fail_contextual_session {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "contextual session failure",
            ));
        }
        Ok(Arc::new(ContextSession(context.session_id().to_owned())))
    }
}

fn context_catalog(
    fixture: &RestTestFixture,
    fail_contextual_session: bool,
) -> (RestSessionCatalog, Arc<Mutex<Vec<String>>>) {
    let seen_session_ids = Arc::new(Mutex::new(Vec::new()));
    let catalog = fixture.catalog_with_auth_manager(
        [],
        ContextManager {
            catalog_session: Arc::new(PlainSession),
            seen_session_ids: Arc::clone(&seen_session_ids),
            fail_contextual_session,
        },
    );
    (catalog, seen_session_ids)
}

#[test]
fn test_contextual_session_authenticates_each_catalog_request() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces"))
            .header("x-session-id", "session-123")
            .respond(
                StatusCode::OK,
                r#"{"namespaces":[["ns1"]],"next-page-token":"next"}"#,
            ),
    );
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces?pageToken=next"))
            .header("x-session-id", "session-123")
            .respond(StatusCode::OK, r#"{"namespaces":[["ns2"]]}"#),
    );
    let (catalog, seen_session_ids) = context_catalog(&fixture, false);
    let context = SessionContext::builder().session_id("session-123").build();

    let namespaces = catalog.list_namespaces(&context, None).unwrap();

    assert_eq!(
        namespaces,
        vec![
            NamespaceIdent::new("ns1".to_owned()),
            NamespaceIdent::new("ns2".to_owned()),
        ]
    );
    assert_eq!(
        *seen_session_ids.lock().unwrap(),
        vec!["session-123", "session-123",]
    );
    fixture.assert_finished();
}

#[test]
fn test_contextual_session_authenticates_namespace_exists_head() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {},
        "endpoints": ["HEAD /v1/{prefix}/namespaces/{namespace}"]
    }));
    fixture.expect(
        ExpectedExchange::head(format!("{CATALOG_URI}/v1/namespaces/ns1"))
            .header("x-session-id", "namespace-session")
            .respond(StatusCode::NO_CONTENT, ""),
    );
    let (catalog, seen_session_ids) = context_catalog(&fixture, false);
    let context = SessionContext::builder()
        .session_id("namespace-session")
        .build();

    assert!(
        catalog
            .namespace_exists(&context, &NamespaceIdent::new("ns1".to_owned()))
            .unwrap()
    );
    assert_eq!(*seen_session_ids.lock().unwrap(), vec!["namespace-session"]);
    fixture.assert_finished();
}

#[test]
fn test_contextual_session_authenticates_table_exists_head() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {},
        "endpoints": ["HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"]
    }));
    fixture.expect(
        ExpectedExchange::head(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/table1"
        ))
        .header("x-session-id", "table-session")
        .respond(StatusCode::NO_CONTENT, ""),
    );
    let (catalog, seen_session_ids) = context_catalog(&fixture, false);
    let context = SessionContext::builder()
        .session_id("table-session")
        .build();
    let table =
        TableIdent::new(NamespaceIdent::new("ns1".to_owned()), "table1".to_owned());

    assert!(catalog.table_exists(&context, &table).unwrap());
    assert_eq!(*seen_session_ids.lock().unwrap(), vec!["table-session"]);
    fixture.assert_finished();
}

#[test]
fn test_contextual_session_authenticates_write_operation() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::delete(format!("{CATALOG_URI}/v1/namespaces/ns1"))
            .header("x-session-id", "write-session")
            .respond(StatusCode::NO_CONTENT, ""),
    );
    let (catalog, seen_session_ids) = context_catalog(&fixture, false);
    let context = SessionContext::builder()
        .session_id("write-session")
        .build();

    catalog
        .drop_namespace(&context, &NamespaceIdent::new("ns1".to_owned()))
        .unwrap();

    assert_eq!(*seen_session_ids.lock().unwrap(), vec!["write-session"]);
    fixture.assert_finished();
}

#[test]
fn test_contextual_session_error_prevents_operation_request() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    let (catalog, seen_session_ids) = context_catalog(&fixture, true);
    let context = SessionContext::builder()
        .session_id("failing-session")
        .build();

    let error = catalog.list_namespaces(&context, None).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert_eq!(error.message(), "contextual session failure");
    assert_eq!(*seen_session_ids.lock().unwrap(), vec!["failing-session"]);
    assert_eq!(fixture.transport().request_count(), 1);
    fixture.assert_finished();
}

#[test]
fn contextual_sessions_reach_get_fallbacks_and_change_between_operations() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {},
        "endpoints": [
            "GET /v1/{prefix}/namespaces/{namespace}",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
        ]
    }));
    fixture.expect(
        ExpectedExchange::get(format!("{CATALOG_URI}/v1/namespaces/ns1"))
            .header("x-session-id", "namespace-session")
            .respond(StatusCode::OK, r#"{"namespace":["ns1"],"properties":{}}"#),
    );
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/table1"
        ))
        .header("x-session-id", "table-session")
        .respond(StatusCode::OK, "status-only response"),
    );
    let (catalog, seen_session_ids) = context_catalog(&fixture, false);
    let namespace = NamespaceIdent::new("ns1".to_owned());
    let namespace_context = SessionContext::builder()
        .session_id("namespace-session")
        .build();
    let table_context = SessionContext::builder()
        .session_id("table-session")
        .build();

    assert!(
        catalog
            .namespace_exists(&namespace_context, &namespace)
            .unwrap()
    );
    assert!(
        catalog
            .table_exists(
                &table_context,
                &TableIdent::new(namespace, "table1".to_owned()),
            )
            .unwrap()
    );
    assert_eq!(
        *seen_session_ids.lock().unwrap(),
        vec!["namespace-session", "table-session",]
    );
    assert!(fixture.storage_configs().is_empty());
    fixture.assert_finished();
}

#[test]
fn prepared_transaction_uses_bound_context_only_when_sent() {
    for fail_contextual_session in [false, true] {
        let fixture = RestTestFixture::default();
        fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
        if !fail_contextual_session {
            fixture.expect(
                ExpectedExchange::post(format!(
                    "{CATALOG_URI}/v1/transactions/commit"
                ))
                .header("x-session-id", "transaction-session")
                .json_body(serde_json::json!({
                        "table-changes": [{
                            "identifier": {"namespace": ["ns1"], "name": "table1"},
                            "requirements": [],
                            "updates": []
                        }]
                }))
                .respond(StatusCode::NO_CONTENT, ""),
            );
        }
        let seen_session_ids = Arc::new(Mutex::new(Vec::new()));
        let catalog = RestCatalogBuilder::default()
            .with_http_transport(fixture.transport())
            .with_session_context(
                SessionContext::builder()
                    .session_id("transaction-session")
                    .build(),
            )
            .with_auth_manager(ContextManager {
                catalog_session: Arc::new(PlainSession),
                seen_session_ids: Arc::clone(&seen_session_ids),
                fail_contextual_session,
            })
            .load(
                "test",
                HashMap::from([(
                    REST_CATALOG_PROP_URI.to_owned(),
                    CATALOG_URI.to_owned(),
                )]),
            )
            .unwrap();
        let identifier = TableIdent::new(
            NamespaceIdent::new("ns1".to_owned()),
            "table1".to_owned(),
        );
        let loaded: LoadTableResult = serde_json::from_str(include_str!(
            "../../../../testdata/rest/load_table_response.json"
        ))
        .unwrap();
        let table = Table::builder()
            .identifier(identifier.clone())
            .file_io(fixture.file_io())
            .metadata(loaded.metadata)
            .metadata_location(loaded.metadata_location.unwrap())
            .build()
            .unwrap();
        let commit = TableCommit::builder()
            .ident(identifier)
            .file_io(fixture.file_io())
            .requirements(Vec::new())
            .updates(Vec::new())
            .build();

        let prepared = catalog
            .prepare_transaction_commit(vec![PreparedTableCommit::new(table, commit)])
            .unwrap();
        assert_eq!(fixture.transport().request_count(), 1);
        assert!(seen_session_ids.lock().unwrap().is_empty());
        let result = catalog.send_prepared_commit(prepared);
        if fail_contextual_session {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unexpected);
            assert_eq!(error.message(), "contextual session failure");
            assert_eq!(fixture.transport().request_count(), 1);
        } else {
            result.unwrap();
            assert_eq!(fixture.transport().request_count(), 2);
        }
        assert_eq!(
            *seen_session_ids.lock().unwrap(),
            vec!["transaction-session"]
        );
        fixture.assert_finished();
    }
}
