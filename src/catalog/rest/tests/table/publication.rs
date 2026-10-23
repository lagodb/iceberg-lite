use http::StatusCode;

use super::super::support::{CATALOG_URI, ExpectedExchange, RestTestFixture};
use super::{
    CREATE_TABLE_RESPONSE, LOAD_TABLE_RESPONSE, table_ident, table_schema,
    transaction_table,
};
use crate::transaction::{ApplyTransactionAction, PreparedTransaction, Transaction};
use crate::{ErrorKind, NamespaceIdent, TableCreation};

const CREATE_ENDPOINT: &str = "POST /v1/{prefix}/namespaces/{namespace}/tables";
const UPDATE_ENDPOINT: &str =
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}";

#[test]
fn staged_create_requires_both_endpoints_before_sending_the_request() {
    for endpoint in [CREATE_ENDPOINT, UPDATE_ENDPOINT] {
        let fixture = RestTestFixture::default();
        fixture.expect_config(serde_json::json!({
            "defaults": {},
            "overrides": {},
            "endpoints": [endpoint]
        }));
        let catalog = fixture.bound_catalog([]);
        let creation = TableCreation::builder()
            .name("test1".to_owned())
            .schema(table_schema())
            .build();

        let error = catalog
            .stage_create_table(&NamespaceIdent::new("ns1".to_owned()), creation)
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(fixture.storage_configs().is_empty());
        fixture.assert_finished();
    }
}

#[test]
fn single_table_preparation_requires_the_update_endpoint() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({
        "defaults": {},
        "overrides": {},
        "endpoints": [
            CREATE_ENDPOINT,
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
        ]
    }));
    let catalog = fixture.bound_catalog([]);
    let table = transaction_table(&fixture, table_ident("test1"));
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .update_table_properties()
        .set("owner".to_owned(), "writer".to_owned())
        .apply(transaction)
        .unwrap();
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .respond(StatusCode::OK, LOAD_TABLE_RESPONSE),
    );
    let PreparedTransaction::Commit(commit) = transaction.prepare(&catalog).unwrap()
    else {
        panic!("property update must produce a commit");
    };

    assert_eq!(
        catalog.prepare_table_commit(commit).unwrap_err().kind(),
        ErrorKind::FeatureUnsupported,
    );
    fixture.assert_finished();
}

#[test]
fn action_free_staged_create_publishes_with_absent_empty_or_required_endpoints() {
    for endpoints in [
        None,
        Some(vec![]),
        Some(vec![CREATE_ENDPOINT, UPDATE_ENDPOINT]),
    ] {
        let fixture = RestTestFixture::default();
        let mut config = serde_json::json!({"defaults": {}, "overrides": {}});
        if let Some(endpoints) = endpoints {
            config["endpoints"] = serde_json::json!(endpoints);
        }
        fixture.expect_config(config);
        fixture.expect(
            ExpectedExchange::post(format!("{CATALOG_URI}/v1/namespaces/ns1/tables"))
                .json_body(serde_json::json!({
                    "name": "test1",
                    "schema": table_schema(),
                    "stage-create": true,
                    "properties": {"format-version": "2"}
                }))
                .respond(StatusCode::OK, CREATE_TABLE_RESPONSE),
        );
        fixture.expect(
            ExpectedExchange::post(format!(
                "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
            ))
            .respond(StatusCode::OK, ""),
        );
        let catalog = fixture.bound_catalog([]);
        let staged = catalog
            .stage_create_table(
                &NamespaceIdent::new("ns1".to_owned()),
                TableCreation::builder()
                    .name("test1".to_owned())
                    .schema(table_schema())
                    .build(),
            )
            .unwrap();
        let PreparedTransaction::Commit(commit) =
            staged.transaction().prepare(&catalog).unwrap()
        else {
            panic!("staged create must produce a commit");
        };
        let request = catalog.prepare_table_commit(commit).unwrap();
        catalog.send_prepared_commit(request).unwrap();
        fixture.assert_finished();
    }
}

#[test]
fn property_publication_serializes_the_base_table_uuid() {
    let fixture = RestTestFixture::default();
    fixture.expect_config(serde_json::json!({"defaults": {}, "overrides": {}}));
    fixture.expect(
        ExpectedExchange::get(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .respond(StatusCode::OK, LOAD_TABLE_RESPONSE),
    );
    let table = transaction_table(&fixture, table_ident("test1"));
    fixture.expect(
        ExpectedExchange::post(format!(
            "{CATALOG_URI}/v1/namespaces/ns1/tables/test1"
        ))
        .json_body(serde_json::json!({
            "identifier": {"namespace": ["ns1"], "name": "test1"},
            "requirements": [{"type": "assert-table-uuid", "uuid": table.metadata().uuid()}],
            "updates": [
                {"action": "set-properties", "updates": {"owner": "writer"}},
                {"action": "remove-properties", "removals": []}
            ]
        }))
        .respond(StatusCode::OK, ""),
    );
    let catalog = fixture.bound_catalog([]);
    let transaction = Transaction::new(&table);
    let transaction = transaction
        .update_table_properties()
        .set("owner".to_owned(), "writer".to_owned())
        .apply(transaction)
        .unwrap();
    let PreparedTransaction::Commit(commit) = transaction.prepare(&catalog).unwrap()
    else {
        panic!("property update must produce a commit");
    };
    let request = catalog.prepare_table_commit(commit).unwrap();
    catalog.send_prepared_commit(request).unwrap();
    fixture.assert_finished();
}
