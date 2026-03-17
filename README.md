# PI Web API Mock Server

Mock AVEVA / OSIsoft PI Web API server with:
- 3 factory asset databases
- realistic nested element hierarchies (average depth around 5)
- child elements and attributes
- element/attribute templates with base template inheritance
- deterministic pseudo-random values over time
- HTTP Basic authentication with multiple users

## Run

```bash
python3 mock_piwebapi_server.py --host 0.0.0.0 --port 8080
```

Base URL:

```text
http://localhost:8080/piwebapi
```

## Authentication

All `/piwebapi/*` endpoints require Basic Auth.

Default users:
- `operator_north / north123`
- `operator_south / south123`
- `supervisor / supervisor123`
- `admin / admin123`

You can override users with:

```bash
export MOCK_PIWEBAPI_USERS='alice:alicePwd,bob:bobPwd,carol:carolPwd'
python3 mock_piwebapi_server.py
```

## Data Shape

Databases:
- `Factory-North`
- `Factory-South`
- `Factory-West`

Each database has a hierarchy like:

```text
Factory-<Region>
  Area-01..08
    Line-01..05
      Unit-01..04
        Station-01..03
          Cell-01..02
```

This yields thousands of elements and attributes across the 3 databases.

## Main Endpoints

- `GET /piwebapi`
- `GET /piwebapi/assetservers`
- `GET /piwebapi/assetservers/{serverWebId}`
- `GET /piwebapi/assetservers/{serverWebId}/assetdatabases`
- `GET /piwebapi/elementtemplates`
- `GET /piwebapi/elementtemplates/{elementTemplateWebId}`
- `GET /piwebapi/elementtemplates/{elementTemplateWebId}/attributetemplates`
- `GET /piwebapi/attributetemplates/{attributeTemplateWebId}`
- `GET /piwebapi/assetdatabases/{dbWebId}/elementtemplates`
- `GET /piwebapi/assetdatabases/{dbWebId}/elements`
- `GET /piwebapi/assetdatabases/{dbWebId}/elements?path=\\Factory-North\\Factory-North\\Area-01\\Line-01\\Unit-01\\Station-01\\Cell-01`
- `GET /piwebapi/elements/{elementWebId}`
- `GET /piwebapi/elements/{elementWebId}/elements`
- `GET /piwebapi/elements/{elementWebId}/attributes`
- `GET /piwebapi/attributes/search?databaseWebId={dbWebId}&query=Element:{Root:'Area-01\\Line-02\\Unit-02\\Station-01\\Cell-01' Name:'*'}`
- `GET /piwebapi/attributes/{attributeWebId}/attributes` (sub-attributes)
- `GET /piwebapi/attributes/{attributeWebId}`
- `GET /piwebapi/streams/{attributeWebId}/value?time=2025-01-01T10:00:00Z`
- `GET /piwebapi/streams/{attributeWebId}/recorded?startTime=2025-01-01T00:00:00Z&endTime=2025-01-01T03:00:00Z&interval=15m`

## Deterministic Values

Values are generated from:
- server seed (`--seed`, default `piwebapi-mock-seed`)
- attribute `WebId`
- timestamp

So if you request the same attribute and same timestamp/time range twice, returned values are identical.

## Template Notes

- Elements expose `TemplateName` and `Links.Template`.
- Attributes expose `TemplateName` and `Links.Template`.
- At least one template has a base template (`BaseTemplateName` + `Links.BaseTemplate`), e.g.:
  - `TPL_Cell` base template is `TPL_Station`

## Quick Example

```bash
# list asset servers
curl -u supervisor:supervisor123 \
  http://localhost:8080/piwebapi/assetservers
```
