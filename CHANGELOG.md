# Changes

## Version 0.4.17

- Fixing error swallowing with `simple_query` and MARS (#105)
- Fixing transaction descriptor reading (#105)
- Fixing envchange token reads (#105)

## Version 0.4.16

- Handle all MARS results properly (#102)

## Version 0.4.14

- Support alternatively `BigNumber` when dealing with numeric values.
- Document feature flags

## Version 0.4.13

- Realizing UTF-16 works just fine with SQL Server. Reverting the UCS2, but
  still keeping the faster writes.

## Version 0.4.12

*SKIP this, go directly to 0.4.13*

- A typo fix in README (#94)
- Faster string writes with better length handling. UCS2 for writes (#95).

## Version 0.4.11

- Allow disabling TLS in connection string (#89)
- Use connection-string for ado.net parsing (#91)
- Handle JDBC connection strings (#92)

## Version 0.4.10

- Handling nullable int values, fix for #78 (#80)
- Reflect tweaks to upstream libgssapi crate (#81)
- Skip default features in libgssapi (for macOS support)
- Handle env change Routing request (#87)

## Version 0.4.9

- BREAKING: `AuthMethod::WindowsIntegrated` renamed to `AuthMethod::Integrated`.
- Use GSSAPI for IntegratedSecurity on Unix platforms
- Fix module docs for examples
- Make `packet_id` wrapping explicit
- Add DNS feature to Tokio

## Version 0.4.8

- BREAKING: `ColumnData::I8(i8)` is now `ColumnData::U8(u8)` due to misunderstanding how `tinyint` works. (#71)
- Skip any received `done_rows` amounts and avoid creating extra resultsets (#67)
- Actually run the chrono tests (#72)
- Fix GUID byte ordering (#69)
- Fix null time/datetime2/datetimeoffset handling (#73)
- Null image data should be `Binary`, not `String`

## Version 0.4.7

- Pass hostname to TLS handshake, allowing usage with AzureSQL using
  `TrustServerCertificate=no`
  ([#62](https://github.com/prisma/tiberius/pull/62))

## Version 0.4.5

- Documenting type conversions and re-exporting chrono types
  ([#60](https://github.com/prisma/tiberius/pull/60))

## Version 0.4.4

- Fixing multi-part table names in `IMAGE`, `TEXT` and `NTEXT` column metadata
  ([#58](https://github.com/prisma/tiberius/pull/58))

## Version 0.4.3

- Starting transactions with `simple_query` now works without crashing
  ([#55](https://github.com/prisma/tiberius/pull/55))

## Version 0.4.2

- Fixing old and wrong `ExecuteResult` docs
- Adding `rows_affected` method to `ExecuteResult`

## Version 0.4.1

- Add all feature flags for docs.rs build

## Version 0.4.0

- A complete rewrite from 0.3.0
- Not bound to Tokio anymore, independent of the runtime
- Support for many more types
- Async/await, futures 0.3
