# GORM Tree

Independent Go 1.25+ module; Core has no GORM/SQL/driver dependency.

`Definition(f, Options)` publishes `database` (`Database`). Supply explicit
`Owned` or `Borrowed` ownership and an `Open(ctx)` callback returning native
`*gorm.DB`. Configure native dial timeouts and `DisableAutomaticPing`; Start
performs bounded PingContext. Open must clean partial errors. If it returns a
non-nil owned handle with an error, rollback Stop also closes that pool.
Borrowed pools are never closed; the host must outlive all users.

Declare your own consumer Port and use `Ref.Use` in a finite Scope. `WithDB`
preserves native GORM sessions/queries; consume/close Rows and finish transactions
inside its callback. Never retain native or derived handles after returning.
`Transaction(ctx, callback, ...*sql.TxOptions)` conservatively tags unsuccessful
Commit with `ErrCommitUnknown`; reconcile business ID before retrying. Callback
and begin failures are returned directly. Native `db.Transaction` is still
available but its raw errors require your own stage classification. No retries.

Pools are stable resources, not HotSwappable definitions. Replace repositories
or business providers while retaining the pool. Stop waits for scoped work then
closes only owned resources. SQL Close has no Context interrupt; Forest reports
Draining timeout without pretending release. Migrations are separate admin work,
never an implicit part of the topology transaction.

Versions: GORM v1.31.2, PostgreSQL driver v1.6.3. Tests use real PostgreSQL with
`FOREST_INTEGRATION=1` and `FOREST_POSTGRES_DSN`. An explicit integration run
fails on missing configuration. `python ../../scripts/test-ecosystem.py`
provisions isolated real services and tests all new modules.
