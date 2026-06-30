use crate::MigrationError;
use crate::declared_types::index::DeclaredIndex;
use crate::declared_types::table::DeclaredTable;
use crate::sqlparser_helpers::{object_names_equal, quote_object_name};
use sqlparser::ast::{CreateExtension, CreateIndex, CreateTable};
use sqlparser::ast::{ObjectName, ObjectNamePart, Statement};
use std::fmt::Display;

pub fn from_to(froms: Vec<Wrapped>, tos: Vec<Wrapped>) -> Result<Vec<Statement>, MigrationError> {
    let mut r: Vec<Statement> = Vec::new();
    for wrapped_to in &tos {
        if let None = wrapped_to.name() {
            return Err(MigrationError::UnnamedObject(wrapped_to.clone()));
        }
        let matched_from = froms.iter().find(|f| f.name_and_type_equals(wrapped_to));
        wrapped_to.statement_from(matched_from, &mut r)?;
    }

    for from in &froms {
        if let None = tos.iter().find(|f| f.name_and_type_equals(from)) {
            from.create_statements(&mut r)?
        }
    }

    Ok(r)
}

#[derive(Clone, Debug)]
pub enum Wrapped {
    CreateTable(DeclaredTable),
    CreateIndex(DeclaredIndex),
    CreateExtension {
        name: sqlparser::ast::Ident,
    },
    CreateSchema {
        schema_name: sqlparser::ast::SchemaName,
        if_not_exists: bool,
        with: Option<Vec<sqlparser::ast::SqlOption>>,
        options: Option<Vec<sqlparser::ast::SqlOption>>,
        default_collate_spec: Option<sqlparser::ast::Expr>,
        clone: Option<ObjectName>,
    },
}

impl Display for Wrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Wrapped::CreateTable(wct) => wct.fmt(f),
            Wrapped::CreateIndex(wci) => wci.fmt(f),
            Wrapped::CreateExtension { name } => {
                sqlparser::ast::Statement::CreateExtension(CreateExtension {
                    name: name.to_owned(),
                    if_not_exists: false,
                    cascade: false,
                    schema: None,
                    version: None,
                })
                .fmt(f)
            }
            Wrapped::CreateSchema {
                schema_name,
                if_not_exists,
                with,
                options,
                default_collate_spec,
                clone,
            } => sqlparser::ast::Statement::CreateSchema {
                schema_name: schema_name.to_owned(),
                if_not_exists: if_not_exists.to_owned(),
                with: with.to_owned(),
                options: options.to_owned(),
                default_collate_spec: default_collate_spec.to_owned(),
                clone: clone.to_owned(),
            }
            .fmt(f),
        }
    }
}

impl Wrapped {
    fn name_and_type_equals(&self, other: &Wrapped) -> bool {
        // Unnamed items shouldn't match
        if let (None, None) = (self.name(), other.name()) {
            return false;
        }
        match (self, other) {
            (Self::CreateTable(ct), Self::CreateTable(ot)) => {
                object_names_equal(&ct.name, &ot.name)
            }
            (Self::CreateIndex(ci), Self::CreateIndex(oi)) => match (&ci.name, &oi.name) {
                (Some(a), Some(b)) => object_names_equal(a, b),
                (None, None) => false,
                _ => false,
            },
            (Self::CreateExtension { name }, Self::CreateExtension { name: other_name }) => {
                name == other_name
            }
            (
                Self::CreateSchema { schema_name, .. },
                Self::CreateSchema {
                    schema_name: other_schema_name,
                    ..
                },
            ) => schema_name == other_schema_name,
            (_, _) => false,
        }
    }

    fn name(&self) -> Option<ObjectName> {
        match self {
            Wrapped::CreateTable(wct) => Some(wct.name.clone()),
            Wrapped::CreateIndex(wci) => wci.name.clone(),
            Wrapped::CreateExtension { name } => {
                Some(ObjectName(vec![ObjectNamePart::Identifier(name.clone())]))
            }
            Wrapped::CreateSchema { schema_name, .. } => match schema_name {
                sqlparser::ast::SchemaName::Simple(obj_name) => Some(obj_name.clone().into()),
                _ => None,
            },
        }
    }

    pub fn try_from(s: Statement) -> anyhow::Result<Wrapped, MigrationError> {
        match s {
            Statement::CreateTable(ct) => Ok(Wrapped::CreateTable(DeclaredTable::new(ct))),
            Statement::CreateIndex(ci) => Ok(Wrapped::CreateIndex(DeclaredIndex::new(ci))),
            Statement::CreateExtension(CreateExtension { name, .. }) => {
                Ok(Wrapped::CreateExtension { name })
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                with,
                options,
                default_collate_spec,
                clone,
            } => Ok(Wrapped::CreateSchema {
                schema_name,
                if_not_exists,
                with,
                options,
                default_collate_spec,
                clone,
            }),

            statement => Err(MigrationError::UnsupportedStatementType(statement)),
        }
    }

    /// Create migration statements to with this object as the known current state.
    fn statement_from(
        &self,
        matched_from: Option<&Wrapped>,
        mut r: &mut Vec<Statement>,
    ) -> anyhow::Result<(), MigrationError> {
        match (self, matched_from) {
            (Wrapped::CreateTable(to_table), Some(Wrapped::CreateTable(from_table))) => {
                to_table.statement_from(from_table, &mut r)?
            }
            (Wrapped::CreateTable(to_table), None) => to_table.create(&mut r)?,
            (Wrapped::CreateIndex(to_index), Some(Wrapped::CreateIndex(from_index))) => {
                if from_index != to_index {
                    return Err(MigrationError::CannotModifyIndex(
                        from_index.clone(),
                        to_index.clone(),
                    ));
                }
            }
            (Wrapped::CreateIndex(to_index), None) => {
                to_index.create(&mut r)?;
            }
            (Wrapped::CreateExtension { name }, _) => {
                if let None = matched_from {
                    r.push(Statement::CreateExtension(CreateExtension {
                        name: name.to_owned(),
                        cascade: false,
                        if_not_exists: false,
                        schema: None,
                        version: None,
                    }))
                }
            }
            (
                Wrapped::CreateSchema {
                    schema_name,
                    if_not_exists,
                    with,
                    options,
                    default_collate_spec,
                    clone,
                },
                _,
            ) => {
                if let None = matched_from {
                    r.push(Statement::CreateSchema {
                        schema_name: schema_name.to_owned(),
                        if_not_exists: if_not_exists.to_owned(),
                        with: with.to_owned(),
                        options: options.to_owned(),
                        default_collate_spec: default_collate_spec.to_owned(),
                        clone: clone.to_owned(),
                    })
                }
            }
            (_, _) => panic!("Unhandled case"),
        }
        Ok(())
    }

    /// Create migration statements to with this object as the known current state.
    fn create_statements(&self, r: &mut Vec<Statement>) -> anyhow::Result<(), MigrationError> {
        match self {
            Wrapped::CreateTable(ct) => {
                let quoted_name = quote_object_name(&ct.name);
                r.push(Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    table: None,
                    if_exists: false,
                    names: vec![quoted_name],
                    cascade: true,
                    purge: false,
                    restrict: false,
                    temporary: false,
                })
            }

            Wrapped::CreateIndex(ci) => {
                if let Some(name) = ci.name.clone() {
                    r.push(Statement::Drop {
                        object_type: sqlparser::ast::ObjectType::Index,
                        table: None,
                        if_exists: false,
                        names: vec![name],
                        cascade: true,
                        purge: false,
                        restrict: false,
                        temporary: false,
                    })
                }
            }
            // Extensions won't be removed
            Wrapped::CreateExtension { .. } => (),
            // Schemas wont be dropped
            Wrapped::CreateSchema { .. } => (),
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_str_to_str {
    use super::*;

    #[test]
    fn test_add_table() {
        let empty = vec![];
        let target = vec![str_to_wrapped(r#"CREATE TABLE "test" (id uuid)"#)];

        let r = from_to(empty, target).expect("works");

        let alter = vec![str_to_statement(r#"CREATE TABLE "test" (id uuid)"#)];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_drop_table() {
        let target = vec![];
        let start = vec![str_to_wrapped(r#"CREATE TABLE "test" (id uuid)"#)];

        let r = from_to(start, target).expect("works");

        let alter = vec![str_to_statement(r#"DROP TABLE "test" CASCADE"#)];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_add_index() {
        let start = vec![];
        let target = vec![str_to_wrapped(r#"CREATE INDEX idx_id on test (id)"#)];

        let r = from_to(start, target).expect("works");

        let alter = vec![str_to_statement(r#"CREATE INDEX idx_id on test (id)"#)];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_add_index_compare() {
        let named_index = str_to_wrapped(r#"CREATE INDEX idx_id on test (id)"#);
        let unnamed_index = str_to_wrapped(r#"CREATE INDEX on test (id)"#);

        // One name and one missing name shouldn't match
        let matched = Wrapped::name_and_type_equals(&named_index, &unnamed_index);
        assert!(!matched);

        // Unnamed items shouldn't match
        let matched = Wrapped::name_and_type_equals(&unnamed_index, &unnamed_index);
        assert!(!matched);
    }

    #[test]
    fn test_create_extension() {
        let start = vec![];
        let target = vec![str_to_wrapped(r#"CREATE EXTENSION ltree"#)];

        let r = from_to(start, target).expect("works");

        let alter = vec![str_to_statement(r#"CREATE EXTENSION ltree"#)];

        assert_eq!(r, alter);
    }

    fn str_to_wrapped(s: &str) -> Wrapped {
        let ast = str_to_statement(s);
        match ast {
            Statement::CreateTable(ct) => Wrapped::CreateTable(DeclaredTable::new(ct)),
            Statement::CreateIndex(ci) => Wrapped::CreateIndex(DeclaredIndex::new(ci)),
            Statement::CreateExtension(CreateExtension { name, .. }) => {
                Wrapped::CreateExtension { name }
            }
            _ => panic!("Expected a CREATE TABLE statement"),
        }
    }

    fn str_to_create_table(s: &str) -> CreateTable {
        let ast = str_to_statement(s);
        match ast {
            Statement::CreateTable(ct) => ct,
            _ => panic!("Expected a CREATE TABLE statement"),
        }
    }

    fn str_to_statement(s: &str) -> Statement {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(s).expect("SQL");
        parser.parse_statement().expect("Not valid sql")
    }
}

#[cfg(test)]
mod test_str_to_pg {
    use super::*;
    use sqlx::postgres::PgPool;

    #[sqlx::test]
    fn test_add_column(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test ()"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ADD COLUMN id UUID"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_column_to_separate_schemas(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE SCHEMA schema1 "#, &pool)
            .await
            .expect("Setup schema");
        crate::migrate_from_string(r#"CREATE SCHEMA schema2 "#, &pool)
            .await
            .expect("Setup schema");
        crate::migrate_from_string(r#"CREATE TABLE schema1.test ()"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE schema2.test ()"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"CREATE TABLE schema2.test ()"#];
        assert_eq!(m, alter);
    }
    #[sqlx::test]
    fn test_add_column_to_default_schemas(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE public.test ()"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ADD COLUMN id UUID"#];

        assert_eq!(m, alter);
    }
    #[sqlx::test]
    fn test_no_add_column_to_named_schema_table(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE SCHEMA schema1 "#, &pool)
            .await
            .expect("Setup schema");
        crate::migrate_from_string(r#"CREATE TABLE schema1.test ()"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string_for_schema(
            "schema1",
            r#"CREATE TABLE test ()"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter: Vec<String> = vec![];
        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_noop_for_schema_table(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE SCHEMA schema1 "#, &pool)
            .await
            .expect("Setup schema");
        crate::migrate_schema_from_string("schema1", r#"CREATE TABLE test ()"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string_for_schema(
            "schema1",
            r#"CREATE TABLE test ()"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter: Vec<String> = vec![];
        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_remove_column(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test ()"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test DROP COLUMN id CASCADE"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_alter_column_not_null(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (id uuid NOT NULL)"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ALTER COLUMN id SET NOT NULL"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_alter_column_drop_not_null(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid NOT NULL)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ALTER COLUMN id DROP NOT NULL"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_alter_column_set_default(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (name varchar)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (name varchar DEFAULT 'foo')"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ALTER COLUMN name SET DEFAULT 'foo'"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_alter_column_set_new_default(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (name varchar DEFAULT 'foo')"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (name varchar DEFAULT 'bar')"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ALTER COLUMN name SET DEFAULT 'bar'"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_alter_column_drop_default(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (name varchar DEFAULT 'foo')"#, &pool)
            .await
            .expect("Setup");
        let m =
            crate::generate_migrations_from_string(r#"CREATE TABLE test (name varchar )"#, &pool)
                .await
                .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ALTER COLUMN name DROP DEFAULT"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_table(pool: PgPool) {
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"CREATE TABLE test (id UUID)"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_table_with_quotes(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE "test" (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE "test" (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter: Vec<String> = vec![];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_table_removing_quotes(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE "test" (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter: Vec<String> = vec![];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_table_adding_quotes(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE "test" (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter: Vec<String> = vec![];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_table_reserved_word(pool: PgPool) {
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE "user" (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"CREATE TABLE "user" (id UUID)"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_drop_table(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#""#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"DROP TABLE "test" CASCADE"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_drop_table_reserved_word(pool: PgPool) {
        let m = crate::migrate_from_string(r#"CREATE TABLE "user" ()"#, &pool)
            .await
            .expect("Migrate");

        let m = crate::generate_migrations_from_string(r#""#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"DROP TABLE "user" CASCADE"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_foreign_key_constraint(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id))"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter =
            vec![r#"ALTER TABLE test ADD CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES items(id)"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_same_foreign_key_constraint(pool: PgPool) {
        crate::migrate_from_string(
            r#"
                CREATE TABLE items (id uuid NOT NULL, PRIMARY KEY(id));
                CREATE TABLE test (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id))"#,
            &pool,
        )
        .await
        .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"
                CREATE TABLE items (id uuid NOT NULL, PRIMARY KEY(id));
                CREATE TABLE test (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id))"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter: Vec<String> = vec![];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_modify_foreign_key_constraint(pool: PgPool) {
        crate::migrate_from_string(
            r#"
                CREATE TABLE items (
                    id uuid NOT NULL,
                    id2 uuid NOT NULL,
                    PRIMARY KEY(id)
                );
                CREATE TABLE test (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id))"#,
            &pool,
        )
        .await
        .expect("Setup");
        let maybe_err = crate::generate_migrations_from_string(
            r#"
                CREATE TABLE items (
                    id uuid NOT NULL,
                    id2 uuid NOT NULL,
                    PRIMARY KEY(id)
                );
                CREATE TABLE test (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id2))"#,
            &pool,
        )
        .await;

        match maybe_err {
            Err(MigrationError::CannotModifyTableConstraint(_, _)) => (),
            _ => panic!("Not the right error {maybe_err:?}"),
        }
    }

    #[sqlx::test]
    fn test_same_check_constraint(pool: PgPool) {
        crate::migrate_from_string(
            r#"CREATE TABLE test (id int, CONSTRAINT check_id CHECK (id = 1))"#,
            &pool,
        )
        .await
        .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (id int, CONSTRAINT check_id CHECK (id = 1))"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter: Vec<&str> = vec![];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_modify_check_constraint(pool: PgPool) {
        crate::migrate_from_string(
            r#"CREATE TABLE test (id int, CONSTRAINT check_id CHECK (id = 1))"#,
            &pool,
        )
        .await
        .expect("Setup");
        let maybe_err = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (id int, CONSTRAINT check_id CHECK (id = 2))"#,
            &pool,
        )
        .await;

        match maybe_err {
            Err(MigrationError::CannotModifyTableConstraint(_, _)) => (),
            _ => panic!("Not the right error {maybe_err:?}"),
        }
    }

    #[sqlx::test]
    fn test_add_index(pool: PgPool) {
        let m =
            crate::generate_migrations_from_string(r#"CREATE INDEX idx_id on test (id)"#, &pool)
                .await
                .expect("Migrate");

        let alter = vec![r#"CREATE INDEX idx_id ON test(id)"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_index_without_name(pool: PgPool) {
        crate::migrate_from_string(
            r#"
                CREATE TABLE test (id uuid NOT NULL);
                CREATE INDEX idx_id on test (id);

            "#,
            &pool,
        )
        .await
        .expect("Setup");
        let maybe_err = crate::generate_migrations_from_string(
            r#"
                    CREATE TABLE test (id uuid);
                    CREATE INDEX on public.test (id)
            "#,
            &pool,
        )
        .await;

        match maybe_err {
            Err(MigrationError::UnnamedObject(_w)) => (),
            _ => panic!("Not the right error {maybe_err:?}"),
        }
    }

    #[sqlx::test]
    fn test_unchanged_index(pool: PgPool) {
        crate::migrate_from_string(
            r#"
                CREATE TABLE test (id uuid);
                CREATE INDEX idx_id on test (id);

            "#,
            &pool,
        )
        .await
        .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"
                CREATE TABLE test (id uuid);
                CREATE INDEX idx_id on public.test USING btree (id);
            "#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter: Vec<String> = vec![];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_unchanged_unique_index(pool: PgPool) {
        crate::migrate_from_string(
            r#"
                CREATE TABLE test (id uuid, name text);
                CREATE UNIQUE INDEX idx_id on test (id, name);

            "#,
            &pool,
        )
        .await
        .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"
                CREATE TABLE test (id uuid, name text);
                CREATE UNIQUE INDEX idx_id on public.test USING btree (id, name);
            "#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter: Vec<String> = vec![];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_changed_index(pool: PgPool) {
        crate::migrate_from_string(
            r#"
                CREATE TABLE test (id uuid);
                CREATE INDEX idx_id on public.test USING BTREE (id ASC);

            "#,
            &pool,
        )
        .await
        .expect("Setup");
        let maybe_err = crate::generate_migrations_from_string(
            r#"
                CREATE TABLE test (id uuid);
                CREATE INDEX idx_id on public.test USING BTREE (id DESC);
            "#,
            &pool,
        )
        .await;

        match maybe_err {
            Err(MigrationError::CannotModifyIndex(_, _)) => (),
            _ => panic!("Not the right error {maybe_err:?}"),
        }
    }

    #[sqlx::test]
    fn test_create_extension(pool: PgPool) {
        let m = crate::generate_migrations_from_string(r#"CREATE EXTENSION ltree;"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"CREATE EXTENSION ltree"#];

        assert_eq!(m, alter);
    }

    fn str_to_wrapped(s: &str) -> Wrapped {
        let ast = str_to_statement(s);
        match ast {
            Statement::CreateTable(ct) => Wrapped::CreateTable(DeclaredTable::new(ct)),
            Statement::CreateIndex(ci) => Wrapped::CreateIndex(DeclaredIndex::new(ci)),
            Statement::CreateExtension(CreateExtension { name, .. }) => {
                Wrapped::CreateExtension { name }
            }
            _ => panic!("Expected a CREATE TABLE statement"),
        }
    }

    fn str_to_statement(s: &str) -> Statement {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let parser = sqlparser::parser::Parser::new(&dialect);
        let mut parser = parser.try_with_sql(s).expect("SQL");
        parser.parse_statement().expect("Not valid sql")
    }
}
