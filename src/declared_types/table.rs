use std::ops::Deref;

use crate::MigrationError;

use crate::sqlparser_helpers::{object_names_equal, quote_object_name};
use sqlparser::ast::table_constraints::{CheckConstraint, ForeignKeyConstraint, UniqueConstraint};
use sqlparser::ast::{AlterTable, CreateTable, DropBehavior};
use sqlparser::ast::{AlterTableOperation, ObjectName, Statement, TableConstraint};

#[derive(Clone, Debug)]
pub struct DeclaredTable(CreateTable);

impl Deref for DeclaredTable {
    type Target = CreateTable;
    fn deref(&self) -> &CreateTable {
        &self.0
    }
}

impl DeclaredTable {
    pub fn new(ct: CreateTable) -> Self {
        DeclaredTable(ct)
    }

    pub fn statement_from(
        &self,
        from: &Self,
        r: &mut Vec<Statement>,
    ) -> anyhow::Result<(), MigrationError> {
        let mut changes = from_to_table(&from, &self.0)?;
        r.append(&mut changes);
        Ok(())
    }

    pub fn create(&self, r: &mut Vec<Statement>) -> anyhow::Result<(), MigrationError> {
        r.push(Statement::CreateTable(self.0.clone()));
        Ok(())
    }
}

pub fn from_to_table(f: &CreateTable, t: &CreateTable) -> Result<Vec<Statement>, MigrationError> {
    if !object_names_equal(&f.name, &t.name) {
        return Err(MigrationError::TablesNotMatching(f.clone(), t.clone()));
    }

    let mut r = Vec::new();
    let mut column_statements = compare_columns(&f.name, &f.columns, &t.columns)?;
    let mut constraint_statements = compare_constraints(&f.name, &f.constraints, &t.constraints)?;

    r.append(&mut column_statements);
    r.append(&mut constraint_statements);
    Ok(r)
}

fn compare_columns(
    table_name: &ObjectName,
    f: &Vec<sqlparser::ast::ColumnDef>,
    t: &Vec<sqlparser::ast::ColumnDef>,
) -> Result<Vec<Statement>, MigrationError> {
    let mut r = Vec::new();
    for f_column in f.clone() {
        eprintln!("find column {}", f_column);
        let maybe_t_column = t.iter().find(|ti| ti.name == f_column.name);
        if let Some(t_column) = maybe_t_column {
            eprintln!("matching column {}", t_column)
        } else {
            r.push(Statement::AlterTable(AlterTable {
                name: table_name.clone(),
                if_exists: false,
                location: None,
                only: false,
                on_cluster: None,
                table_type: None,
                operations: vec![AlterTableOperation::DropColumn {
                    column_names: vec![f_column.name.clone()],
                    has_column_keyword: true,
                    if_exists: false,
                    drop_behavior: Some(DropBehavior::Cascade),
                }],
                end_token: semicolon_token(),
            }));
        }
    }
    for t_column in t {
        eprintln!("find column {}", t_column);
        let maybe_f_column = f.iter().find(|fi| fi.name == t_column.name);
        if let Some(f_column) = maybe_f_column {
            eprintln!("matching column {}", f_column);
            let mut column_statements = compare_column(&table_name, &f_column, &t_column)?;
            r.append(&mut column_statements);
        } else {
            r.push(Statement::AlterTable(AlterTable {
                name: table_name.clone(),
                if_exists: false,
                location: None,
                only: false,
                on_cluster: None,
                table_type: None,
                operations: vec![AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: false,
                    column_def: t_column.to_owned(),
                    column_position: None,
                }],
                end_token: semicolon_token(),
            }));
        }
    }
    Ok(r)
}

fn compare_column(
    table_name: &ObjectName,
    f: &sqlparser::ast::ColumnDef,
    t: &sqlparser::ast::ColumnDef,
) -> Result<Vec<Statement>, MigrationError> {
    let mut r = Vec::new();
    for to_opt in &t.options {
        match &to_opt.option {
            sqlparser::ast::ColumnOption::NotNull => {
                let from_not_null = &f
                    .options
                    .iter()
                    .find(|f_opt| matches!(f_opt.option, sqlparser::ast::ColumnOption::NotNull));
                if let None = from_not_null {
                    r.push(Statement::AlterTable(AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::AlterColumn {
                            column_name: t.name.clone(),
                            op: sqlparser::ast::AlterColumnOperation::SetNotNull,
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            sqlparser::ast::ColumnOption::Default(expr) => {
                let from_default = &f
                    .options
                    .iter()
                    .find(|f_opt| matches!(f_opt.option, sqlparser::ast::ColumnOption::Default(_)));
                // Create the alter statement but dont push it yet
                let alter = Statement::AlterTable(AlterTable {
                    name: table_name.clone(),
                    if_exists: false,
                    location: None,
                    only: false,
                    on_cluster: None,
                    table_type: None,
                    operations: vec![AlterTableOperation::AlterColumn {
                        column_name: t.name.clone(),
                        op: sqlparser::ast::AlterColumnOperation::SetDefault {
                            value: expr.to_owned(),
                        },
                    }],
                    end_token: semicolon_token(),
                });
                match from_default {
                    // There is no default previously, alter the table
                    None => r.push(alter),
                    Some(f_opt) => {
                        let to_opt_option = &to_opt.option;
                        // If the from and to options are different, alter the table
                        if f_opt.option != to_opt_option.clone() {
                            r.push(alter)
                        }
                    }
                }
                if let None = from_default {}
            }

            x => eprintln!("Column Option not supported yet {:?}", x),
        }
    }
    for f_opt in &f.options {
        match &f_opt.option {
            sqlparser::ast::ColumnOption::NotNull => {
                let to_not_null = &t
                    .options
                    .iter()
                    .find(|to_opt| matches!(to_opt.option, sqlparser::ast::ColumnOption::NotNull));
                if let None = to_not_null {
                    r.push(Statement::AlterTable(AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::AlterColumn {
                            column_name: t.name.clone(),
                            op: sqlparser::ast::AlterColumnOperation::DropNotNull,
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }

            sqlparser::ast::ColumnOption::Default(_) => {
                let to_default = &t.options.iter().find(|to_opt| {
                    matches!(to_opt.option, sqlparser::ast::ColumnOption::Default(_))
                });
                if let None = to_default {
                    r.push(Statement::AlterTable(AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::AlterColumn {
                            column_name: t.name.clone(),
                            op: sqlparser::ast::AlterColumnOperation::DropDefault,
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }

            x => eprintln!("Column Option not supported yet {:?}", x),
        }
    }
    Ok(r)
}

fn compare_constraints(
    table_name: &ObjectName,
    f: &Vec<sqlparser::ast::TableConstraint>,
    t: &Vec<sqlparser::ast::TableConstraint>,
) -> Result<Vec<Statement>, MigrationError> {
    let mut r = Vec::new();

    let maybe_f_pk = f
        .iter()
        .find(|fc| matches!(fc, TableConstraint::PrimaryKey { .. }));
    for t_constraint in t.clone() {
        match &t_constraint {
            TableConstraint::PrimaryKey { .. } => {
                if let Some(_f_pk) = maybe_f_pk {
                    eprintln!("Has pk already")
                } else {
                    eprintln!("Needs pk");

                    r.push(Statement::AlterTable(AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::AddConstraint {
                            constraint: t_constraint.to_owned(),
                            not_valid: false,
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            TableConstraint::ForeignKey(ForeignKeyConstraint { name, .. }) => {
                let to_name = name;
                let maybe_fk = f.iter().find(|fc| {
                    if let TableConstraint::ForeignKey(ForeignKeyConstraint { name, .. }) = fc {
                        to_name == name
                    } else {
                        false
                    }
                });
                if let Some(fk) = maybe_fk {
                    if fk != &t_constraint {
                        return Err(MigrationError::CannotModifyTableConstraint(
                            fk.clone(),
                            t_constraint.clone(),
                        ));
                    }
                } else {
                    r.push(Statement::AlterTable(AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::AddConstraint {
                            constraint: t_constraint.to_owned(),
                            not_valid: false,
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            TableConstraint::Unique(UniqueConstraint { name, .. }) => {
                let to_name = name;
                let maybe_uniq = f.iter().find(|uniq| {
                    if let TableConstraint::Unique(UniqueConstraint { name, .. }) = uniq {
                        to_name == name
                    } else {
                        false
                    }
                });
                if let Some(_uniq) = maybe_uniq {
                    eprintln!("Has Unique already TODO: Check equal")
                } else {
                    r.push(Statement::AlterTable(AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::AddConstraint {
                            constraint: t_constraint.to_owned(),
                            not_valid: false,
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            TableConstraint::Check(CheckConstraint { name, .. }) => {
                let to_name = name;
                let maybe_check = f.iter().find(|check| {
                    if let TableConstraint::Check(CheckConstraint { name, .. }) = check {
                        to_name == name
                    } else {
                        false
                    }
                });
                if let Some(fk) = maybe_check {
                    if fk != &t_constraint {
                        return Err(MigrationError::CannotModifyTableConstraint(
                            fk.clone(),
                            t_constraint.clone(),
                        ));
                    }
                } else {
                    r.push(Statement::AlterTable(AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::AddConstraint {
                            constraint: t_constraint.to_owned(),
                            not_valid: false,
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            x => eprintln!("Constraints not supported {:?}", x),
        }
    }
    for f_constraint in f {
        match &f_constraint {
            TableConstraint::ForeignKey(ForeignKeyConstraint { name, .. }) => {
                let from_name = name;
                let maybe_fk = &t.iter().find(|tc| {
                    if let TableConstraint::ForeignKey(ForeignKeyConstraint { name, .. }) = tc {
                        from_name == name
                    } else {
                        false
                    }
                });
                if let None = maybe_fk {
                    let quoted_name = quote_object_name(&table_name);
                    r.push(Statement::AlterTable(AlterTable {
                        name: quoted_name,
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::DropConstraint {
                            if_exists: false,
                            drop_behavior: Some(DropBehavior::Cascade),
                            name: name.clone().unwrap(),
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            TableConstraint::Unique(UniqueConstraint { name, .. }) => {
                let from_name = name;
                let maybe_uniq = &t.iter().find(|uniq| {
                    if let TableConstraint::Unique(UniqueConstraint { name, .. }) = uniq {
                        from_name == name
                    } else {
                        false
                    }
                });
                if let None = maybe_uniq {
                    let quoted_name = quote_object_name(&table_name);
                    r.push(Statement::AlterTable(AlterTable {
                        name: quoted_name,
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::DropConstraint {
                            if_exists: false,
                            drop_behavior: Some(DropBehavior::Cascade),
                            name: name.clone().unwrap(),
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            TableConstraint::Check(CheckConstraint { name, .. }) => {
                let from_name = name;
                let maybe_check = &t.iter().find(|check| {
                    if let TableConstraint::Check(CheckConstraint { name, .. }) = check {
                        from_name == name
                    } else {
                        false
                    }
                });
                if let None = maybe_check {
                    let quoted_name = quote_object_name(&table_name);
                    r.push(Statement::AlterTable(AlterTable {
                        name: quoted_name,
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        table_type: None,
                        operations: vec![AlterTableOperation::DropConstraint {
                            if_exists: false,
                            drop_behavior: Some(DropBehavior::Cascade),
                            name: name.clone().unwrap(),
                        }],
                        end_token: semicolon_token(),
                    }));
                }
            }
            TableConstraint::PrimaryKey { .. } => {}
            x => eprintln!("Contraints not supported, {:?}", x),
        }
    }
    Ok(r)
}

fn semicolon_token() -> sqlparser::ast::helpers::attached_token::AttachedToken {
    use sqlparser::ast::helpers::attached_token::AttachedToken;
    use sqlparser::tokenizer::{Location, Span, Token, TokenWithLocation};
    AttachedToken(TokenWithLocation::new(
        Token::SemiColon,
        Span::new(Location::new(1, 10), Location::new(1, 11)),
    ))
}

mod test_table_str_to_str {
    use super::*;

    #[test]
    fn test_add_column() {
        let empty_table = str_to_create_table(r#"CREATE TABLE "test" ()"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);

        let r = from_to_table(&empty_table, &target).expect("works");

        let alter = vec![str_to_statement(r#"ALTER TABLE "test" ADD COLUMN id uuid"#)];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_remove_column() {
        let start = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" ()"#);

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" DROP COLUMN id CASCADE"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_alter_column_not_null() {
        let empty_table = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (id uuid NOT NULL)"#);

        let r = from_to_table(&empty_table, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ALTER COLUMN id SET NOT NULL"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_alter_column_drop_not_null() {
        let empty_table = str_to_create_table(r#"CREATE TABLE "test" (id uuid NOT NULL)"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);

        let r = from_to_table(&empty_table, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ALTER COLUMN id DROP NOT NULL"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_alter_column_set_default() {
        let empty_table = str_to_create_table(r#"CREATE TABLE "test" (name varchar)"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (name varchar DEFAULT 'foo')"#);

        let r = from_to_table(&empty_table, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ALTER COLUMN name SET DEFAULT 'foo'"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_alter_column_set_new_default() {
        let empty_table =
            str_to_create_table(r#"CREATE TABLE "test" (name varchar DEFAULT 'foo')"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (name varchar DEFAULT 'bar')"#);

        let r = from_to_table(&empty_table, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ALTER COLUMN name SET DEFAULT 'bar'"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_alter_column_drop_default() {
        let empty_table =
            str_to_create_table(r#"CREATE TABLE "test" (name varchar DEFAULT 'foo')"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (name varchar )"#);

        let r = from_to_table(&empty_table, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ALTER COLUMN name DROP DEFAULT"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_add_primary_key_constraint() {
        let start = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (id uuid, PRIMARY KEY(id))"#);

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ADD PRIMARY KEY(id)"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_add_foreign_key_constraint() {
        let start = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);
        let target = str_to_create_table(
            r#"CREATE TABLE "test" (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id))"#,
        );

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ADD CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id)"#,
        )];

        assert_eq!(r, alter);
    }
    #[test]
    fn test_drop_foreign_key_constraint() {
        let start = str_to_create_table(
            r#"CREATE TABLE "test" (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id))"#,
        );
        let target = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" DROP CONSTRAINT fk_id CASCADE"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_drop_foreign_key_constraint_includes_quotes() {
        let start = str_to_create_table(
            r#"CREATE TABLE test (id uuid, CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id))"#,
        );
        let target = str_to_create_table(r#"CREATE TABLE test (id uuid)"#);

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" DROP CONSTRAINT fk_id CASCADE"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_add_check_constraint() {
        let start = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);
        let target = str_to_create_table(
            r#"CREATE TABLE "test" (id uuid, CONSTRAINT check_id CHECK (id = 1))"#,
        );

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ADD CONSTRAINT check_id CHECK (id = 1)"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_drop_check_constraint() {
        let start = str_to_create_table(
            r#"CREATE TABLE "test" (id uuid, CONSTRAINT check_id CHECK (id = 1))"#,
        );
        let target = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" DROP CONSTRAINT check_id CASCADE"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_add_unique_constraint() {
        let start = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);
        let target =
            str_to_create_table(r#"CREATE TABLE "test" (id uuid, CONSTRAINT id_u UNIQUE (id))"#);

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" ADD CONSTRAINT id_u UNIQUE (id)"#,
        )];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_drop_unique_constraint() {
        let start =
            str_to_create_table(r#"CREATE TABLE "test" (id uuid, CONSTRAINT id_u UNIQUE (id))"#);
        let target = str_to_create_table(r#"CREATE TABLE "test" (id uuid)"#);

        let r = from_to_table(&start, &target).expect("works");

        let alter = vec![str_to_statement(
            r#"ALTER TABLE "test" DROP CONSTRAINT id_u CASCADE"#,
        )];

        assert_eq!(r, alter);
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
