use crate::MigrationError;
use sqlparser::ast::{AlterTableOperation, ObjectName, ObjectNamePart, Statement, TableConstraint};
use sqlparser::ast::{CreateIndex, CreateTable, DropBehavior};
use std::fmt::Display;

pub fn from_to_table(f: &CreateTable, t: &CreateTable) -> Result<Vec<Statement>, MigrationError> {
    if f.name != t.name {
        return Err(MigrationError::TablesNotMatching(f.clone(), t.clone()));
    }

    let mut r = Vec::new();
    let mut column_statements = compare_columns(&f.name, &f.columns, &t.columns)?;
    let mut constraint_statements = compare_constraints(&f.name, &f.constraints, &t.constraints)?;

    r.append(&mut column_statements);
    r.append(&mut constraint_statements);
    Ok(r)
}

pub fn from_to(froms: Vec<Wrapped>, tos: Vec<Wrapped>) -> Result<Vec<Statement>, MigrationError> {
    let mut r: Vec<Statement> = Vec::new();
    for wrapped_to in &tos {
        if let None = wrapped_to.name() {
            return Err(MigrationError::UnnamedObject(wrapped_to.clone()));
        }
        let matched_from = froms.iter().find(|f| f.name_and_type_equals(wrapped_to));
        match wrapped_to {
            Wrapped::CreateTable(to_table) => {
                if let Some(Wrapped::CreateTable(from)) = matched_from {
                    let mut changes = from_to_table(&from, &to_table)?;
                    r.append(&mut changes);
                } else {
                    r.push(Statement::CreateTable(to_table.clone()));
                }
            }
            Wrapped::CreateIndex(to_index) => {
                if let Some(Wrapped::CreateIndex(from)) = matched_from {
                    if from != to_index {
                        return Err(MigrationError::CannotModifyIndex(
                            from.clone(),
                            to_index.clone(),
                        ));
                    }
                    eprintln!("TODO: Existing index matched, should check for changes");
                } else {
                    r.push(Statement::CreateIndex(to_index.clone()));
                }
            }
            Wrapped::CreateExtension { name } => {
                if let None = matched_from {
                    r.push(Statement::CreateExtension {
                        name: name.to_owned(),
                        cascade: false,
                        if_not_exists: false,
                        schema: None,
                        version: None,
                    })
                }
            }
        }
    }

    for from in &froms {
        if let None = tos.iter().find(|f| f.name() == from.name()) {
            match from {
                Wrapped::CreateTable(ct) => r.push(Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    table: None,
                    if_exists: false,
                    names: vec![ct.name.clone()],
                    cascade: true,
                    purge: false,
                    restrict: false,
                    temporary: false,
                }),

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
                Wrapped::CreateExtension { .. } => (),
            }
        }
    }

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
            r.push(Statement::AlterTable {
                name: table_name.clone(),
                if_exists: false,
                location: None,
                only: false,
                on_cluster: None,
                iceberg: false,
                operations: vec![AlterTableOperation::DropColumn {
                    column_name: f_column.name.clone(),
                    has_column_keyword: true,
                    if_exists: false,
                    drop_behavior: Some(DropBehavior::Cascade),
                }],
            });
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
            r.push(Statement::AlterTable {
                name: table_name.clone(),
                if_exists: false,
                location: None,
                only: false,
                on_cluster: None,
                iceberg: false,
                operations: vec![AlterTableOperation::AddColumn {
                    column_keyword: true,
                    if_not_exists: false,
                    column_def: t_column.to_owned(),
                    column_position: None,
                }],
            });
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
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::AlterColumn {
                            column_name: t.name.clone(),
                            op: sqlparser::ast::AlterColumnOperation::SetNotNull,
                        }],
                    });
                }
            }
            sqlparser::ast::ColumnOption::Default(expr) => {
                let from_default = &f
                    .options
                    .iter()
                    .find(|f_opt| matches!(f_opt.option, sqlparser::ast::ColumnOption::Default(_)));
                // Create the alter statement but dont push it yet
                let alter = Statement::AlterTable {
                    name: table_name.clone(),
                    if_exists: false,
                    location: None,
                    only: false,
                    on_cluster: None,
                    iceberg: false,
                    operations: vec![AlterTableOperation::AlterColumn {
                        column_name: t.name.clone(),
                        op: sqlparser::ast::AlterColumnOperation::SetDefault {
                            value: expr.to_owned(),
                        },
                    }],
                };
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
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::AlterColumn {
                            column_name: t.name.clone(),
                            op: sqlparser::ast::AlterColumnOperation::DropNotNull,
                        }],
                    });
                }
            }

            sqlparser::ast::ColumnOption::Default(_) => {
                let to_default = &t.options.iter().find(|to_opt| {
                    matches!(to_opt.option, sqlparser::ast::ColumnOption::Default(_))
                });
                if let None = to_default {
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::AlterColumn {
                            column_name: t.name.clone(),
                            op: sqlparser::ast::AlterColumnOperation::DropDefault,
                        }],
                    });
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

                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::AddConstraint(
                            t_constraint.to_owned(),
                        )],
                    });
                }
            }
            TableConstraint::ForeignKey { name, .. } => {
                let to_name = name;
                let maybe_fk = f.iter().find(|fc| {
                    if let TableConstraint::ForeignKey { name, .. } = fc {
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
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::AddConstraint(
                            t_constraint.to_owned(),
                        )],
                    });
                }
            }
            TableConstraint::Unique { name, .. } => {
                let to_name = name;
                let maybe_uniq = f.iter().find(|uniq| {
                    if let TableConstraint::Unique { name, .. } = uniq {
                        to_name == name
                    } else {
                        false
                    }
                });
                if let Some(_uniq) = maybe_uniq {
                    eprintln!("Has Unique already TODO: Check equal")
                } else {
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::AddConstraint(
                            t_constraint.to_owned(),
                        )],
                    });
                }
            }
            TableConstraint::Check { name, .. } => {
                let to_name = name;
                let maybe_check = f.iter().find(|check| {
                    if let TableConstraint::Check { name, .. } = check {
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
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::AddConstraint(
                            t_constraint.to_owned(),
                        )],
                    });
                }
            }
            x => eprintln!("Constraints not supported {:?}", x),
        }
    }
    for f_constraint in f {
        match &f_constraint {
            TableConstraint::ForeignKey { name, .. } => {
                let from_name = name;
                let maybe_fk = &t.iter().find(|tc| {
                    if let TableConstraint::ForeignKey { name, .. } = tc {
                        from_name == name
                    } else {
                        false
                    }
                });
                if let None = maybe_fk {
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::DropConstraint {
                            if_exists: false,
                            drop_behavior: Some(DropBehavior::Cascade),
                            name: name.clone().unwrap(),
                        }],
                    });
                }
            }
            TableConstraint::Unique { name, .. } => {
                let from_name = name;
                let maybe_uniq = &t.iter().find(|uniq| {
                    if let TableConstraint::Unique { name, .. } = uniq {
                        from_name == name
                    } else {
                        false
                    }
                });
                if let None = maybe_uniq {
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::DropConstraint {
                            if_exists: false,
                            drop_behavior: Some(DropBehavior::Cascade),
                            name: name.clone().unwrap(),
                        }],
                    });
                }
            }
            TableConstraint::Check { name, .. } => {
                let from_name = name;
                let maybe_check = &t.iter().find(|check| {
                    if let TableConstraint::Check { name, .. } = check {
                        from_name == name
                    } else {
                        false
                    }
                });
                if let None = maybe_check {
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        on_cluster: None,
                        iceberg: false,
                        operations: vec![AlterTableOperation::DropConstraint {
                            if_exists: false,
                            drop_behavior: Some(DropBehavior::Cascade),
                            name: name.clone().unwrap(),
                        }],
                    });
                }
            }
            TableConstraint::PrimaryKey { .. } => {}
            x => eprintln!("Contraints not supported, {:?}", x),
        }
    }
    Ok(r)
}

#[derive(Clone, Debug)]
pub enum Wrapped {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    CreateExtension { name: sqlparser::ast::Ident },
}

impl Display for Wrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Wrapped::CreateTable(wct) => wct.fmt(f),
            Wrapped::CreateIndex(wci) => {
                sqlparser::ast::Statement::CreateIndex(wci.to_owned()).fmt(f)
            }
            Wrapped::CreateExtension { name } => sqlparser::ast::Statement::CreateExtension {
                name: name.to_owned(),
                if_not_exists: false,
                cascade: false,
                schema: None,
                version: None,
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
        match self {
            Self::CreateTable(ct) => {
                if let Self::CreateTable(other_table) = other {
                    return ct.name == other_table.name;
                }
            }
            Self::CreateIndex(ci) => {
                if let Self::CreateIndex(other_index) = other {
                    return ci.name == other_index.name;
                }
            }
            Self::CreateExtension { name } => {
                let name1 = name;
                if let Self::CreateExtension { name } = other {
                    return name1 == name;
                }
            }
        }
        return false;
    }

    fn name(&self) -> Option<ObjectName> {
        match self {
            Wrapped::CreateTable(wct) => Some(wct.name.clone()),
            Wrapped::CreateIndex(wci) => wci.name.clone(),
            Wrapped::CreateExtension { name } => {
                Some(ObjectName(vec![ObjectNamePart::Identifier(name.clone())]))
            }
        }
    }

    pub fn try_from(s: Statement) -> anyhow::Result<Wrapped, MigrationError> {
        match s {
            Statement::CreateTable(ct) => Ok(Wrapped::CreateTable(ct)),
            Statement::CreateIndex(ci) => Ok(Wrapped::CreateIndex(ci)),
            Statement::CreateExtension { name, .. } => Ok(Wrapped::CreateExtension { name }),

            statement => Err(MigrationError::UnsupportedStatementType(statement)),
        }
    }
}

#[cfg(test)]
mod test_str_to_str {
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
            Statement::CreateTable(ct) => Wrapped::CreateTable(ct),
            Statement::CreateIndex(ci) => Wrapped::CreateIndex(ci),
            Statement::CreateExtension { name, .. } => Wrapped::CreateExtension { name },
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
    fn test_drop_table(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(r#""#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"DROP TABLE test CASCADE"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_add_primary_key_constraint(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (id uuid, PRIMARY KEY(id))"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ADD PRIMARY KEY (id)"#];

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
    fn test_drop_foreign_key_constraint(pool: PgPool) {
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
               CREATE TABLE test (id uuid)"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test DROP CONSTRAINT fk_id CASCADE"#];

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
    fn test_add_check_constraint(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (id uuid, CONSTRAINT check_id CHECK (id = 1))"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ADD CONSTRAINT check_id CHECK (id = 1)"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_drop_check_constraint(pool: PgPool) {
        crate::migrate_from_string(
            r#"CREATE TABLE test (id int, CONSTRAINT check_id CHECK (id = 1))"#,
            &pool,
        )
        .await
        .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test (id int)"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test DROP CONSTRAINT check_id CASCADE"#];

        assert_eq!(m, alter);
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
    fn test_add_unique_constraint(pool: PgPool) {
        crate::migrate_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Setup");
        let m = crate::generate_migrations_from_string(
            r#"CREATE TABLE test (id uuid, CONSTRAINT id_u UNIQUE (id))"#,
            &pool,
        )
        .await
        .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test ADD CONSTRAINT id_u UNIQUE (id)"#];

        assert_eq!(m, alter);
    }

    #[sqlx::test]
    fn test_drop_unique_constraint(pool: PgPool) {
        crate::migrate_from_string(
            r#"CREATE TABLE test (id uuid, CONSTRAINT id_u UNIQUE (id))"#,
            &pool,
        )
        .await
        .expect("Setup");
        let m = crate::generate_migrations_from_string(r#"CREATE TABLE test (id uuid)"#, &pool)
            .await
            .expect("Migrate");

        let alter = vec![r#"ALTER TABLE test DROP CONSTRAINT id_u CASCADE"#];

        assert_eq!(m, alter);
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
            Err(MigrationError::UnnamedObject(w)) => (),
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
            Statement::CreateTable(ct) => Wrapped::CreateTable(ct),
            Statement::CreateIndex(ci) => Wrapped::CreateIndex(ci),
            Statement::CreateExtension { name, .. } => Wrapped::CreateExtension { name },
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
