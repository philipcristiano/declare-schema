use sqlparser::ast::CreateTable;
use sqlparser::ast::{AlterTableOperation, ObjectName, Statement, TableConstraint};

pub fn from_to_table(f: &CreateTable, t: &CreateTable) -> anyhow::Result<Vec<Statement>> {
    if f.name != t.name {
        return Err(anyhow::anyhow!("Not the same table"));
    }

    let mut r = Vec::new();
    let mut column_statements = compare_columns(&f.name, &f.columns, &t.columns)?;
    let mut constraint_statements = compare_contraints(&f.name, &f.constraints, &t.constraints)?;

    r.append(&mut column_statements);
    r.append(&mut constraint_statements);
    Ok(r)
}

pub fn from_to(froms: Vec<Wrapped>, tos: Vec<Wrapped>) -> anyhow::Result<Vec<Statement>> {
    let mut r: Vec<Statement> = Vec::new();
    for wrapped_to in &tos {
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
            _ => {}
        }
    }

    for from in &froms {
        if let None = tos.iter().find(|f| f.name() == from.name()) {
            match from {
                Wrapped::CreateTable(ct) => r.push(Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    if_exists: false,
                    names: vec![ct.name.clone()],
                    cascade: true,
                    purge: false,
                    restrict: false,
                    temporary: false,
                }),
            }
        }
    }

    Ok(r)
}

fn compare_columns(
    table_name: &ObjectName,
    f: &Vec<sqlparser::ast::ColumnDef>,
    t: &Vec<sqlparser::ast::ColumnDef>,
) -> anyhow::Result<Vec<Statement>> {
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
                operations: vec![AlterTableOperation::DropColumn {
                    column_name: f_column.name.clone(),
                    if_exists: false,
                    cascade: true,
                }],
            });
        }
    }
    for t_column in t {
        eprintln!("find column {}", t_column);
        let maybe_f_column = f.iter().find(|fi| fi.name == t_column.name);
        if let Some(f_column) = maybe_f_column {
            eprintln!("matching column {}", f_column)
        } else {
            r.push(Statement::AlterTable {
                name: table_name.clone(),
                if_exists: false,
                location: None,
                only: false,
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

fn compare_contraints(
    table_name: &ObjectName,
    f: &Vec<sqlparser::ast::TableConstraint>,
    t: &Vec<sqlparser::ast::TableConstraint>,
) -> anyhow::Result<Vec<Statement>> {
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
                if let Some(_fk) = maybe_fk {
                    eprintln!("Has fk already TODO: Check equal")
                } else {
                    r.push(Statement::AlterTable {
                        name: table_name.clone(),
                        if_exists: false,
                        location: None,
                        only: false,
                        operations: vec![AlterTableOperation::AddConstraint(
                            t_constraint.to_owned(),
                        )],
                    });
                }
            }
            _ => eprintln!("Contraints not supported"),
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
                        operations: vec![AlterTableOperation::DropConstraint {
                            if_exists: false,
                            cascade: true,
                            name: name.clone().unwrap(),
                        }],
                    });
                }
            }
            _ => eprintln!("Contraints not supported"),
        }
    }
    Ok(r)
}

pub enum Wrapped {
    CreateTable(CreateTable),
}

impl Wrapped {
    fn name_and_type_equals(&self, other: &Wrapped) -> bool {
        match self {
            Self::CreateTable(ct) => {
                if let Self::CreateTable(other_table) = other {
                    return ct.name == other_table.name;
                }
            }
        }
        return false;
    }

    fn name(&self) -> &ObjectName {
        match self {
            Wrapped::CreateTable(wct) => &wct.name,
        }
    }

    pub fn try_from(s: Statement) -> anyhow::Result<Wrapped> {
        match s {
            Statement::CreateTable(ct) => Ok(Wrapped::CreateTable(ct)),
            _ => Err(anyhow::anyhow!("Not a CreateTable")),
        }
    }
}

#[cfg(test)]
mod tests {
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
    fn test_add_table() {
        let empty = vec![];
        let target = vec![str_to_wrapped_table(r#"CREATE TABLE "test" (id uuid)"#)];

        let r = from_to(empty, target).expect("works");

        let alter = vec![str_to_statement(r#"CREATE TABLE "test" (id uuid)"#)];

        assert_eq!(r, alter);
    }

    #[test]
    fn test_drop_table() {
        let target = vec![];
        let start = vec![str_to_wrapped_table(r#"CREATE TABLE "test" (id uuid)"#)];

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

    //#[test]
    // fn test_add_index() {
    //     let start = vec![];
    //     let target = vec![str_to_statement(r#"CREATE INDEX idx_id on test (id)"#)];

    //     let r = from_to(&start, &target).expect("works");

    //     let alter = vec![str_to_statement(
    //         r#"ALTER TABLE "test" ADD CONSTRAINT fk_id FOREIGN KEY(id) REFERENCES items(id)"#,
    //     )];

    //     assert_eq!(r, alter);
    // }

    fn str_to_wrapped_table(s: &str) -> Wrapped {
        let ast = str_to_statement(s);
        match ast {
            Statement::CreateTable(ct) => Wrapped::CreateTable(ct),
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
