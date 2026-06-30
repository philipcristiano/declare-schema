use std::fmt::Display;
use std::ops::Deref;

use crate::MigrationError;

use crate::sqlparser_helpers::{object_names_equal, quote_object_name};
use sqlparser::ast::CreateIndex;
use sqlparser::ast::Statement;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredIndex(CreateIndex);

impl Deref for DeclaredIndex {
    type Target = CreateIndex;
    fn deref(&self) -> &CreateIndex {
        &self.0
    }
}

impl Display for DeclaredIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        sqlparser::ast::Statement::CreateIndex(self.0.to_owned()).fmt(f)
    }
}

impl DeclaredIndex {
    pub fn new(ct: CreateIndex) -> Self {
        DeclaredIndex(ct)
    }

    pub fn create(&self, r: &mut Vec<Statement>) -> anyhow::Result<(), MigrationError> {
        r.push(Statement::CreateIndex(self.0.clone()));
        Ok(())
    }
}
