use sqlparser::ast::{AlterTableOperation, ObjectName, ObjectNamePart, Statement, TableConstraint};

pub fn quote_object_name(name: &ObjectName) -> ObjectName {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
    use sqlparser::tokenizer::Span;
    ObjectName(
        name.0
            .iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => ObjectNamePart::Identifier(Ident {
                    value: ident.value.clone(),
                    quote_style: Some('"'),
                    span: Span::empty(),
                }),
                ObjectNamePart::Function(f) => ObjectNamePart::Function(f.clone()),
            })
            .collect(),
    )
}

pub fn object_names_equal(a: &ObjectName, b: &ObjectName) -> bool {
    if a.0.len() != b.0.len() {
        return false;
    }
    a.0.iter().zip(b.0.iter()).all(|(a_part, b_part)| {
        match (a_part, b_part) {
            (ObjectNamePart::Identifier(a_ident), ObjectNamePart::Identifier(b_ident)) => {
                // Compare case-insensitively by value, ignoring quote_style
                a_ident.value.eq_ignore_ascii_case(&b_ident.value)
            }
            _ => a_part == b_part,
        }
    })
}
