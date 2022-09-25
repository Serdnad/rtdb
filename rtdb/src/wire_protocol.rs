// TODO: a lot of this code, particularly the parsing, will very easily cause panics if
//  the input is not perfect.

pub mod query;
pub mod insert;


#[derive(Debug, Eq, PartialEq)]
#[repr(u8)]
enum DataType {
    Float = 0,
    Bool = 1,
}

impl std::convert::TryFrom<u8> for DataType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DataType::Float),
            1 => Ok(DataType::Bool),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Field {
    pub name: String,
    pub data_type: DataType,
}
