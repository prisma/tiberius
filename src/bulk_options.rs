use bitflags::bitflags;

bitflags! {
    /// Options for MS Sql Bulk Insert
    /// see also: https://learn.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlbulkcopyoptions?view=dotnet-plat-ext-7.0#fields
    pub struct SqlBulkCopyOptions: u32 {
        /// Default options
        const Default           = 0b00000000;
        /// Preserve source identity values. When not specified, identity values are assigned by the destination.
        const KeepIdentity      = 0b00000001;
        /// Check constraints while data is being inserted. By default, constraints are not checked.
        const CheckConstraints  = 0b00000010;
        /// Obtain a bulk update lock for the duration of the bulk copy operation. When not specified, row locks are used.
        const TableLock         = 0b00000100;
        /// Preserve null values in the destination table regardless of the settings for default values. When not specified, null values are replaced by default values where applicable.
        const KeepNulls         = 0b00001000;
        /// When specified, cause the server to fire the insert triggers for the rows being inserted into the database.
        const FireTriggers      = 0b00010000;
    }
}

impl Default for SqlBulkCopyOptions {
    fn default() -> Self {
        SqlBulkCopyOptions::Default
    }
}

/// The sort order of a column, used for bulk insert
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// Ascending order
    Ascending,
    /// Descending order
    Descending
}

/// An order hint for bulk insert
pub type ColumOrderHint<'a> = (&'a str, SortOrder);
