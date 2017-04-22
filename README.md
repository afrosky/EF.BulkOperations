# EFBulkExtensions
This project is a rewrite largely inspired by **Tiago L. da Nóbrega** excellent
**[EntityFramework.BulkExtensions](https://github.com/tiagoln/EntityFramework.BulkExtensions)** project
with additional/rewritten features:
  - [x] Specify the columns to include in bulk operations
  - [x] Retrieve generated identities after a bulk insert
  - [x] Specify the columns to use as identifier for bulk operations
  - [ ] Specify internal SqlBulkCopy options **[Coming next]**

## Sample usages

### Bulk insert

#### Simple

By default, all columns are included.
Duplicates are automatically excluded (According to the specified identifier columns or identity column).

```
context.BulkInsert(usersToInsert);
```

#### Custom settings

```
context.BulkInsert(usersToInsert, settings =>
{
    // Specify included columns
    settings.IncludedColumns = s => new { s.FirstName, s.LastName, s.LastLoginDate };
    
    // Retrieve generated ids
    settings.IsIdentityOutputEnabled = true;
});
```

### Bulk update

#### Simple

By default:
  - All columns are included (properties not set result in default value).
  - The primary key is used as identifier column.

```
context.BulkUpdate(usersToUpdate);
```

#### Custom settings

The following example updates the last update date of all external users:
```
var usersToUpdate = new List<User>
{
	new User { IsInternal = false, LastUpdateDate = DateTime.Now }
};

context.BulkUpdate(usersToUpdate, settings =>
{
    // Specify identifier columns
    settings.IdentiferColumns = s => new { s.IsInternal };

    // Specify included columns
    settings.IncludedColumns = s => new { s.LastUpdateDate };
});
```

### Bulk delete

#### Simple

By default, the primary key is used as identifier column.

```
context.BulkDelete(usersToDelete);
```

#### Custom settings

The following example deletes all external users:
```
var usersToDelete = new List<User>
{
	new User { IsInternal = false }
};

context.BulkDelete(usersToDelete, settings =>
{
    // Specify identifier columns
    settings.IdentiferColumns = s => new { s.IsInternal };
});
```