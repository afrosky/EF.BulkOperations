# EFBulkExtensions
This project is a rewrite largely inspired by **Tiago L. da Nóbrega** excellent
**[EntityFramework.BulkExtensions](https://github.com/tiagoln/EntityFramework.BulkExtensions)** project
with additional/rewritten features:
  - [x] Specify the columns to include in bulk operations
  - [x] Retrieve generated identities after a bulk insert
  - [x] Specify the columns to use as identifier for bulk operations
  - [ ] Specify internal SqlBulkCopy options **[Coming next]**

NOTE: Identifier columns (only primary keys for now) have to be retrieved before calling
bulk update/delete in order for the operation to retrieve entities to process.

## Sample usages

### Bulk insert
```
context.BulkInsert(usersToInsert, settings =>
{
    settings.IncludedColumns = s => new { s.FirstName, s.LastName, s.LastLoginDate };
    settings.IsIdentityOutputEnabled = true;
});
```

### Bulk update

#### Simple

By default, the primary key is used as identifier column.

```
context.BulkUpdate(usersToUpdate, settings =>
{
	settings.IncludedColumns = s => new { s.LastLoginDate };
});
```

#### Custom identifier column

The following example updates the last update date of all external users:
```
var usersToUpdate = new List<User>
{
	new User { IsInternal = false, LastUpdateDate = DateTime.Now }
};

context.BulkUpdate(usersToUpdate, settings =>
{
	settings.IdentiferColumns = s => new { s.IsInternal };
	settings.IncludedColumns = s => new { s.LastUpdateDate };
});
```

### Bulk delete

#### Simple

By default, the primary key is used as identifier column.

```
context.BulkDelete(usersToDelete);
```

#### Custom identifier column

The following example deletes all external users:
```
var usersToDelete = new List<User>
{
	new User { IsInternal = false }
};

context.BulkDelete(usersToDelete, settings =>
{
	settings.IdentiferColumns = s => new { s.IsInternal };
});
```