# EFBulkExtensions
This project is a rewrite largely inspired by **Tiago L. da Nóbrega** excellent
**[EntityFramework.BulkExtensions](https://github.com/tiagoln/EntityFramework.BulkExtensions)** project
with additional/rewritten features:
  - [x] Specify the columns to include in bulk operations
  - [x] Retrieve generated identities after a bulk insert
  - [ ] Specify the columns to use as identifier for bulk operations **[Coming next]**
  - [ ] Specify internal SqlBulkCopy options **[Coming next]**

NOTE: Identifier columns (only primary keys for now) have to be retrieved before calling
bulk update/delete in order for the operation to retrieve entities to process.

## Sample usages

### Bulk insert
```
context.BulkInsert(usersToInsert, settings =>
{
    settings.IncludedColumns = s => new { s.Id, s.FirstName, s.LastName, s.LastLoginDate };
    settings.IsIdentityOutputEnabled = true;
});
```

### Bulk update
```
context.BulkUpdate(usersToUpdate, settings =>
{
    settings.IncludedColumns = s => new { s.Id, s.LastLoginDate };
});
```

### Bulk delete
```
context.BulkDelete(usersToDelete);
```
