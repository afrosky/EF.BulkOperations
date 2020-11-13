# EFBulkExtensions

This project is a port of **Boris Djurdjevic** excellent **[EFCore.BulkExtensions](https://github.com/borisdj/EFCore.BulkExtensions)** project to .NET Framework.

## Usage

It's pretty simple and straightforward.
Bulk Extensions are made on DbContext class and can be used like this (Async methods are not supported yet):

```
context.BulkInsert(usersToInsert, optionalConfig);
context.BulkUpdate(usersToUpdate, optionalConfig);
context.BulkDelete(usersToDelete, optionalConfig);
```

**BulkMerge** can be used if you need to do bulk operations like `upsert` (insert or update at once).

```
context.BulkMerge(usersToMerge, bulkMergeOperationType, optionalConfig);
```

**BulkMergeOperationType** is a `Bitwise Enum` with the following values:

- Insert
- Update
- Delete

> A `Bitwise Enum` implies you can specify any combination. For example, an `upsert` would be represented like the following expression: `BulkMergeOperationType.Insert | BulkMergeOperationType.Update`.

## BulkConfig arguments

**BulkInsert**, **BulkUpdate**, **BulkDelete**, **BulkMerge** methods can have optional argument **BulkConfig** with following properties:

| Name                            | Type                              | Description                                                                                                                                                   | Default Value              |
| ------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| SetOutputIdentity               | bool                              | A value indicating whether identity of inserted entities should be returned                                                                                   | false                      |
| IdentifierColumns               | Expression<Func<TEntity, object>> | The columns to use as identifer. By default, primary key columns are taken                                                                                    | null                       |
| IncludedColumns                 | Expression<Func<TEntity, object>> | The included columns. By default, all columns are included                                                                                                    | null                       |
| SqlBulkCopyOptions              | SqlBulkCopyOptions                | Bitwise flag that specifies one or more options to use with an instance of System.Data.SqlClient.SqlBulkCopy                                                  | SqlBulkCopyOptions.Default |
| SqlBulkCopyBatchSize            | int                               | Number of rows in each batch. At the end of each batch, the rows in the batch are sent to the server                                                          | 0                          |
| SqlBulkCopyNotifyAfter          | int?                              | Defines the number of rows to be processed before generating a notification event                                                                             | null                       |
| SqlBulkCopyTimeout              | int?                              | Number of seconds for the operation to complete before it times out (30 seconds by default)<br />Set 0 for infinite timeout                                   | null                       |
| SqlBulkCopyEnableStreaming      | bool                              | A value indicating whether System.Data.SqlClient.SqlBulkCopy object streams data from an System.Data.IDataReader object                                       | false                      |
| SqlBulkCopyProgressEventHandler | Action&lt;decimal&gt;             | An action to be executed while bulk operation is in progress (useful for long process and display loading status) <br/>**Input**: the current progress (in %) |
| null                            |
| MergeWithHoldLock               | bool                              | A value indicating whether merge operation uses HOLD LOCK                                                                                                     | true                       |
| IsBulkResultEnabled             | bool                              | A value indicating whether bulk results should be calculated                                                                                                  | false                      |
| UseTempDb                       | bool                              | A value indicating whether the use of tempDB is enabled                                                                                                       | true                       |
| BulkResult                      | BulkResult                        | The bulk operation results.                                                                                                                                   | null                       |

If a different behavior is intended, create a new instance of `BulkConfig`, set the desired properties to their desired value, then pass it to bulk extension methods.

### _SetOutputIdentity_

When **SetOutputIdentity** is set to `True`, you have to set ordered value to your identity column.
For example if table already has rows, let's say it has 1000 rows with Id-s (1:1000), and we now want to add 300 more.
Since Id-s are generated in Db we could not set them, they would all be 0 (int default) in list.
But if we want to keep the order as they are ordered in list then those Id-s should be set say 1 to 300 (for BulkInsert).
Here single Id value itself doesn't matter, db will change it to (1001:1300), what matters is their mutual relationship for sorting.
Insertion order is implemented with TOP in conjuction with ORDER BY [stackoverflow:merge-into-insertion-order](https://stackoverflow.com/questions/884187/merge-into-insertion-order).

> As a general rule, for sorting to be preserved, set ascending ordered negative values to the identity property for values to insert.
> In case of a upsert operation (using **BulkMerge**), don't forget to sort the list of items before calling the operation.<br />
> Example: if we have a list of 8000 entries to insert, say 3000 for update (they keep the real Id) and 5000 for insert then Id-s could be (-5000:-1).

**SetOutputIdentity** is useful when **BulkInsert** is done to multiple related tables, that have Identity column.
After Insert is done to first table, we need Id-s that were generated in Db because they are FK(ForeignKey) in second table.

### _IsBulkResultEnabled_

When **IsBulkResultEnabled** is set to `True` the result is added to the provided **BulkConfig**, in **BulkResult** property which itself contains the following properties:

| Name     | Type | Description                 |
| -------- | ---- | --------------------------- |
| Inserted | int  | The number of inserted rows |
| Updated  | int  | The number of updated rows  |
| Deleted  | int  | The number of deleted rows  |

### _UseTempDB_

When **UseTempDB** is set to `False`, temporary tables will be created next to other tables.

> Use this settings for debug purpose.