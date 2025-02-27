/**
 * Helper functions for accessing the RxStorage instances.
 */
import { map } from 'rxjs/operators';
import { runPluginHooks } from './hooks';
import { overwritable } from './overwritable';
import { newRxError } from './rx-error';
import { fillPrimaryKey, getPrimaryFieldOfPrimaryKey } from './rx-schema-helper';
import { createRevision, ensureNotFalsy, firstPropertyValueOfObject, flatClone, getDefaultRevision, getDefaultRxDocumentMeta, now, parseRevision, randomCouchString } from './util';

/**
 * Writes a single document,
 * throws RxStorageBulkWriteError on failure
 */
export var writeSingle = function writeSingle(instance, writeRow, context) {
  try {
    return Promise.resolve(instance.bulkWrite([writeRow], context)).then(function (writeResult) {
      if (Object.keys(writeResult.error).length > 0) {
        var error = firstPropertyValueOfObject(writeResult.error);
        throw error;
      } else {
        var ret = firstPropertyValueOfObject(writeResult.success);
        return ret;
      }
    });
  } catch (e) {
    return Promise.reject(e);
  }
};
/**
 * Checkpoints must be stackable over another.
 * This is required form some RxStorage implementations
 * like the sharding plugin, where a checkpoint only represents
 * the document state from some, but not all shards.
 */

export var getSingleDocument = function getSingleDocument(storageInstance, documentId) {
  try {
    return Promise.resolve(storageInstance.findDocumentsById([documentId], false)).then(function (results) {
      var doc = results[documentId];

      if (doc) {
        return doc;
      } else {
        return null;
      }
    });
  } catch (e) {
    return Promise.reject(e);
  }
};

/**
 * Returns all non-deleted documents
 * of the storage.
 */
export var getAllDocuments = function getAllDocuments(primaryKey, storageInstance) {
  try {
    var _ref;

    var storage = storageInstance.storage;
    var getAllQueryPrepared = storage.statics.prepareQuery(storageInstance.schema, {
      selector: {},
      sort: [(_ref = {}, _ref[primaryKey] = 'asc', _ref)],
      skip: 0
    });
    return Promise.resolve(storageInstance.query(getAllQueryPrepared)).then(function (queryResult) {
      var allDocs = queryResult.documents;
      return allDocs;
    });
  } catch (e) {
    return Promise.reject(e);
  }
};
export var INTERNAL_STORAGE_NAME = '_rxdb_internal';
export var RX_DATABASE_LOCAL_DOCS_STORAGE_NAME = 'rxdatabase_storage_local';
export function stackCheckpoints(checkpoints) {
  return Object.assign.apply(Object, [{}].concat(checkpoints));
}
export function storageChangeEventToRxChangeEvent(isLocal, rxStorageChangeEvent, rxCollection) {
  var documentData;
  /**
   * TODO
   * this data design is shit,
   * instead of having the documentData depending on the operation,
   * we should always have a current doc data, that might or might not
   * have set _deleted to true.
   */

  if (rxStorageChangeEvent.change.operation !== 'DELETE') {
    documentData = rxStorageChangeEvent.change.doc;
  }

  var previousDocumentData;

  if (rxStorageChangeEvent.change.operation !== 'INSERT') {
    previousDocumentData = rxStorageChangeEvent.change.previous;
  }

  var ret = {
    eventId: rxStorageChangeEvent.eventId,
    documentId: rxStorageChangeEvent.documentId,
    collectionName: rxCollection ? rxCollection.name : undefined,
    startTime: rxStorageChangeEvent.startTime,
    endTime: rxStorageChangeEvent.endTime,
    isLocal: isLocal,
    operation: rxStorageChangeEvent.change.operation,
    documentData: overwritable.deepFreezeWhenDevMode(documentData),
    previousDocumentData: overwritable.deepFreezeWhenDevMode(previousDocumentData)
  };
  return ret;
}
export function throwIfIsStorageWriteError(collection, documentId, writeData, error) {
  if (error) {
    if (error.status === 409) {
      throw newRxError('COL19', {
        collection: collection.name,
        id: documentId,
        error: error,
        data: writeData
      });
    } else {
      throw error;
    }
  }
}
export function getNewestOfDocumentStates(primaryPath, docs) {
  var ret = null;
  docs.forEach(function (doc) {
    if (!ret || doc._meta.lwt > ret._meta.lwt || doc._meta.lwt === ret._meta.lwt && doc[primaryPath] > ret[primaryPath]) {
      ret = doc;
    }
  });
  return ensureNotFalsy(ret);
}
/**
 * Analyzes a list of BulkWriteRows and determines
 * which documents must be inserted, updated or deleted
 * and which events must be emitted and which documents cause a conflict
 * and must not be written.
 * Used as helper inside of some RxStorage implementations.
 */

export function categorizeBulkWriteRows(storageInstance, primaryPath,
/**
 * Current state of the documents
 * inside of the storage. Used to determine
 * which writes cause conflicts.
 */
docsInDb,
/**
 * The write rows that are passed to
 * RxStorageInstance().bulkWrite().
 */
bulkWriteRows, context) {
  var hasAttachments = !!storageInstance.schema.attachments;
  var bulkInsertDocs = [];
  var bulkUpdateDocs = [];
  var errors = [];
  var changedDocumentIds = [];
  var eventBulk = {
    id: randomCouchString(10),
    events: [],
    checkpoint: null,
    context: context
  };
  var attachmentsAdd = [];
  var attachmentsRemove = [];
  var attachmentsUpdate = [];
  var startTime = now();
  bulkWriteRows.forEach(function (writeRow) {
    var id = writeRow.document[primaryPath];
    var documentInDb = docsInDb.get(id);
    var attachmentError;

    if (!documentInDb) {
      /**
       * It is possible to insert already deleted documents,
       * this can happen on replication.
       */
      var insertedIsDeleted = writeRow.document._deleted ? true : false;
      Object.entries(writeRow.document._attachments).forEach(function (_ref2) {
        var attachmentId = _ref2[0],
            attachmentData = _ref2[1];

        if (!attachmentData.data) {
          attachmentError = {
            documentId: id,
            isError: true,
            status: 510,
            writeRow: writeRow
          };
          errors.push(attachmentError);
        } else {
          attachmentsAdd.push({
            documentId: id,
            attachmentId: attachmentId,
            attachmentData: attachmentData
          });
        }
      });

      if (!attachmentError) {
        if (hasAttachments) {
          bulkInsertDocs.push(stripAttachmentsDataFromRow(writeRow));
        } else {
          bulkInsertDocs.push(writeRow);
        }
      }

      if (!insertedIsDeleted) {
        changedDocumentIds.push(id);
        eventBulk.events.push({
          eventId: getUniqueDeterministicEventKey(storageInstance, primaryPath, writeRow),
          documentId: id,
          change: {
            doc: hasAttachments ? stripAttachmentsDataFromDocument(writeRow.document) : writeRow.document,
            id: id,
            operation: 'INSERT',
            previous: null
          },
          startTime: startTime,
          endTime: now()
        });
      }
    } else {
      // update existing document
      var revInDb = documentInDb._rev;
      /**
       * Check for conflict
       */

      if (!writeRow.previous || !!writeRow.previous && revInDb !== writeRow.previous._rev) {
        // is conflict error
        var err = {
          isError: true,
          status: 409,
          documentId: id,
          writeRow: writeRow,
          documentInDb: documentInDb
        };
        errors.push(err);
        return;
      } // handle attachments data


      if (writeRow.document._deleted) {
        /**
         * Deleted documents must have cleared all their attachments.
         */
        if (writeRow.previous) {
          Object.keys(writeRow.previous._attachments).forEach(function (attachmentId) {
            attachmentsRemove.push({
              documentId: id,
              attachmentId: attachmentId
            });
          });
        }
      } else {
        // first check for errors
        Object.entries(writeRow.document._attachments).find(function (_ref3) {
          var attachmentId = _ref3[0],
              attachmentData = _ref3[1];
          var previousAttachmentData = writeRow.previous ? writeRow.previous._attachments[attachmentId] : undefined;

          if (!previousAttachmentData && !attachmentData.data) {
            attachmentError = {
              documentId: id,
              documentInDb: documentInDb,
              isError: true,
              status: 510,
              writeRow: writeRow
            };
          }

          return true;
        });

        if (!attachmentError) {
          Object.entries(writeRow.document._attachments).forEach(function (_ref4) {
            var attachmentId = _ref4[0],
                attachmentData = _ref4[1];
            var previousAttachmentData = writeRow.previous ? writeRow.previous._attachments[attachmentId] : undefined;

            if (!previousAttachmentData) {
              attachmentsAdd.push({
                documentId: id,
                attachmentId: attachmentId,
                attachmentData: attachmentData
              });
            } else {
              attachmentsUpdate.push({
                documentId: id,
                attachmentId: attachmentId,
                attachmentData: attachmentData
              });
            }
          });
        }
      }

      if (attachmentError) {
        errors.push(attachmentError);
      } else {
        if (hasAttachments) {
          bulkUpdateDocs.push(stripAttachmentsDataFromRow(writeRow));
        } else {
          bulkUpdateDocs.push(writeRow);
        }
      }

      var change = null;
      var writeDoc = writeRow.document;

      if (writeRow.previous && writeRow.previous._deleted && !writeDoc._deleted) {
        change = {
          id: id,
          operation: 'INSERT',
          previous: null,
          doc: hasAttachments ? stripAttachmentsDataFromDocument(writeDoc) : writeDoc
        };
      } else if (writeRow.previous && !writeRow.previous._deleted && !writeDoc._deleted) {
        change = {
          id: id,
          operation: 'UPDATE',
          previous: writeRow.previous,
          doc: hasAttachments ? stripAttachmentsDataFromDocument(writeDoc) : writeDoc
        };
      } else if (writeRow.previous && !writeRow.previous._deleted && writeDoc._deleted) {
        change = {
          id: id,
          operation: 'DELETE',
          previous: writeRow.previous,
          doc: null
        };
      }

      if (!change) {
        if (writeRow.previous && writeRow.previous._deleted && writeRow.document._deleted) {// deleted doc got overwritten with other deleted doc -> do not send an event
        } else {
          throw newRxError('SNH', {
            args: {
              writeRow: writeRow
            }
          });
        }
      } else {
        changedDocumentIds.push(id);
        eventBulk.events.push({
          eventId: getUniqueDeterministicEventKey(storageInstance, primaryPath, writeRow),
          documentId: id,
          change: change,
          startTime: startTime,
          endTime: now()
        });
      }
    }
  });
  return {
    bulkInsertDocs: bulkInsertDocs,
    bulkUpdateDocs: bulkUpdateDocs,
    errors: errors,
    changedDocumentIds: changedDocumentIds,
    eventBulk: eventBulk,
    attachmentsAdd: attachmentsAdd,
    attachmentsRemove: attachmentsRemove,
    attachmentsUpdate: attachmentsUpdate
  };
}
export function stripAttachmentsDataFromRow(writeRow) {
  return {
    previous: writeRow.previous,
    document: stripAttachmentsDataFromDocument(writeRow.document)
  };
}
export function stripAttachmentsDataFromDocument(doc) {
  var useDoc = flatClone(doc);
  useDoc._attachments = {};
  Object.entries(doc._attachments).forEach(function (_ref5) {
    var attachmentId = _ref5[0],
        attachmentData = _ref5[1];
    useDoc._attachments[attachmentId] = {
      digest: attachmentData.digest,
      length: attachmentData.length,
      type: attachmentData.type
    };
  });
  return useDoc;
}
/**
 * Flat clone the document data
 * and also the _meta field.
 * Used many times when we want to change the meta
 * during replication etc.
 */

export function flatCloneDocWithMeta(doc) {
  var ret = flatClone(doc);
  ret._meta = flatClone(doc._meta);
  return ret;
}
/**
 * Each event is labeled with the id
 * to make it easy to filter out duplicates.
 */

export function getUniqueDeterministicEventKey(storageInstance, primaryPath, writeRow) {
  var docId = writeRow.document[primaryPath];
  var binaryValues = [!!writeRow.previous, writeRow.previous && writeRow.previous._deleted, !!writeRow.document._deleted];
  var binary = binaryValues.map(function (v) {
    return v ? '1' : '0';
  }).join('');
  var eventKey = storageInstance.databaseName + '|' + storageInstance.collectionName + '|' + docId + '|' + '|' + binary + '|' + writeRow.document._rev;
  return eventKey;
}
export function hashAttachmentData(attachmentBase64String, storageStatics) {
  return storageStatics.hash(atob(attachmentBase64String));
}
export function getAttachmentSize(attachmentBase64String) {
  return atob(attachmentBase64String).length;
}
/**
 * Wraps the normal storageInstance of a RxCollection
 * to ensure that all access is properly using the hooks
 * and other data transformations and also ensure that database.lockedRun()
 * is used properly.
 */

export function getWrappedStorageInstance(database, storageInstance,
/**
 * The original RxJsonSchema
 * before it was mutated by hooks.
 */
rxJsonSchema) {
  overwritable.deepFreezeWhenDevMode(rxJsonSchema);
  var primaryPath = getPrimaryFieldOfPrimaryKey(rxJsonSchema.primaryKey);

  function transformDocumentDataFromRxDBToRxStorage(writeRow) {
    var data = flatClone(writeRow.document);
    data._meta = flatClone(data._meta);
    /**
     * Do some checks in dev-mode
     * that would be too performance expensive
     * in production.
     */

    if (overwritable.isDevMode()) {
      // ensure that the primary key has not been changed
      data = fillPrimaryKey(primaryPath, rxJsonSchema, data);
      /**
       * Ensure that the new revision is higher
       * then the previous one
       */

      if (writeRow.previous) {
        var prev = parseRevision(writeRow.previous._rev);
        var current = parseRevision(writeRow.document._rev);

        if (current.height <= prev.height) {
          throw newRxError('SNH', {
            dataBefore: writeRow.previous,
            dataAfter: writeRow.document,
            args: {
              prev: prev,
              current: current
            }
          });
        }
      }
      /**
       * Ensure that _meta fields have been merged
       * and not replaced.
       * This is important so that when one plugin A
       * sets a _meta field and another plugin B does a write
       * to the document, it must be ensured that the
       * field of plugin A was not removed.
       */


      if (writeRow.previous) {
        Object.keys(writeRow.previous._meta).forEach(function (metaFieldName) {
          if (!writeRow.document._meta.hasOwnProperty(metaFieldName)) {
            throw newRxError('SNH', {
              dataBefore: writeRow.previous,
              dataAfter: writeRow.document
            });
          }
        });
      }
    }

    data._meta.lwt = now();
    var hookParams = {
      database: database,
      primaryPath: primaryPath,
      schema: rxJsonSchema,
      doc: data
    };
    /**
     * Run the hooks once for the previous doc,
     * once for the new write data
     */

    var previous = writeRow.previous;

    if (previous) {
      hookParams.doc = previous;
      runPluginHooks('preWriteToStorageInstance', hookParams);
      previous = hookParams.doc;
    }

    hookParams.doc = data;
    runPluginHooks('preWriteToStorageInstance', hookParams);
    data = hookParams.doc;
    /**
     * Do not update the revision here.
     * The caller of bulkWrite() must be able to set
     * the revision and to be sure that the given revision
     * is used when storing the document.
     * The revision must be provided by the caller of bulkWrite().
     */

    if (!data._rev) {
      throw newRxError('SNH', {
        data: data
      });
    }

    return {
      document: data,
      previous: previous
    };
  }

  function transformDocumentDataFromRxStorageToRxDB(data) {
    var hookParams = {
      database: database,
      primaryPath: primaryPath,
      schema: rxJsonSchema,
      doc: data
    };
    runPluginHooks('postReadFromInstance', hookParams);
    return hookParams.doc;
  }

  function transformErrorDataFromRxStorageToRxDB(error) {
    var ret = flatClone(error);
    ret.writeRow = flatClone(ret.writeRow);

    if (ret.documentInDb) {
      ret.documentInDb = transformDocumentDataFromRxStorageToRxDB(ret.documentInDb);
    }

    ret.writeRow.document = transformDocumentDataFromRxStorageToRxDB(ret.writeRow.document);

    if (ret.writeRow.previous) {
      ret.writeRow.previous = transformDocumentDataFromRxStorageToRxDB(ret.writeRow.previous);
    }

    return ret;
  }

  var ret = {
    storage: storageInstance.storage,
    schema: storageInstance.schema,
    internals: storageInstance.internals,
    collectionName: storageInstance.collectionName,
    databaseName: storageInstance.databaseName,
    options: storageInstance.options,
    bulkWrite: function bulkWrite(rows, context) {
      var toStorageWriteRows = rows.map(function (row) {
        return transformDocumentDataFromRxDBToRxStorage(row);
      });
      return database.lockedRun(function () {
        return storageInstance.bulkWrite(toStorageWriteRows, context);
      })
      /**
       * The RxStorageInstance MUST NOT allow to insert already _deleted documents,
       * without sending the previous document version.
       * But for better developer experience, RxDB does allow to re-insert deleted documents.
       * We do this by automatically fixing the conflict errors for that case
       * by running another bulkWrite() and merging the results.
       * @link https://github.com/pubkey/rxdb/pull/3839
       */
      .then(function (writeResult) {
        var reInsertErrors = Object.values(writeResult.error).filter(function (error) {
          if (error.status === 409 && !error.writeRow.previous && !error.writeRow.document._deleted && ensureNotFalsy(error.documentInDb)._deleted) {
            return true;
          }

          return false;
        });

        if (reInsertErrors.length > 0) {
          var useWriteResult = {
            error: flatClone(writeResult.error),
            success: flatClone(writeResult.success)
          };
          var reInserts = reInsertErrors.map(function (error) {
            delete useWriteResult.error[error.documentId];
            return {
              previous: error.documentInDb,
              document: Object.assign({}, error.writeRow.document, {
                _rev: createRevision(error.writeRow.document, error.documentInDb)
              })
            };
          });
          return database.lockedRun(function () {
            return storageInstance.bulkWrite(reInserts, context);
          }).then(function (subResult) {
            useWriteResult.error = Object.assign(useWriteResult.error, subResult.error);
            useWriteResult.success = Object.assign(useWriteResult.success, subResult.success);
            return useWriteResult;
          });
        }

        return writeResult;
      }).then(function (writeResult) {
        var ret = {
          success: {},
          error: {}
        };
        Object.entries(writeResult.success).forEach(function (_ref6) {
          var k = _ref6[0],
              v = _ref6[1];
          ret.success[k] = transformDocumentDataFromRxStorageToRxDB(v);
        });
        Object.entries(writeResult.error).forEach(function (_ref7) {
          var k = _ref7[0],
              error = _ref7[1];
          ret.error[k] = transformErrorDataFromRxStorageToRxDB(error);
        });
        return ret;
      });
    },
    query: function query(preparedQuery) {
      return database.lockedRun(function () {
        return storageInstance.query(preparedQuery);
      }).then(function (queryResult) {
        return {
          documents: queryResult.documents.map(function (doc) {
            return transformDocumentDataFromRxStorageToRxDB(doc);
          })
        };
      });
    },
    findDocumentsById: function findDocumentsById(ids, deleted) {
      return database.lockedRun(function () {
        return storageInstance.findDocumentsById(ids, deleted);
      }).then(function (findResult) {
        var ret = {};
        Object.entries(findResult).forEach(function (_ref8) {
          var key = _ref8[0],
              doc = _ref8[1];
          ret[key] = transformDocumentDataFromRxStorageToRxDB(doc);
        });
        return ret;
      });
    },
    getAttachmentData: function getAttachmentData(documentId, attachmentId) {
      return database.lockedRun(function () {
        return storageInstance.getAttachmentData(documentId, attachmentId);
      });
    },
    getChangedDocumentsSince: function getChangedDocumentsSince(limit, checkpoint) {
      return database.lockedRun(function () {
        return storageInstance.getChangedDocumentsSince(limit, checkpoint);
      }).then(function (result) {
        return {
          checkpoint: result.checkpoint,
          documents: result.documents.map(function (d) {
            return transformDocumentDataFromRxStorageToRxDB(d);
          })
        };
      });
    },
    cleanup: function cleanup(minDeletedTime) {
      return database.lockedRun(function () {
        return storageInstance.cleanup(minDeletedTime);
      });
    },
    remove: function remove() {
      return database.lockedRun(function () {
        return storageInstance.remove();
      });
    },
    close: function close() {
      return database.lockedRun(function () {
        return storageInstance.close();
      });
    },
    changeStream: function changeStream() {
      return storageInstance.changeStream().pipe(map(function (eventBulk) {
        var ret = {
          id: eventBulk.id,
          events: eventBulk.events.map(function (event) {
            return {
              eventId: event.eventId,
              documentId: event.documentId,
              endTime: event.endTime,
              startTime: event.startTime,
              change: {
                id: event.change.id,
                operation: event.change.operation,
                doc: event.change.doc ? transformDocumentDataFromRxStorageToRxDB(event.change.doc) : undefined,
                previous: event.change.previous ? transformDocumentDataFromRxStorageToRxDB(event.change.previous) : undefined
              }
            };
          }),
          checkpoint: eventBulk.checkpoint,
          context: eventBulk.context
        };
        return ret;
      }));
    },
    conflictResultionTasks: function conflictResultionTasks() {
      return storageInstance.conflictResultionTasks().pipe(map(function (task) {
        var assumedMasterState = task.input.assumedMasterState ? transformDocumentDataFromRxStorageToRxDB(task.input.assumedMasterState) : undefined;
        var newDocumentState = transformDocumentDataFromRxStorageToRxDB(task.input.newDocumentState);
        var realMasterState = transformDocumentDataFromRxStorageToRxDB(task.input.realMasterState);
        return {
          id: task.id,
          context: task.context,
          input: {
            assumedMasterState: assumedMasterState,
            realMasterState: realMasterState,
            newDocumentState: newDocumentState
          }
        };
      }));
    },
    resolveConflictResultionTask: function resolveConflictResultionTask(taskSolution) {
      if (taskSolution.output.isEqual) {
        return storageInstance.resolveConflictResultionTask(taskSolution);
      }

      var hookParams = {
        database: database,
        primaryPath: primaryPath,
        schema: rxJsonSchema,
        doc: Object.assign({}, taskSolution.output.documentData, {
          _meta: getDefaultRxDocumentMeta(),
          _rev: getDefaultRevision(),
          _attachments: {}
        })
      };
      hookParams.doc._rev = createRevision(hookParams.doc);
      runPluginHooks('preWriteToStorageInstance', hookParams);
      var postHookDocData = hookParams.doc;
      var documentData = flatClone(postHookDocData);
      delete documentData._meta;
      delete documentData._rev;
      delete documentData._attachments;
      return storageInstance.resolveConflictResultionTask({
        id: taskSolution.id,
        output: {
          isEqual: false,
          documentData: documentData
        }
      });
    }
  };
  return ret;
}
//# sourceMappingURL=rx-storage-helper.js.map