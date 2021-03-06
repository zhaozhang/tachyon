package tachyon.master;

/** Type of EditLog entry. */
enum EditLogOperationType {
  ADD_BLOCK, ADD_CHECKPOINT, CREATE_FILE, COMPLETE_FILE, SET_PINNED, RENAME, DELETE,
  CREATE_RAW_TABLE, UPDATE_RAW_TABLE_METADATA, CREATE_DEPENDENCY,
}
