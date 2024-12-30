import 'dart:async';

// ignore: implementation_imports
import 'package:sqflite_common/src/open_options.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:sqflite_common_ffi_web/sqflite_ffi_web.dart';
import 'package:sql_crdt/sql_crdt.dart';
import 'package:sqlite_crdt/src/sqlite_api.dart';

import 'src/batch_executor.dart';
import 'src/is_web_locator.dart';

export 'package:sqflite_common/sqlite_api.dart';
export 'package:sql_crdt/sql_crdt.dart';

class SqliteCrdt extends SqlCrdt {
  final Database _db;
  final Iterable<String> _excludedTables;

  SqliteCrdt._(this._db, this._excludedTables) : super(ExecutorApi(_db));

  /// Open or create a SQLite container as a SqlCrdt instance.
  ///
  /// See the Sqflite documentation for more details on opening a database:
  /// https://github.com/tekartik/sqflite/blob/master/sqflite/doc/opening_db.md
  static Future<SqliteCrdt> open(
    String path, {
    bool singleInstance = true,
    int? version,
    Iterable<String>? excludedTables,
    FutureOr<void> Function(CrdtTableExecutor db, int version)? onCreate,
    FutureOr<void> Function(CrdtTableExecutor db, int from, int to)? onUpgrade,
  }) =>
      _open(path, false, singleInstance, version, excludedTables, onCreate, onUpgrade);

  /// Open a transient SQLite in memory.
  /// Useful for testing or temporary sessions.
  static Future<SqliteCrdt> openInMemory({
    bool singleInstance = false,
    int? version,
    Iterable<String>? excludedTables,
    FutureOr<void> Function(CrdtTableExecutor db, int version)? onCreate,
    FutureOr<void> Function(CrdtTableExecutor db, int from, int to)? onUpgrade,
  }) =>
      _open(null, true, singleInstance, version, excludedTables, onCreate, onUpgrade);

  static Future<SqliteCrdt> _open(
    String? path,
    bool inMemory,
    bool singleInstance,
    int? version,
    Iterable<String>? excludedTables,
    FutureOr<void> Function(CrdtTableExecutor crdt, int version)? onCreate,
    FutureOr<void> Function(CrdtTableExecutor crdt, int from, int to)?
        onUpgrade,
  ) async {
    print('SqliteCrdt._open, excludedTables: $excludedTables');

    if (sqliteCrdtIsWeb && !inMemory && path!.contains('/')) {
      path = path.substring(path.lastIndexOf('/') + 1);
    }
    assert(inMemory || path!.isNotEmpty);
    final databaseFactory =
        sqliteCrdtIsWeb ? databaseFactoryFfiWeb : databaseFactoryFfi;

    if (!sqliteCrdtIsWeb && sqliteCrdtIsLinux) {
      await databaseFactory.setDatabasesPath('.');
    }

    final db = await databaseFactory.openDatabase(
      inMemory ? inMemoryDatabasePath : path!,
      options: SqfliteOpenDatabaseOptions(
        singleInstance: singleInstance,
        version: version,
        onCreate: onCreate == null
            ? null
            : (db, version) =>
                onCreate(CrdtTableExecutor(ExecutorApi(db)), version),
        onUpgrade: onUpgrade == null
            ? null
            : (db, from, to) =>
                onUpgrade(CrdtTableExecutor(ExecutorApi(db)), from, to),
      ),
    );

    final crdt = SqliteCrdt._(db, excludedTables ?? []);
    print('crdt._excludedTables: ${crdt._excludedTables}');
    await crdt.init();
    return crdt;
  }

  Future<void> close() => _db.close();

  @override
  Future<Iterable<String>> getTables() async {
    print('SqliteCrdt.getTables, _excludedTables: $_excludedTables');

    final excludedTablesStatement = _excludedTables.map((name) => "'$name'").join(', ');
    print('excludedTablesStatement, $excludedTablesStatement');

    final tableNames = (await _db.rawQuery('''
        SELECT name FROM sqlite_schema
        WHERE type ='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ($excludedTablesStatement)
      ''')).map((e) => e['name'] as String);

    print('tableNames: $tableNames');
    return tableNames;
  }

  @override
  Future<Iterable<String>> getTableKeys(String table) async =>
      (await _db.rawQuery('''
         SELECT name FROM pragma_table_info(?1)
         WHERE pk > 0
       ''', [table])).map((e) => e['name'] as String);

  BatchExecutor batch() =>
      BatchExecutor(_db.batch(), canonicalTime.increment());
}
