#!/usr/bin/env -S node
import type { Contract as End } from '../../snapshots/49f50c6058b06c01949aa66f12f6598480d342bdb1a06fef1d4879760ddaf063/contract';
import endContract from '../../snapshots/49f50c6058b06c01949aa66f12f6598480d342bdb1a06fef1d4879760ddaf063/contract.json' with { type: 'json' };
import {
  Migration,
  MigrationCLI,
  checkExpression,
  col,
  fn,
  lit,
  primaryKey,
  rawSql,
} from '@prisma/orm-postgres/migration';

export default class M extends Migration<never, End> {
  override readonly endContractJson = endContract;

  override get operations() {
    return [
      this.createSchema({ schema: 'public' }),
      this.createTable({
        schema: 'public',
        table: 'archive',
        columns: [
          col('created', 'float8', { notNull: true, codecRef: { codecId: 'pg/float8@1' } }),
          col('digest', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('entry', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('gaps', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('owner', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('payload', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('sequence', 'BIGSERIAL', { notNull: true, codecRef: { codecId: 'pg/int8@1' } }),
          col('session', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['sequence'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'consents',
        columns: [
          col('enabled', 'int4', { notNull: true, codecRef: { codecId: 'pg/int4@1' } }),
          col('owner', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('participants', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('session', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['owner', 'session'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'effect_queue',
        columns: [
          col('acquired_at', 'timestamp(6)', {
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
          col('acquired_by', 'uuid', { codecRef: { codecId: 'pg/uuid@1' } }),
          col('attempts', 'int4', {
            notNull: true,
            default: lit(0),
            codecRef: { codecId: 'pg/int4@1' },
          }),
          col('created_at', 'timestamp(6)', {
            notNull: true,
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
          col('element', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('id', 'character varying(255)', {
            notNull: true,
            codecRef: { codecId: 'sql/varchar@1', typeParams: { length: 255 } },
          }),
          col('last_failure', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('queue_name', 'character varying(255)', {
            notNull: true,
            codecRef: { codecId: 'sql/varchar@1', typeParams: { length: 255 } },
          }),
          col('sequence', 'BIGSERIAL', { notNull: true, codecRef: { codecId: 'pg/int8@1' } }),
          col('state', 'character varying(10)', {
            notNull: true,
            codecRef: { codecId: 'sql/varchar@1', typeParams: { length: 10 } },
          }),
          col('updated_at', 'timestamp(6)', {
            notNull: true,
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
          col('visible_at', 'timestamp(6)', {
            notNull: true,
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
        ],
        constraints: [primaryKey(['sequence'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'effect_queue_migrations',
        columns: [
          col('created_at', 'timestamptz(6)', {
            notNull: true,
            default: fn('now()'),
            codecRef: { codecId: 'pg/timestamptz-string@1', typeParams: { precision: 6 } },
          }),
          col('migration_id', 'int4', { notNull: true, codecRef: { codecId: 'pg/int4@1' } }),
          col('name', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['migration_id'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'evidence',
        columns: [
          col('data', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('id', 'BIGSERIAL', { notNull: true, codecRef: { codecId: 'pg/int8@1' } }),
          col('kind', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('observed', 'float8', { notNull: true, codecRef: { codecId: 'pg/float8@1' } }),
          col('principal', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('sha', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('task', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['id'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'participant_consents',
        columns: [
          col('enabled', 'int4', { notNull: true, codecRef: { codecId: 'pg/int4@1' } }),
          col('owner', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('participant', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('session', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['owner', 'session', 'participant'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'peers',
        columns: [
          col('expires', 'float8', { notNull: true, codecRef: { codecId: 'pg/float8@1' } }),
          col('fingerprint', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('id', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('incarnation', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('owner', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('payload', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('sender', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('session', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['id'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'recovery_gate',
        columns: [
          col('id', 'int4', { notNull: true, codecRef: { codecId: 'pg/int4@1' } }),
          col('ready', 'bool', { notNull: true, codecRef: { codecId: 'pg/bool@1' } }),
        ],
        constraints: [
          primaryKey(['id']),
          checkExpression('recovery_gate_singleton_7faf435e', 'id = 1'),
        ],
      }),
      this.createTable({
        schema: 'public',
        table: 'sessions',
        columns: [
          col('data', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('id', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['id'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'tasks',
        columns: [
          col('data', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('id', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['id'])],
      }),
      this.addUnique({
        schema: 'public',
        table: 'archive',
        constraint: 'archive_owner_session_entry_digest_key',
        columns: ['owner', 'session', 'entry', 'digest'],
      }),
      this.addUnique({
        schema: 'public',
        table: 'effect_queue',
        constraint: 'idx_effect_queue_id',
        columns: ['id', 'queue_name'],
      }),
      this.createIndex({
        schema: 'public',
        table: 'effect_queue',
        index: 'idx_effect_queue_take_3179164e',
        columns: ['queue_name', 'visible_at'],
        extras: { where: "state = 'pending'" },
      }),
      this.createIndex({
        schema: 'public',
        table: 'effect_queue',
        index: 'idx_effect_queue_update',
        columns: ['sequence', 'acquired_by'],
      }),
      // Native Effect must observe the final schema as already migrated before it can serve.
      rawSql({
        id: 'data.effect_queue_migrations.seed',
        label: 'Attest native Effect queue versions',
        operationClass: 'data',
        target: { id: 'postgres' },
        precheck: [{
          description: 'Tracker is empty before initial seeding',
          sql: 'SELECT NOT EXISTS (SELECT 1 FROM public.effect_queue_migrations) AS ok',
        }],
        execute: [{
          description: 'Seed the native queue migration versions atomically with schema creation',
          sql: "INSERT INTO public.effect_queue_migrations (migration_id,name) VALUES (1,'create_table'),(2,'upgrade_schema')",
        }],
        postcheck: [{
          description: 'Tracker contains exactly the native final schema versions',
          sql: "SELECT count(*) = 2 AND count(*) FILTER (WHERE (migration_id=1 AND name='create_table') OR (migration_id=2 AND name='upgrade_schema')) = 2 AS ok FROM public.effect_queue_migrations",
        }],
      }),
    ];
  }
}

MigrationCLI.run(import.meta.url, M);
