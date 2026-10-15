#!/usr/bin/env -S node
import type { Contract as End } from '../../snapshots/8a45665e4f7f16574e423fa23578c2cd161a2a1c75d00e68dfd7f63bfdff3ef2/contract';
import endContract from '../../snapshots/8a45665e4f7f16574e423fa23578c2cd161a2a1c75d00e68dfd7f63bfdff3ef2/contract.json' with { type: 'json' };
import {
  Migration,
  MigrationCLI,
  checkExpression,
  col,
  lit,
  primaryKey,
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
        table: 'dispatch_queue',
        columns: [
          col('attempts', 'int4', {
            notNull: true,
            default: lit(0),
            codecRef: { codecId: 'pg/int4@1' },
          }),
          col('available_at', 'timestamp(6)', {
            notNull: true,
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
          col('created_at', 'timestamp(6)', {
            notNull: true,
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
          col('failure', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('id', 'character varying(255)', {
            notNull: true,
            codecRef: { codecId: 'sql/varchar@1', typeParams: { length: 255 } },
          }),
          col('lease_id', 'uuid', { codecRef: { codecId: 'pg/uuid@1' } }),
          col('leased_at', 'timestamp(6)', {
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
          col('payload', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('queue', 'character varying(255)', {
            notNull: true,
            codecRef: { codecId: 'sql/varchar@1', typeParams: { length: 255 } },
          }),
          col('sequence', 'BIGSERIAL', { notNull: true, codecRef: { codecId: 'pg/int8@1' } }),
          col('state', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('updated_at', 'timestamp(6)', {
            notNull: true,
            codecRef: { codecId: 'pg/timestamp-string@1', typeParams: { precision: 6 } },
          }),
        ],
        constraints: [
          primaryKey(['sequence']),
          checkExpression(
            'dispatch_queue_state_check_7cf91d99',
            `"state" IN ('pending', 'failed', 'completed')`,
          ),
        ],
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
        table: 'runner_events',
        columns: [
          col('payload', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('seq', 'SERIAL', { notNull: true, codecRef: { codecId: 'pg/int4@1' } }),
          col('taskId', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [primaryKey(['seq'])],
      }),
      this.createTable({
        schema: 'public',
        table: 'runner_tasks',
        columns: [
          col('address', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('agentBinding', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('agentCursor', 'int4', {
            notNull: true,
            default: lit(0),
            codecRef: { codecId: 'pg/int4@1' },
          }),
          col('agentRunId', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('detached', 'int4', {
            notNull: true,
            default: lit(0),
            codecRef: { codecId: 'pg/int4@1' },
          }),
          col('fingerprint', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('incarnation', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('occupied', 'int4', { notNull: true, codecRef: { codecId: 'pg/int4@1' } }),
          col('pid', 'int4', { codecRef: { codecId: 'pg/int4@1' } }),
          col('promptId', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('reason', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('result', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('sequence', 'SERIAL', { notNull: true, codecRef: { codecId: 'pg/int4@1' } }),
          col('state', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('taskId', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('work', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('worktree', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
        ],
        constraints: [
          primaryKey(['sequence']),
          checkExpression(
            'runner_tasks_state_check_40691796',
            `"state" IN ('ACCEPTED', 'LAUNCHING', 'READY', 'PROMPT_INTENT', 'RUNNING', 'SUCCEEDED', 'FAILED', 'HELD')`,
          ),
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
        table: 'dispatch_queue',
        constraint: 'idx_dispatch_queue_id',
        columns: ['id', 'queue'],
      }),
      this.addUnique({
        schema: 'public',
        table: 'runner_tasks',
        constraint: 'runner_tasks_taskId_key',
        columns: ['taskId'],
      }),
      this.createIndex({
        schema: 'public',
        table: 'dispatch_queue',
        index: 'idx_dispatch_queue_take_d4c7b4d1',
        columns: ['queue', 'available_at'],
        extras: { where: "state = 'pending'" },
      }),
      this.createIndex({
        schema: 'public',
        table: 'dispatch_queue',
        index: 'idx_dispatch_queue_update',
        columns: ['sequence', 'lease_id'],
      }),
      this.createIndex({
        schema: 'public',
        table: 'runner_events',
        index: 'runner_events_taskId_seq_idx_de7fe5ec',
        columns: ['taskId', 'seq'],
      }),
    ];
  }
}

MigrationCLI.run(import.meta.url, M);
