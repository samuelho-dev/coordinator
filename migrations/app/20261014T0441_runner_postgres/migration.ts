#!/usr/bin/env -S node
import type { Contract as Start } from '../../snapshots/49f50c6058b06c01949aa66f12f6598480d342bdb1a06fef1d4879760ddaf063/contract';
import startContract from '../../snapshots/49f50c6058b06c01949aa66f12f6598480d342bdb1a06fef1d4879760ddaf063/contract.json' with { type: 'json' };
import type { Contract as End } from '../../snapshots/760c0ad54b65dc6f3098b00865c256f062047846de76dda5c9e3c53a65ba6825/contract';
import endContract from '../../snapshots/760c0ad54b65dc6f3098b00865c256f062047846de76dda5c9e3c53a65ba6825/contract.json' with { type: 'json' };
import { Migration, MigrationCLI, col, lit, primaryKey } from '@prisma/orm-postgres/migration';

export default class M extends Migration<Start, End> {
  override readonly startContractJson = startContract;
  override readonly endContractJson = endContract;

  override get operations() {
    return [
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
          col('detached', 'int4', {
            notNull: true,
            default: lit(0),
            codecRef: { codecId: 'pg/int4@1' },
          }),
          col('fingerprint', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('incarnation', 'text', { notNull: true, codecRef: { codecId: 'pg/text@1' } }),
          col('nativeBinding', 'text', { codecRef: { codecId: 'pg/text@1' } }),
          col('nativeCursor', 'int4', {
            notNull: true,
            default: lit(0),
            codecRef: { codecId: 'pg/int4@1' },
          }),
          col('nativeRunId', 'text', { codecRef: { codecId: 'pg/text@1' } }),
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
        constraints: [primaryKey(['sequence'])],
      }),
      this.addUnique({
        schema: 'public',
        table: 'runner_tasks',
        constraint: 'runner_tasks_taskId_key',
        columns: ['taskId'],
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
