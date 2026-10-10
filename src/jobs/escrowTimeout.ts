import { Pool, PoolClient } from "pg";
import { Queue, Worker, Job } from "bullmq";
import nodemailer from "nodemailer";
import dotenv from "dotenv";
import { pool as defaultPool } from "../db/db";

dotenv.config();

export interface EscrowTimeoutConfig {
  warningThresholdMs: number; // default: 12 hours (12 * 60 * 60 * 1000)
  expiryThresholdMs: number;  // default: 24 hours (24 * 60 * 60 * 1000)
  cronIntervalMs: number;     // default: 5 minutes (5 * 60 * 1000)
  advisoryLockId: number;     // Postgres advisory lock ID (default: 894721)
  smtpFrom?: string;
}

export interface EscrowTimeoutExecutionResult {
  scanned: number;
  warned: number;
  cancelled: number;
  errors: string[];
  durationMs: number;
  skippedDueToLock?: boolean;
}

export const DEFAULT_TIMEOUT_CONFIG: EscrowTimeoutConfig = {
  warningThresholdMs: 12 * 60 * 60 * 1000, // 12 hours
  expiryThresholdMs: 24 * 60 * 60 * 1000,  // 24 hours
  cronIntervalMs: 5 * 60 * 1000,           // 5 minutes
  advisoryLockId: 894721,
  smtpFrom: process.env.SMTP_FROM || "no-reply@veloxous.com",
};

/**
 * Distributed Cron Job Scheduler for Escrow & Swap Timeouts
 * Enforces distributed singleton execution using Postgres advisory locks,
 * ACID state-machine transitions, and automated notification dispatch.
 */
export class EscrowTimeoutScheduler {
  private pool: Pool;
  private config: EscrowTimeoutConfig;
  private transporter: nodemailer.Transporter | null = null;
  private timer: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;
  private bullQueue: Queue | null = null;
  private bullWorker: Worker | null = null;

  constructor(
    customPool?: Pool,
    customConfig?: Partial<EscrowTimeoutConfig>,
    customTransporter?: nodemailer.Transporter
  ) {
    this.pool = customPool || defaultPool;
    this.config = { ...DEFAULT_TIMEOUT_CONFIG, ...customConfig };

    if (customTransporter) {
      this.transporter = customTransporter;
    } else if (process.env.SMTP_HOST) {
      this.transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST,
        port: parseInt(process.env.SMTP_PORT || "587"),
        secure: process.env.SMTP_SECURE === "true",
        auth: {
          user: process.env.SMTP_USER,
          pass: process.env.SMTP_PASS,
        },
      });
    }
  }

  /**
   * Run a single sweep over agreed swaps and execute warning / cancellation logic.
   * Uses Postgres advisory locks to guarantee only one distributed worker executes at a time.
   */
  public async executeSweep(now: Date = new Date()): Promise<EscrowTimeoutExecutionResult> {
    const startTime = Date.now();
    const result: EscrowTimeoutExecutionResult = {
      scanned: 0,
      warned: 0,
      cancelled: 0,
      errors: [],
      durationMs: 0,
    };

    const client = await this.pool.connect();
    let lockAcquired = false;

    try {
      // 1. Acquire distributed advisory lock
      const lockRes = await client.query(
        `SELECT pg_try_advisory_lock($1) as acquired`,
        [this.config.advisoryLockId]
      );
      lockAcquired = lockRes.rows[0]?.acquired === true;

      if (!lockAcquired) {
        result.skippedDueToLock = true;
        result.durationMs = Date.now() - startTime;
        return result;
      }

      // 2. Scan all agreed swaps awaiting collateral
      const swapsQuery = await client.query(
        `SELECT s.id, s.state, s.agreed_at, s.listing_a_id, s.listing_b_id,
                s.counter_offer_details,
                la.owner_email as party_a_email,
                lb.owner_email as party_b_email
         FROM swaps s
         JOIN listings la ON s.listing_a_id = la.id
         JOIN listings lb ON s.listing_b_id = lb.id
         WHERE s.state = 'agreed' AND s.agreed_at IS NOT NULL`
      );

      result.scanned = swapsQuery.rows.length;

      for (const swap of swapsQuery.rows) {
        try {
          const agreedAt = new Date(swap.agreed_at).getTime();
          const elapsed = now.getTime() - agreedAt;

          // Check for Expiry / Cancellation (>= 24 hours)
          if (elapsed >= this.config.expiryThresholdMs) {
            const cancelled = await this.cancelExpiredSwap(
              swap.id,
              swap.listing_a_id,
              swap.listing_b_id,
              [swap.party_a_email, swap.party_b_email]
            );
            if (cancelled) {
              result.cancelled++;
            }
          }
          // Check for 12-Hour Warning (< 24 hours && >= 12 hours)
          else if (elapsed >= this.config.warningThresholdMs) {
            const warned = await this.sendWarningIfNeeded(swap, [
              swap.party_a_email,
              swap.party_b_email,
            ]);
            if (warned) {
              result.warned++;
            }
          }
        } catch (itemError: any) {
          result.errors.push(`Swap ${swap.id}: ${itemError.message}`);
        }
      }
    } catch (err: any) {
      result.errors.push(`Sweep error: ${err.message}`);
    } finally {
      if (lockAcquired) {
        try {
          await client.query(`SELECT pg_advisory_unlock($1)`, [
            this.config.advisoryLockId,
          ]);
        } catch {
          // ignore unlock error
        }
      }
      client.release();
      result.durationMs = Date.now() - startTime;
    }

    return result;
  }

  /**
   * Send 12-hour collateral reminder if not previously recorded in counter_offer_details.
   */
  private async sendWarningIfNeeded(swap: any, emails: (string | null | undefined)[]): Promise<boolean> {
    const details = swap.counter_offer_details || {};
    if (details.warning_sent_at) {
      return false; // Already sent
    }

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      // Atomically tag that warning has been dispatched
      details.warning_sent_at = new Date().toISOString();
      await client.query(
        `UPDATE swaps
         SET counter_offer_details = $1, updated_at = NOW()
         WHERE id = $2 AND state = 'agreed'`,
        [details, swap.id]
      );

      await client.query("COMMIT");

      const validEmails = emails.filter((e): e is string => typeof e === "string" && e.length > 0);
      if (this.transporter && validEmails.length > 0) {
        await this.transporter.sendMail({
          from: this.config.smtpFrom,
          to: validEmails,
          subject: "Reminder: Deposit your collateral to complete the swap",
          html: `
            <div style="font-family: 'Space Grotesk', sans-serif; color: #0F172A;">
              <h2 style="color: #059669;">Collateral Deposit Reminder</h2>
              <p>Your swap <strong>#${swap.id}</strong> has been in agreed state for over 12 hours.</p>
              <p>Please fund your escrow collateral within the next 12 hours to prevent automatic cancellation.</p>
            </div>
          `,
        });
      }

      return true;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Cancel an expired swap and release listing locks in a single ACID transaction.
   */
  private async cancelExpiredSwap(
    swapId: string,
    listingAId: string,
    listingBId: string,
    emails: (string | null | undefined)[]
  ): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");

      // Verify and lock the swap record
      const swapRes = await client.query(
        `SELECT id, state FROM swaps WHERE id = $1 AND state = 'agreed' FOR UPDATE`,
        [swapId]
      );

      if (swapRes.rows.length === 0) {
        await client.query("COMMIT");
        return false; // Already cancelled or progressed
      }

      // Transition swap to cancelled
      await client.query(
        `UPDATE swaps
         SET state = 'cancelled', updated_at = NOW()
         WHERE id = $1`,
        [swapId]
      );

      // Release listing locks
      await client.query(
        `UPDATE listings
         SET is_locked = FALSE, current_swap_id = NULL, updated_at = NOW()
         WHERE id IN ($1, $2)`,
        [listingAId, listingBId]
      );

      await client.query("COMMIT");

      // Dispatch cancellation notification
      const validEmails = emails.filter((e): e is string => typeof e === "string" && e.length > 0);
      if (this.transporter && validEmails.length > 0) {
        await this.transporter.sendMail({
          from: this.config.smtpFrom,
          to: validEmails,
          subject: "Swap Cancelled: Collateral Deposit Timeout",
          html: `
            <div style="font-family: 'Space Grotesk', sans-serif; color: #0F172A;">
              <h2 style="color: #DC2626;">Swap Cancelled</h2>
              <p>Your swap <strong>#${swapId}</strong> has been automatically cancelled because collateral was not deposited within the 24-hour limit.</p>
              <p>Your listings have been unlocked and reactivated for new proposals.</p>
            </div>
          `,
        });
      }

      return true;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Start recurring background cron loop.
   */
  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;

    // Run immediate sweep on startup
    this.executeSweep().catch((err) => {
      console.error("[EscrowTimeoutScheduler] Initial sweep error:", err);
    });

    // Schedule periodic sweeps
    this.timer = setInterval(() => {
      this.executeSweep().catch((err) => {
        console.error("[EscrowTimeoutScheduler] Cron execution error:", err);
      });
    }, this.config.cronIntervalMs);
  }

  /**
   * Stop recurring cron loop and close resources.
   */
  public async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.isRunning = false;

    if (this.bullWorker) {
      await this.bullWorker.close();
      this.bullWorker = null;
    }
    if (this.bullQueue) {
      await this.bullQueue.close();
      this.bullQueue = null;
    }
  }
}
