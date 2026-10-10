import { EscrowTimeoutScheduler, DEFAULT_TIMEOUT_CONFIG } from "../../jobs/escrowTimeout";
import { Pool } from "pg";

describe("EscrowTimeoutScheduler", () => {
  let mockPool: any;
  let mockClient: any;
  let mockTransporter: any;

  beforeEach(() => {
    mockClient = {
      query: jest.fn(),
      release: jest.fn(),
    };
    mockPool = {
      connect: jest.fn().mockResolvedValue(mockClient),
    };
    mockTransporter = {
      sendMail: jest.fn().mockResolvedValue({ messageId: "test-id" }),
    };
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("Distributed Advisory Lock Handling", () => {
    it("skips execution when advisory lock cannot be acquired", async () => {
      // Mock lock acquisition failure
      mockClient.query.mockResolvedValueOnce({
        rows: [{ acquired: false }],
      });

      const scheduler = new EscrowTimeoutScheduler(mockPool, {}, mockTransporter);
      const result = await scheduler.executeSweep();

      expect(result.skippedDueToLock).toBe(true);
      expect(result.scanned).toBe(0);
      expect(result.cancelled).toBe(0);
      expect(result.warned).toBe(0);
      expect(mockClient.release).toHaveBeenCalled();
    });

    it("releases advisory lock upon successful sweep execution", async () => {
      // Mock lock acquired + empty swaps
      mockClient.query
        .mockResolvedValueOnce({ rows: [{ acquired: true }] }) // advisory lock
        .mockResolvedValueOnce({ rows: [] }) // query swaps
        .mockResolvedValueOnce({ rows: [] }); // advisory unlock

      const scheduler = new EscrowTimeoutScheduler(mockPool, {}, mockTransporter);
      const result = await scheduler.executeSweep();

      expect(result.skippedDueToLock).toBeUndefined();
      expect(result.scanned).toBe(0);
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("pg_advisory_unlock"),
        [DEFAULT_TIMEOUT_CONFIG.advisoryLockId]
      );
      expect(mockClient.release).toHaveBeenCalled();
    });
  });

  describe("Warning Threshold Handling (12 Hours)", () => {
    it("dispatches warning email and updates swap record for 12h overdue swap", async () => {
      const now = new Date("2026-10-10T12:00:00Z");
      const agreedAt = new Date("2026-10-09T23:00:00Z"); // 13 hours ago (>= 12h, < 24h)

      const swap = {
        id: "swap-123",
        state: "agreed",
        agreed_at: agreedAt.toISOString(),
        listing_a_id: "listing-a",
        listing_b_id: "listing-b",
        counter_offer_details: null,
        party_a_email: "alice@example.com",
        party_b_email: "bob@example.com",
      };

      // Mock sequence:
      // 1. pg_try_advisory_lock
      // 2. SELECT swaps
      // 3. BEGIN
      // 4. UPDATE swaps (set warning_sent_at)
      // 5. COMMIT
      // 6. pg_advisory_unlock
      mockClient.query
        .mockResolvedValueOnce({ rows: [{ acquired: true }] })
        .mockResolvedValueOnce({ rows: [swap] })
        .mockResolvedValueOnce({ rows: [] }) // BEGIN
        .mockResolvedValueOnce({ rows: [] }) // UPDATE swaps
        .mockResolvedValueOnce({ rows: [] }) // COMMIT
        .mockResolvedValueOnce({ rows: [] }); // pg_advisory_unlock

      const scheduler = new EscrowTimeoutScheduler(mockPool, {}, mockTransporter);
      const result = await scheduler.executeSweep(now);

      expect(result.scanned).toBe(1);
      expect(result.warned).toBe(1);
      expect(result.cancelled).toBe(0);
      expect(mockTransporter.sendMail).toHaveBeenCalledWith(
        expect.objectContaining({
          to: ["alice@example.com", "bob@example.com"],
          subject: expect.stringContaining("Reminder"),
        })
      );
    });

    it("does not re-send warning if warning_sent_at is already present", async () => {
      const now = new Date("2026-10-10T12:00:00Z");
      const agreedAt = new Date("2026-10-09T23:00:00Z"); // 13 hours ago

      const swap = {
        id: "swap-124",
        state: "agreed",
        agreed_at: agreedAt.toISOString(),
        listing_a_id: "listing-a",
        listing_b_id: "listing-b",
        counter_offer_details: { warning_sent_at: "2026-10-10T11:00:00Z" },
        party_a_email: "alice@example.com",
        party_b_email: "bob@example.com",
      };

      mockClient.query
        .mockResolvedValueOnce({ rows: [{ acquired: true }] })
        .mockResolvedValueOnce({ rows: [swap] })
        .mockResolvedValueOnce({ rows: [] }); // pg_advisory_unlock

      const scheduler = new EscrowTimeoutScheduler(mockPool, {}, mockTransporter);
      const result = await scheduler.executeSweep(now);

      expect(result.scanned).toBe(1);
      expect(result.warned).toBe(0);
      expect(mockTransporter.sendMail).not.toHaveBeenCalled();
    });
  });

  describe("Timeout Cancellation Handling (24 Hours)", () => {
    it("cancels expired swap and releases listing locks when overdue by >= 24h", async () => {
      const now = new Date("2026-10-10T12:00:00Z");
      const agreedAt = new Date("2026-10-09T10:00:00Z"); // 26 hours ago (>= 24h)

      const swap = {
        id: "swap-expired",
        state: "agreed",
        agreed_at: agreedAt.toISOString(),
        listing_a_id: "listing-a",
        listing_b_id: "listing-b",
        counter_offer_details: null,
        party_a_email: "alice@example.com",
        party_b_email: "bob@example.com",
      };

      // Mock query sequence:
      // 1. pg_try_advisory_lock
      // 2. SELECT swaps
      // 3. BEGIN
      // 4. SELECT swaps FOR UPDATE
      // 5. UPDATE swaps SET state = 'cancelled'
      // 6. UPDATE listings SET is_locked = FALSE
      // 7. COMMIT
      // 8. pg_advisory_unlock
      mockClient.query
        .mockResolvedValueOnce({ rows: [{ acquired: true }] })
        .mockResolvedValueOnce({ rows: [swap] })
        .mockResolvedValueOnce({ rows: [] }) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: swap.id, state: "agreed" }] }) // FOR UPDATE
        .mockResolvedValueOnce({ rows: [] }) // UPDATE swaps
        .mockResolvedValueOnce({ rows: [] }) // UPDATE listings
        .mockResolvedValueOnce({ rows: [] }) // COMMIT
        .mockResolvedValueOnce({ rows: [] }); // pg_advisory_unlock

      const scheduler = new EscrowTimeoutScheduler(mockPool, {}, mockTransporter);
      const result = await scheduler.executeSweep(now);

      expect(result.scanned).toBe(1);
      expect(result.cancelled).toBe(1);
      expect(result.warned).toBe(0);

      // Verify cancellation email sent
      expect(mockTransporter.sendMail).toHaveBeenCalledWith(
        expect.objectContaining({
          to: ["alice@example.com", "bob@example.com"],
          subject: expect.stringContaining("Swap Cancelled"),
        })
      );
    });

    it("aborts cancellation if swap state changed concurrently", async () => {
      const now = new Date("2026-10-10T12:00:00Z");
      const agreedAt = new Date("2026-10-09T10:00:00Z"); // 26 hours ago

      const swap = {
        id: "swap-already-processed",
        state: "agreed",
        agreed_at: agreedAt.toISOString(),
        listing_a_id: "listing-a",
        listing_b_id: "listing-b",
        counter_offer_details: null,
        party_a_email: "alice@example.com",
        party_b_email: "bob@example.com",
      };

      // In the transaction, FOR UPDATE query returns empty because it's no longer agreed
      mockClient.query
        .mockResolvedValueOnce({ rows: [{ acquired: true }] })
        .mockResolvedValueOnce({ rows: [swap] })
        .mockResolvedValueOnce({ rows: [] }) // BEGIN
        .mockResolvedValueOnce({ rows: [] }) // FOR UPDATE returned 0 rows
        .mockResolvedValueOnce({ rows: [] }) // COMMIT
        .mockResolvedValueOnce({ rows: [] }); // pg_advisory_unlock

      const scheduler = new EscrowTimeoutScheduler(mockPool, {}, mockTransporter);
      const result = await scheduler.executeSweep(now);

      expect(result.cancelled).toBe(0);
      expect(mockTransporter.sendMail).not.toHaveBeenCalled();
    });
  });

  describe("Lifecycle & Timer Management", () => {
    it("starts and stops timer cleanly", async () => {
      jest.useFakeTimers();

      mockClient.query.mockResolvedValue({ rows: [{ acquired: true }] });

      const scheduler = new EscrowTimeoutScheduler(
        mockPool,
        { cronIntervalMs: 1000 },
        mockTransporter
      );

      scheduler.start();
      await scheduler.stop();

      jest.useRealTimers();
    });
  });
});
