import request from "supertest";
import { createApp } from "../../api";

// ── Module mocks must be declared before any imports that use them ──────────
jest.mock("../../db", () => ({
  queryTransfers: jest.fn(),
  queryAllTransfers: jest.fn(),
  queryByTxHash: jest.fn(),
  querySummary: jest.fn(),
  getLastIndexedLedger: jest.fn(),
  prisma: {
    $queryRaw: jest.fn(),
    tokenTransfer: { findMany: jest.fn() },
    tokenMetadata: { findMany: jest.fn() },
  },
}));

jest.mock("../../rpc", () => ({
  getLatestLedger: jest.fn(),
}));

jest.mock("../../indexer", () => ({
  // #161: /status also reads per-network loop state. Listed explicitly
  // because a partial mock silently 500s the route rather than failing loudly.
  getAllIndexerStats: jest.fn().mockReturnValue({}),
  runningNetworks: jest.fn().mockReturnValue([]),
  getIndexerStats: jest
    .fn()
    .mockReturnValue({ startedAt: "2024-01-01T00:00:00.000Z", uptimeSeconds: 0, totalIndexed: 0 }),
}));

jest.mock("../../linq/ngnOrders", () => ({}));

import { prisma } from "../../db";
import { _resetTokenCache, initTokenCache } from "../../tokenCache";

// ── Typed mock helpers ────────────────────────────────────────────────────────
const mockPrismaTokenTransferFindMany = prisma.tokenTransfer.findMany as jest.MockedFunction<typeof prisma.tokenTransfer.findMany>;

// ── Seed data factory ─────────────────────────────────────────────────────────
const CONTRACT_A = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";

const ALICE = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF";
const BOB   = "GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBWWHF";

function makeTransfer(id: number, overrides: Partial<ReturnType<typeof baseTransfer>> = {}) {
  return { ...baseTransfer(), id, ...overrides };
}

function baseTransfer() {
  return {
    id: 1,
    network: "testnet",
    contractId: CONTRACT_A,
    eventType: "transfer",
    fromAddress: BOB,
    toAddress: ALICE,
    amount: "10000000000",
    ledger: 1000,
    ledgerClosedAt: new Date("2025-01-01T00:00:00Z"),
    txHash: "aaaa1111",
    eventId: "evt-001",
    isSac: false,
    createdAt: new Date("2025-01-01T00:00:01Z"),
  };
}

// ─────────────────────────────────────────────────────────────────────────────

describe("Export route handlers", () => {
  const app = createApp();

  beforeEach(() => {
    _resetTokenCache();
    mockPrismaTokenTransferFindMany.mockClear();
  });

  describe("GET /transfers.csv", () => {
    it("returns 400 for invalid fromLedger (non-numeric)", async () => {
      const res = await request(app).get("/transfers.csv").query({ fromLedger: "abc" });

      expect(res.status).toBe(400);
      expect(res.body.error).toMatch(/invalid/i);
    });

    it("returns 400 for invalid maxRows (negative)", async () => {
      const res = await request(app).get("/transfers.csv").query({ maxRows: "-1" });

      expect(res.status).toBe(400);
    });

    it("returns 400 for invalid fromDate", async () => {
      const res = await request(app).get("/transfers.csv").query({ fromDate: "not-a-date" });

      expect(res.status).toBe(400);
      expect(res.body.error).toMatch(/Invalid date/i);
    });

    it("returns 400 for invalid toDate", async () => {
      const res = await request(app).get("/transfers.csv").query({ toDate: "garbage" });

      expect(res.status).toBe(400);
    });

    it("caps results at maxRows when more rows exist", async () => {
      // Seed 10 rows, cap at 5
      const transfers = Array.from({ length: 10 }, (_, i) => makeTransfer(i + 1));
      
      // First call: isTruncated check (skip 5, take 1, select: { id: true })
      mockPrismaTokenTransferFindMany
        .mockResolvedValueOnce([{ id: 6 } as any]) // Row exists beyond cap
        // Second call: streamTransfers (take 5)
        .mockResolvedValueOnce(transfers.slice(0, 5));

      (prisma.tokenMetadata.findMany as jest.Mock).mockResolvedValue([
        { network: "testnet", contractId: CONTRACT_A, symbol: "TOK", name: "Token", decimals: 7 },
      ]);
      await initTokenCache("testnet");

      const res = await request(app).get("/transfers.csv").query({ maxRows: "5" });

      expect(res.status).toBe(200);
      expect(res.headers["x-truncated"]).toBe("true");
      expect(res.headers["x-row-limit"]).toBe("5");
      expect(res.headers["content-type"]).toBe("text/csv");
      
      const csvBody = res.text;
      const lines = csvBody.split("\n").filter(Boolean);
      expect(lines.length).toBe(6); // header + 5 rows
    });

    it("does not set truncation headers when fewer rows than cap", async () => {
      const transfers = Array.from({ length: 3 }, (_, i) => makeTransfer(i + 1));
      
      // First call: isTruncated check (skip 5, take 1)
      mockPrismaTokenTransferFindMany
        .mockResolvedValueOnce([]) // No row beyond cap
        // Second call: streamTransfers (take 5, but only 3 exist)
        .mockResolvedValueOnce(transfers);

      (prisma.tokenMetadata.findMany as jest.Mock).mockResolvedValue([
        { network: "testnet", contractId: CONTRACT_A, symbol: "TOK", name: "Token", decimals: 7 },
      ]);
      await initTokenCache("testnet");

      const res = await request(app).get("/transfers.csv").query({ maxRows: "5" });

      expect(res.status).toBe(200);
      expect(res.headers["x-truncated"]).toBeUndefined();
      expect(res.headers["x-row-limit"]).toBeUndefined();
      
      const csvBody = res.text;
      const lines = csvBody.split("\n").filter(Boolean);
      expect(lines.length).toBe(4); // header + 3 rows
    });

    it("handles empty string query params gracefully (ignores them)", async () => {
      mockPrismaTokenTransferFindMany
        .mockResolvedValueOnce([]) // isTruncated check
        .mockResolvedValueOnce([]); // streamTransfers

      const res = await request(app)
        .get("/transfers.csv")
        .query({ address: "", contractId: "", maxRows: "" });

      expect(res.status).toBe(200);
    });

    it("takes first value when duplicate params are provided", async () => {
      mockPrismaTokenTransferFindMany
        .mockResolvedValueOnce([]) // isTruncated check
        .mockResolvedValueOnce([]); // streamTransfers

      const res = await request(app)
        .get("/transfers.csv")
        .query({ maxRows: ["10", "20"] });

      expect(res.status).toBe(200);
    });
  });

  describe("GET /transfers.parquet", () => {
    it("returns 400 for invalid fromLedger (non-numeric)", async () => {
      const res = await request(app).get("/transfers.parquet").query({ fromLedger: "abc" });

      expect(res.status).toBe(400);
      expect(res.body.error).toMatch(/invalid/i);
    });

    it("returns 400 for invalid maxRows (negative)", async () => {
      const res = await request(app).get("/transfers.parquet").query({ maxRows: "-1" });

      expect(res.status).toBe(400);
    });

    it("caps results at maxRows when more rows exist", async () => {
      const transfers = Array.from({ length: 10 }, (_, i) => makeTransfer(i + 1));
      
      mockPrismaTokenTransferFindMany
        .mockResolvedValueOnce([{ id: 6 }] as any) // isTruncated check
        .mockResolvedValueOnce(transfers.slice(0, 5)) // streamTransfers batch 1
        .mockResolvedValueOnce([]); // streamTransfers batch 2 (exhausted)

      (prisma.tokenMetadata.findMany as jest.Mock).mockResolvedValue([
        { network: "testnet", contractId: CONTRACT_A, symbol: "TOK", name: "Token", decimals: 7 },
      ]);
      await initTokenCache("testnet");

      const res = await request(app).get("/transfers.parquet").query({ maxRows: "5" });

      expect(res.status).toBe(200);
      expect(res.headers["x-truncated"]).toBe("true");
      expect(res.headers["x-row-limit"]).toBe("5");
      expect(res.headers["content-type"]).toBe("application/octet-stream");
      expect(res.headers["content-disposition"]).toContain("transfers.parquet");
    });

    it("does not set truncation headers when fewer rows than cap", async () => {
      const transfers = Array.from({ length: 3 }, (_, i) => makeTransfer(i + 1));
      
      mockPrismaTokenTransferFindMany
        .mockResolvedValueOnce([]) // isTruncated check
        .mockResolvedValueOnce(transfers) // streamTransfers
        .mockResolvedValueOnce([]); // streamTransfers exhausted

      (prisma.tokenMetadata.findMany as jest.Mock).mockResolvedValue([
        { network: "testnet", contractId: CONTRACT_A, symbol: "TOK", name: "Token", decimals: 7 },
      ]);
      await initTokenCache("testnet");

      const res = await request(app).get("/transfers.parquet").query({ maxRows: "5" });

      expect(res.status).toBe(200);
      expect(res.headers["x-truncated"]).toBeUndefined();
      expect(res.headers["x-row-limit"]).toBeUndefined();
    });
  });
});
