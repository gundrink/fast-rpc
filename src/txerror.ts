// bincode TransactionError (as shipped by Geyser/gRPC) -> JSON-RPC error JSON (same as getTransaction meta.err).
const TX = ["AccountInUse", "AccountLoadedTwice", "AccountNotFound", "ProgramAccountNotFound", "InsufficientFundsForFee", "InvalidAccountForFee", "AlreadyProcessed",
  "BlockhashNotFound", "InstructionError", "CallChainTooDeep", "MissingSignatureForFee", "InvalidAccountIndex", "SignatureFailure", "InvalidProgramForExecution",
  "SanitizeFailure", "ClusterMaintenance", "AccountBorrowOutstanding", "WouldExceedMaxBlockCostLimit", "UnsupportedVersion", "InvalidWritableAccount",
  "WouldExceedMaxAccountCostLimit", "WouldExceedAccountDataBlockLimit", "TooManyAccountLocks", "AddressLookupTableNotFound", "InvalidAddressLookupTableOwner",
  "InvalidAddressLookupTableData", "InvalidAddressLookupTableIndex", "InvalidRentPayingAccount", "WouldExceedMaxVoteCostLimit", "WouldExceedAccountDataTotalLimit",
  "DuplicateInstruction", "InsufficientFundsForRent", "MaxLoadedAccountsDataSizeExceeded", "InvalidLoadedAccountsDataSizeLimit", "ResanitizationNeeded",
  "ProgramExecutionTemporarilyRestricted", "UnbalancedTransaction", "ProgramCacheHitMaxLimit", "CommitCancelled"];
const IX = ["GenericError", "InvalidArgument", "InvalidInstructionData", "InvalidAccountData", "AccountDataTooSmall", "InsufficientFunds", "IncorrectProgramId",
  "MissingRequiredSignature", "AccountAlreadyInitialized", "UninitializedAccount", "UnbalancedInstruction", "ModifiedProgramId", "ExternalAccountLamportSpend",
  "ExternalAccountDataModified", "ReadonlyLamportChange", "ReadonlyDataModified", "DuplicateAccountIndex", "ExecutableModified", "RentEpochModified",
  "NotEnoughAccountKeys", "AccountDataSizeChanged", "AccountNotExecutable", "AccountBorrowFailed", "AccountBorrowOutstanding", "DuplicateAccountOutOfSync",
  "Custom", "InvalidError", "ExecutableDataModified", "ExecutableLamportChange", "ExecutableAccountNotRentExempt", "UnsupportedProgramId", "CallDepth",
  "MissingAccount", "ReentrancyNotAllowed", "MaxSeedLengthExceeded", "InvalidSeeds", "InvalidRealloc", "ComputationalBudgetExceeded", "PrivilegeEscalation",
  "ProgramEnvironmentSetupFailure", "ProgramFailedToComplete", "ProgramFailedToCompile", "Immutable", "IncorrectAuthority", "BorshIoError", "AccountNotRentExempt",
  "InvalidAccountOwner", "ArithmeticOverflow", "UnsupportedSysvar", "IllegalOwner", "MaxAccountsDataAllocationsExceeded", "MaxAccountsExceeded",
  "MaxInstructionTraceLengthExceeded", "BuiltinProgramsMustConsumeComputeUnits"];

export function decodeTxError(bytes: Uint8Array | undefined | null): any {
  if (!bytes || !bytes.length) return null;
  const b = Buffer.from(bytes);
  try {
    const v = b.readUInt32LE(0); const name = TX[v];
    if (!name) return { Unknown: b.toString("base64") };
    if (name === "InstructionError") {
      const idx = b[4]; const iv = b.readUInt32LE(5); const iname = IX[iv];
      if (iname === "Custom") return { InstructionError: [idx, { Custom: b.readUInt32LE(9) }] };
      if (iname === "BorshIoError") { const n = Number(b.readBigUInt64LE(9)); return { InstructionError: [idx, { BorshIoError: b.subarray(17, 17 + n).toString("utf8") }] }; }
      return { InstructionError: [idx, iname ?? { Unknown: iv }] };
    }
    if (name === "DuplicateInstruction") return { DuplicateInstruction: b[4] };
    if (name === "InsufficientFundsForRent" || name === "ProgramExecutionTemporarilyRestricted") return { [name]: { account_index: b[4] } };
    return name;
  } catch { return { Unknown: b.toString("base64") }; }
}
/** inverse (tests / mocks) */
export function encodeTxError(e: any): Buffer {
  const u32 = (n: number) => { const x = Buffer.alloc(4); x.writeUInt32LE(n); return x; };
  if (typeof e === "string") return u32(TX.indexOf(e));
  const [k, v] = Object.entries(e)[0] as [string, any];
  if (k === "InstructionError") {
    const [i, ie] = v; if (typeof ie === "string") return Buffer.concat([u32(8), Buffer.from([i]), u32(IX.indexOf(ie))]);
    if ("Custom" in ie) return Buffer.concat([u32(8), Buffer.from([i]), u32(25), u32(ie.Custom)]);
  }
  if (k === "DuplicateInstruction") return Buffer.concat([u32(30), Buffer.from([v])]);
  return Buffer.concat([u32(TX.indexOf(k)), Buffer.from([v.account_index])]);
}
