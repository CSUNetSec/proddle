@0xa74d94a500622841;

interface Proddle {
    getModules @0 (bucketHashes :List(BucketHash)) -> (moduleBucket :List(ModuleBucket));
    getOperations @1 (bucketHashes :List(BucketHash)) -> (operationBucket: List(OperationBucket));
}

struct BucketHash {
    bucket @0 :UInt64;
    hash @1 :UInt64;
}

# Module Definition
struct Module {
    id @0 :UInt64;
    name @1 :Text;
    version @2 :UInt16;
    content @3 :Text;
}

struct ModuleBucket {
    bucket @0 :UInt64;
    modules @1 :List(Module);
}

# Operation Definition
struct Operation {
    id @0 :UInt64;
    domain @1 :Text;
    module @2 :Text;
    interval @3 :UInt32;
}

struct OperationBucket {
    bucket @0: UInt64;
    operataions @1 :List(Operation);
}
