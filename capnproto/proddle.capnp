@0xa74d94a500622841;

interface Proddle {
    getModules @0 (modules :List(Module)) -> (modules :List(Module));
    getOperations @1 (bucketHashes :List(BucketHash)) -> (operationBuckets :List(OperationBucket));
}

struct BucketHash {
    bucket @0 :UInt64;
    hash @1 :UInt64;
}

# Module Definition
struct Module {
    id @0 :UInt64;
    timestamp @1 :UInt64;
    name @2 :Text;
    version @3 :UInt16;
    dependencies @4 :List(Text);
    content @5 :Text;
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
