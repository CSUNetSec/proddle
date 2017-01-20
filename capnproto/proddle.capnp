@0xa74d94a500622841;

interface Proddle {
    getMeasurements @0 (measurements :List(Measurement)) -> (measurements :List(Measurement));
    getOperations @1 (bucketHashes :List(BucketHash)) -> (operationBuckets :List(OperationBucket));
    sendResults @2 (results: List(Result)) -> ();
}

struct BucketHash {
    bucket @0 :UInt64;
    hash @1 :UInt64;
}

# Measurement Definition
struct Measurement {
    timestamp @0 :UInt64;
    name @1 :Text;
    version @2 :UInt16;
    dependencies @3 :List(Text);
    content @4 :Text;
}

# Operation Definition
struct Operation {
    timestamp @0 :UInt64;
    domain @1 :Text;
    measurement @2 :Text;
    interval @3 :UInt32;
}

struct OperationBucket {
    bucket @0: UInt64;
    operations @1 :List(Operation);
}

# Result Definition
struct Result {
    jsonString @0: Text;
}
