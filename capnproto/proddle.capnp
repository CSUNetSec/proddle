@0xa74d94a500622841;

interface Proddle {
    getOperations @0 (bucketHashes :List(BucketHash)) -> (operationBuckets :List(OperationBucket));
    sendMeasurements @1 (measurements: List(Result)) -> ();
}

struct BucketHash {
    bucket @0 :UInt64;
    hash @1 :UInt64;
}

struct Parameter {
    name @0 :Text;
    value @1 :Text;
}

struct Operation {
    timestamp @0 :Int64;
    measurementClass @1 :Text;
    domain @2 :Text;
    parameters @3 :List(Parameter);
    tags @4 :List(Text);
}

struct OperationBucket {
    bucket @0: UInt64;
    operations @1 :List(Operation);
}

struct Result {
    jsonString @0: Text;
}
