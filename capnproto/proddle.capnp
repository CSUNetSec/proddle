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

struct Parameter {
    name @0 :Text;
    value @1 :Text;
}

struct Measurement {
    timestamp @0 :UInt64;
    name @1 :Text;
    version @2 :UInt16;
    parameters @3: List(Parameter);
    dependencies @4 :List(Text);
    content @5 :Text;
}

struct Operation {
    timestamp @0 :UInt64;
    measurement @1 :Text;
    domain @2 :Text;
    url @3 :Text;
    parameters @4 :List(Parameter);
    interval @5 :UInt32;
    tags @6 :List(Text);
}

struct OperationBucket {
    bucket @0: UInt64;
    operations @1 :List(Operation);
}

struct Result {
    jsonString @0: Text;
}
