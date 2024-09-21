# JSON vs ProtoBuf


## Results

```
goos: windows
goarch: amd64
pkg: benchmark
cpu: 12th Gen Intel(R) Core(TM) i7-1270P
BenchmarkJSONMarshal
BenchmarkJSONMarshal-16
  394483	      3225 ns/op	     156 B/op	       2 allocs/op
BenchmarkJSONUnmarshal
BenchmarkJSONUnmarshal-16
  177482	      7099 ns/op	     312 B/op	       7 allocs/op
BenchmarkProtobufMarshal
BenchmarkProtobufMarshal-16
  638736	      1781 ns/op	      48 B/op	       1 allocs/op
BenchmarkProtobufUnmarshal
BenchmarkProtobufUnmarshal-16
  656088	      1821 ns/op	     120 B/op	       3 allocs/op
```